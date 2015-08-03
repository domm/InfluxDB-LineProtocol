#!/usr/bin/env perl
use strict;
use warnings;
use 5.012;
use Test::Most;
use InfluxDB::LineProtocol qw(data2line line2data);

my @faketime = ( 1437072205, 500681 );
my $nano = join( '', @faketime ) * 1000;
{
    no warnings 'redefine';

    sub InfluxDB::LineProtocol::gettimeofday() {
        wantarray ? @faketime : join( '.', @faketime );
    }
};

# tests look like:
# - boolan flag if we provide an explicit timestamp
# - ArrayRef of data passed to data2line
# - String of the expected line (without the timestamp if we're not using explicit_timestamp
# - ArrayRef we expected when parsing the line
# - Optional is-TODO-marker

my @tests = (
    # some basic tests without timestamps
    [   0,
        [ 'metric', 42 ],
        'metric value=42',
        [ 'metric', { value => 42 }, undef ]
    ],
    [
        0, ['metric', {hit=>1, cost=>42}],
        'metric cost=42,hit=1',
        [ 'metric', {hit=>1, cost=>42}, undef ]
    ],
    [
        0, ['metric', 42, {server=>'srv1',location=>'eu'}],
        'metric,location=eu,server=srv1 value=42',
        [ 'metric', {value=>42}, {server=>'srv1',location=>'eu'} ]
    ],
    [
        0, ['metric', {cost=>42}, {server=>'srv1',location=>'eu'}],
        'metric,location=eu,server=srv1 cost=42',
        [ 'metric', {cost=>42}, {server=>'srv1',location=>'eu'} ]
    ],
    # now with timestamps
    [   1,
        [ 'metric', 42, 1437072299900001000 ],
        'metric value=42 1437072299900001000',
        [ 'metric', { value => 42 }, undef, 1437072299900001000 ]
    ],
    [
        1, ['metric', {hit=>1, cost=>42},1437072299900001000],
        'metric cost=42,hit=1 1437072299900001000',
        [ 'metric', {hit=>1, cost=>42},undef, 1437072299900001000 ]
    ],
    [
        1, ['metric', 42, , {server=>'srv1',location=>'eu'},1437072299900001000],
        'metric,location=eu,server=srv1 value=42 1437072299900001000',
        [ 'metric', {value=>42}, {server=>'srv1',location=>'eu'}, 1437072299900001000 ]
    ],
    [
        1, ['metric', {cost=>42}, {server=>'srv1',location=>'eu'} ,1437072299900001000],
        'metric,location=eu,server=srv1 cost=42 1437072299900001000',
        [ 'metric', {cost=>42}, {server=>'srv1',location=>'eu'},1437072299900001000 ]
    ],
    # weird measurment names
    [   0,
        [ 'metric with space', 42 ],
        'metric\ with\ space value=42',
        [ 'metric with space', { value => '42' }, undef ]
    ],
    [   0,
        [ 'metric,with,comma', 42 ],
        'metric\,with\,comma value=42',
        [ 'metric,with,comma', { value => '42' }, undef ]
    ],
    [   0,
        [ 'metric,with,comma', 42 , { tag=>'foo' }],
        'metric\,with\,comma,tag=foo value=42',
        [ 'metric,with,comma', { value => '42' }, { tag=>'foo' } ]
    ],
    [   0,
        [ 'metric\with\backslash', 42 ],
        'metric\with\backslash value=42',
        [ 'metric\with\backslash', { value => '42' }, undef ]
    ],

    # different value types
    [   0,
        [ 'metric', 'foo' ],
        'metric value="foo"',
        [ 'metric', { value => 'foo' }, undef ]
    ],
    [   0,
        [ 'metric', 1.41 ],
        'metric value=1.41',
        [ 'metric', { value => 1.41 }, undef ]
    ],
    [   0,
        [ 'metric', -1.41 ],
        'metric value=-1.41',
        [ 'metric', { value => -1.41 }, undef ]
    ],
    [   0,
        [ 'metric', -42 ],
        'metric value=-42',
        [ 'metric', { value => -42 }, undef ]
    ],
    [   0,
        [ 'metric', 7.51696501241595e-05 ],
        'metric value=7.51696501241595e-05',
        [ 'metric', { value => 7.51696501241595e-05 }, undef ],
        [ 'SKIP', sub { $^O eq 'MSWin32' }, 'negative exponentials are strange on windows' ]
    ],
    [   0,
        [ 'metric', '7.51696501241595e05' ],
        'metric value=7.51696501241595e05',
        [ 'metric', { value => '7.51696501241595e05' }, undef ]
    ],
    [   0,
        [ 'metric', 'foo"bar"' ],
        'metric value="foo\"bar\""',
        [ 'metric', { value => 'foo"bar"' }, undef ],
        [ 'TODO' ]
    ],
    [
        0,
        [ 'metric', 't' ],
        'metric value=t',
        [ 'metric', { value => 't' }, undef ],
    ],
    [
        0,
        [ 'metric', 'T' ],
        'metric value=T',
        [ 'metric', { value => 'T' }, undef ],
    ],
    [
        0,
        [ 'metric', 'FALSE' ],
        'metric value=FALSE',
        [ 'metric', { value => 'FALSE' }, undef ],
    ],
    [
        0,
        [ 'metric', 'False' ],
        'metric value="False"',
        [ 'metric', { value => 'False' }, undef ],
    ],
    [
        0,
        [ 'metric', 'tru' ],
        'metric value="tru"',
        [ 'metric', { value => 'tru' }, undef ],
    ],
    
    # escape values
    # tag types
    # escape tags
);

while ( my ( $i, $case ) = each @tests ) {
    my ( $explicit_timestamp, $in, $raw_line, $out, $testtag ) = @$case;
    explain("case $i: $raw_line");

    my $expected_line;
    if ($explicit_timestamp) {
        $expected_line = $raw_line;
    }
    else {
        $expected_line = $raw_line . ' ' . $nano;
        push(@$out,$nano);
    }

    if ($testtag) {
        if ($testtag->[0] eq 'TODO') {
            TODO: {
                local $TODO = 'not implemented yet';
                _do_test($i, $in, $expected_line, $out);
            };
            next;
        }
        elsif ($testtag->[0] eq 'SKIP' && $testtag->[1]->()) {
            SKIP: {
                skip $testtag->[2], 2;
                _do_test($i, $in, $expected_line, $out);
            };
            next;
        }
    }

    _do_test($i, $in, $expected_line, $out);
}

sub _do_test {
    my ($i, $in, $expected_line, $out) = @_;
    is( data2line(@$in), $expected_line, "data2line case $i" );
    my @result = line2data($expected_line);
    cmp_deeply( \@result, $out, "line2data case $i" );
}
done_testing();
