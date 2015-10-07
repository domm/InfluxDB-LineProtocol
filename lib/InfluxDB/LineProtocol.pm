package InfluxDB::LineProtocol;
use strict;
use warnings;

our $VERSION = '1.005';

# ABSTRACT: Write and read InfluxDB LineProtocol

use Carp qw(croak);
use Time::HiRes qw(gettimeofday);

my %versions = (
    'v0.9.2' => '_0_9_2',
);

sub import {
    my $class = shift;
    my $caller = caller();


    my @to_export;
    my $version;
    foreach my $param (@_) {
        if ($param eq 'data2line' || $param eq 'line2data') {
            push(@to_export,$param);
        }
        if ($param =~ /^v[\d\.]+$/ && $versions{$param}) {
            $version = $versions{$param};
        }
    }

    foreach my $function (@to_export) {
        my $target = $function;
        $function = '_'.$function.$version if $version;

        {
            no strict 'refs';
            *{"$caller\::$target"} = \&$function;
        }
    }

}

sub data2line {
    my ( $measurement, $values, $tags, $timestamp ) = @_;

    if ( @_ == 1 ) {
        # no $fields, so assume we already got a line
        return $measurement;
    }

    my $key = $measurement;
    $key =~ s/([, ])/\\$1/g;

    # $tags has to be a hashref, if it's not, we dont have tags, so it's the timestamp
    if ( defined $tags ) {
        if ( ref($tags) eq 'HASH' ) {
            my @tags;
            foreach my $k ( sort keys %$tags )
            {    # Influx wants the tags presorted
                # TODO check if sorting algorithm matches
                #      http://golang.org/pkg/bytes/#Compare
                my $v = $tags->{$k};
                $k =~ s/([, ])/\\$1/g;
                $v =~ s/([, ])/\\$1/g;
                push( @tags, $k . '=' . $v );
            }
            $key .= join( ',', '', @tags ) if @tags;
        }
        elsif ( !ref($tags) ) {
            $timestamp = $tags;
        }
    }

    if ($timestamp) {
        croak("$timestamp does not look like an epoch timestamp")
            unless $timestamp =~ /^\d+$/;
        if ( length($timestamp) < 19 ) {
            my $missing = 19 - length($timestamp);
            my $zeros   = 0 x $missing;
            $timestamp .= $zeros;
        }
    }
    else {
        # Get time of day returns (seconds, microseconds)
        # $timestamp needs to be nanoseconds
        # it must also be a string to avoid conversion to sci notations
        $timestamp = sprintf "%s%06d000", gettimeofday();
    }

    # If values is not a hashref, convert it into one
    $values = { value => $values } if (not ref($values));

    my @fields;
    foreach my $k ( sort keys %$values ) {
        my $v = $values->{$k};
        $k =~ s/([, ])/\\$1/g;

        if (
            # positive & negativ ints, exponentials, use Regexp::Common?
            $v !~ /^-?\d+(?:\.\d+)?(?:e-?\d+)?$/
            &&
            # perl 5.12 Regexp::Assemble->new->add(qw(t T true TRUE f F false FALSE))->re;
            $v !~ /^(?:F(?:ALSE)?|f(?:alse)?|T(?:RUE)?|t(?:rue)?)$/
        )
        {
            $v =~ s/"/\\"/g;
            $v = '"' . $v . '"';
        }
        elsif ($v=~/^-?\d+$/) { # looks like int
            $v.='i';
        }
        push( @fields, $k . '=' . $v );
    }
    my $fields = join( ',', @fields );

    return sprintf( "%s %s %s", $key, $fields, $timestamp );
}

sub line2data {
    my $line = shift;
    chomp($line);

    $line =~ s/\\ /ESCAPEDSPACE/g;
    $line =~ s/\\,/ESCAPEDCOMMA/g;
    $line =~ s/\\"/ESCAPEDDBLQUOTE/g;

    $line=~/^(.*?) (.*) (.*)$/;
    my ($key, $fields, $timestamp) = ( $1, $2, $3);

    my ( $measurement, @taglist ) = split( /,/, $key );
    $measurement =~ s/ESCAPEDSPACE/ /g;
    $measurement =~ s/ESCAPEDCOMMA/,/g;

    my $tags;
    foreach my $tagset (@taglist) {
        $tagset =~ s/ESCAPEDSPACE/ /g;
        $tagset =~ s/ESCAPEDCOMMA/,/g;
        my ( $k, $v ) = split( /=/, $tagset );
        $tags->{$k} = $v;
    }

    my $values;
    my @strings;
    if ($fields =~ /"/) {
        my $cnt=0;
        $fields=~s/"(.*?)"/push(@strings, $1); 'ESCAPEDSTRING_'.$cnt++;/ge;
    }
    foreach my $valset ( split( /,/, $fields ) ) {
        $valset =~ s/ESCAPEDSPACE/ /g;
        $valset =~ s/ESCAPEDCOMMA/,/g;
        my ( $k, $v ) = split( /=/, $valset );
        $v =~ s/ESCAPEDSTRING_(\d+)/$strings[$1]/ge;
        $v =~ s/ESCAPEDDBLQUOTE/"/g;
        $v =~ s/^(-?\d+)i$/$1/;
        $values->{$k} = $v;
    }

    return ( $measurement, $values, $tags, $timestamp );
}

sub _data2line_0_9_2 {
    my ( $measurement, $values, $tags, $timestamp ) = @_;

    if ( @_ == 1 ) {
        # no $fields, so assume we already got a line
        return $measurement;
    }

    my $key = $measurement;
    $key =~ s/([, ])/\\$1/g;

    # $tags has to be a hashref, if it's not, we dont have tags, so it's the timestamp
    if ( defined $tags ) {
        if ( ref($tags) eq 'HASH' ) {
            my @tags;
            foreach my $k ( sort keys %$tags )
            {    # Influx wants the tags presorted
                # TODO check if sorting algorithm matches
                #      http://golang.org/pkg/bytes/#Compare
                my $v = $tags->{$k};
                next unless defined $v;
                $k =~ s/([, ])/\\$1/g;
                $v =~ s/([, ])/\\$1/g;
                push( @tags, $k . '=' . $v );
            }
            $key .= join( ',', '', @tags ) if @tags;
        }
        elsif ( !ref($tags) ) {
            $timestamp = $tags;
        }
    }

    if ($timestamp) {
        croak("$timestamp does not look like an epoch timestamp")
            unless $timestamp =~ /^\d+$/;
        if ( length($timestamp) < 19 ) {
            my $missing = 19 - length($timestamp);
            my $zeros   = 0 x $missing;
            $timestamp .= $zeros;
        }
    }
    else {
        $timestamp = join( '', gettimeofday(), '000' );
        $timestamp .= '0' if length($timestamp) < 19;
    }

    # If values is not a hashref, convert it into one
    $values = { value => $values } if (not ref($values));

    my @fields;
    foreach my $k ( sort keys %$values ) {
        my $v = $values->{$k};
        $k =~ s/([, ])/\\$1/g;

        if (
            # positive & negativ ints, exponentials, use Regexp::Common?
            $v !~ /^-?\d+(?:\.\d+)?(?:e-?\d+)?$/
            &&
            # perl 5.12 Regexp::Assemble->new->add(qw(t T true TRUE f F false FALSE))->re;
            $v !~ /^(?:F(?:ALSE)?|f(?:alse)?|T(?:RUE)?|t(?:rue)?)$/
        )
        {
            $v =~ s/"/\\"/g;
            $v = '"' . $v . '"';
        }
        push( @fields, $k . '=' . $v );
    }
    my $fields = join( ',', @fields );

    return sprintf( "%s %s %s", $key, $fields, $timestamp );
}

sub _line2data_0_9_2 {
    my $line = shift;
    chomp($line);

    $line =~ s/\\ /ESCAPEDSPACE/g;
    $line =~ s/\\,/ESCAPEDCOMMA/g;
    $line =~ s/\\"/ESCAPEDDBLQUOTE/g;

    $line=~/^(.*?) (.*) (.*)$/;
    my ($key, $fields, $timestamp) = ( $1, $2, $3);

    my ( $measurement, @taglist ) = split( /,/, $key );
    $measurement =~ s/ESCAPEDSPACE/ /g;
    $measurement =~ s/ESCAPEDCOMMA/,/g;

    my $tags;
    foreach my $tagset (@taglist) {
        $tagset =~ s/ESCAPEDSPACE/ /g;
        $tagset =~ s/ESCAPEDCOMMA/,/g;
        my ( $k, $v ) = split( /=/, $tagset );
        $tags->{$k} = $v;
    }

    my $values;
    my @strings;
    if ($fields =~ /"/) {
        my $cnt=0;
        $fields=~s/"(.*?)"/push(@strings, $1); 'ESCAPEDSTRING_'.$cnt++;/ge;
    }
    foreach my $valset ( split( /,/, $fields ) ) {
        $valset =~ s/ESCAPEDSPACE/ /g;
        $valset =~ s/ESCAPEDCOMMA/,/g;
        my ( $k, $v ) = split( /=/, $valset );
        $v =~ s/ESCAPEDSTRING_(\d+)/$strings[$1]/ge;
        $v =~ s/ESCAPEDDBLQUOTE/"/g;
        $values->{$k} = $v;
    }

    return ( $measurement, $values, $tags, $timestamp );
}

1;

__END__

=head1 SYNOPSIS

  use InfluxDB::LineProtocol qw(data2line line2data);

  # convert some Perl data into InfluxDB LineProtocol
  my $influx_line = data2line('measurement', 42);
  my $influx_line = data2line('measurement', { cost => 42 });
  my $influx_line = data2line('measurement', 42, { tag => 'foo'} );

  # convert InfluxDB Line back into Perl
  my ($measurement, $values, $tags, $timestamp) =
    line2data("metric,location=eu,server=srv1 value=42 1437072299900001000");

=head1 DESCRIPTION

L<InfluxDB|https://influxdb.com> is a rather new time series database.
Since version 0.9 they use their
L<LineProtocol|https://influxdb.com/docs/v0.9/write_protocols/line.html>
to write time series data into the database. This module allows you to
generate such a line from a datastructure, handling all the the annoying
escaping and sorting for you. You can also use it to parse a line
(maybe you want to add some tags to a line written by another app).

Please read the InfluxDB docs so you understand how metrics, values
and tags work.

C<InfluxDB::LineProtocol> will always try to implement the most
current version of the InfluxDB line protocol, while allowing you to
also get the old behaviour. Currently we support C<0.9.3> per default,
and C<0.9.2> if you ask nicely.

=head2 FUNCTIONS

=head3 data2line

 data2line($metric, $single_value);
 data2line($metric, $values_hashref);
 data2line($metric, $value, $tags_hashref);
 data2line($metric, $value, $nanoseconds);
 data2line($metric, $value, $tags_hashref, $nanoseconds);

C<data2line> takes various parameters and converts them to an
InfluxDB Line.

C<metric> has to be valid InfluxDB measurement name. Required.

C<value> can be either a scalar, which will be turned into
"value=$value"; or a hashref, if you want to write several values (or
a value with another name than "value"). Required.

C<tags_hashref> is an optional hashref of tag-names and tag-values.

C<nanoseconds> is an optional integer representing nanoseconds since
the epoch. If you do not pass it, C<InfluxDB::LineProtocol> will use
C<Time::HiRes> to get the current timestamp.

=head3 line2data

  my ($metric, $value_hashref, $tags_hashref, $timestamp) = line2data( $line );

C<line2data> parses an InfluxDB line and allways returns 4 values.

C<tags_hashref> is undef if there are no tags!

=head1 LOADING LEGACY PROTOCOL VERSIONS

To use an old version of the line protocol, specify the version you
want when loading C<InfluxDB::LineProtocol>:

  use InfluxDB::LineProtocol qw(v0.9.2 data2line);

You will get a version of C<data2line> that conforms to the C<0.9.2>
version of the line protocol.

Currently supported version are:

=over

=item * 0.9.3

default, no need to specify anything

=item * 0.9.2

load via C<v0.9.2>

=back

=head1 TODO

=over

=item * check if tag sorting algorithm matches
http://golang.org/pkg/bytes/#Compare

=back

=head1 SEE ALSO

=over

=item *

L<InfluxDB|https://metacpan.org/pod/InfluxDB> provides access to the
old 0.8 API. It also allows searching etc.

=item *

L<AnyEvent::InfluxDB|https://metacpan.org/pod/AnyEvent::InfluxDB> - An
asynchronous library for InfluxDB time-series database. Does not
implement escaping etc, so if you want to use AnyEvent::InfluxDB to
send data to InfluxDB you can use InfluxDB::LineProtocol to convert
your measurement data structure before sending it via
AnyEvent::InfluxDB.

=back

=head1 THANKS

Thanks to

=over

=item *

L<validad.com|http://www.validad.com/> for funding the
development of this code.

=item *

L<Jose Luis Martinez|https://github.com/pplu> for implementing
negative & exponential number support and pointing out the change in
the line protocol in 0.9.3.

=item *

L<mvgrimes|https://github.com/mvgrimes> for fixing a bug when
nanosecond timestamps cause some Perls to render the timestamp in
scientific notation.

=back

