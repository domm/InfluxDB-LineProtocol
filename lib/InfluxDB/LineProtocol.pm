package InfluxDB::LineProtocol;
use strict;
use warnings;

our $VERSION = '1.000';

# ABSTRACT: Write and read InfluxDB LineProtocol

use Exporter qw(import);
use Carp qw(croak);
use Time::HiRes qw(gettimeofday);

our @EXPORT_OK = qw(data2line line2data);

sub data2line {
    my ( $measurment, $values, $tags, $timestamp ) = @_;

    if ( @_ == 1 ) {
        # no $fields, so assume we already got a line
        return $measurment;
    }

    my $key = $measurment;
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
        $timestamp = join( '', gettimeofday() ) * 1000;
        $timestamp *= 10 if length($timestamp) < 19;
    }

    # $fields can be a hashref or a scalar
    my $fields;
    my $ref_values = ref($values);
    if ( $ref_values eq 'HASH' ) {
        my @fields;
        foreach my $k ( sort keys %$values ) {
            my $v = $values->{$k};
            $k =~ s/([, ])/\\$1/g;

            # TODO handle booleans
            # TODO handle negative, exponentials
            if ( $v =~ /[^\d\.]/ ) {
                $v =~ s/"/\\"/g;
                $v = '"' . $v . '"';
            }
            push( @fields, $k . '=' . $v );
        }
        $fields = join( ',', @fields );
    }
    elsif ( !$ref_values ) {
        if ( $values =~ /[^\d\.]/ ) {
            $values =~ s/([, ])/\\$1/g;
            $fields = 'value="' . $values . '"';
        }
        else {
            $fields = 'value=' . $values;
        }
    }
    else {
        croak("Invalid fields $ref_values");
    }

    return sprintf( "%s %s %s", $key, $fields, $timestamp );
}

sub line2data {
    my $line = shift;
    chomp($line);

    $line =~ s/\\ /ESCAPEDSPACE/g;
    $line =~ s/\\,/ESCAPEDCOMMA/g;
    my ( $key, $fields, $timestamp ) = split( / /, $line );

    my ( $measurment, @taglist ) = split( /,/, $key );
    $measurment =~ s/ESCAPEDSPACE/ /g;
    $measurment =~ s/ESCAPEDCOMMA/,/g;

    my $tags;
    foreach my $tagset (@taglist) {
        $tagset =~ s/ESCAPEDSPACE/ /g;
        $tagset =~ s/ESCAPEDCOMMA/,/g;
        my ( $k, $v ) = split( /=/, $tagset );
        $tags->{$k} = $v;
    }

    my $values;
    foreach my $valset ( split( /,/, $fields ) ) {
        $valset =~ s/ESCAPEDSPACE/ /g;
        $valset =~ s/ESCAPEDCOMMA/,/g;
        my ( $k, $v ) = split( /=/, $valset );
        $v =~ s/^"//;
        $v =~ s/"$//;
        $values->{$k} = $v;
    }

    return ( $measurment, $values, $tags, $timestamp );
}

1;

__END__

