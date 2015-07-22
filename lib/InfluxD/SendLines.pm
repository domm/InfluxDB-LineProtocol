package InfluxD::SendLines;
use strict;
use warnings;
use feature 'say';

use Moose;
use Carp qw(croak);
use Log::Any qw($log);
use File::Spec::Functions;
use Hijk ();

has 'file'        => ( is => 'ro', isa => 'Str', required => 1 );
has 'influx_host' => ( is => 'ro', isa => 'Str', required => 1 );
has 'influx_port' =>
    ( is => 'ro', isa => 'Int', default => 8086, required => 1 );
has 'influx_db'   => ( is => 'ro', isa => 'Str', required => 1 );
has 'buffer_size' => ( is => 'ro', isa => 'Int', default  => 1000 );

$| = 1;

my @buffer;

sub run {
    my $self = shift;

    $log->infof( "Starting InfluxD::SendLines %s", $self->file );

    my $f     = $self->file;
    my $lines = `wc -l $f`;
    chomp($lines);
    $lines =~ s/ .*//;
    my $total = $lines;
    my $start = scalar time;

    open( my $in, "<", $self->file ) || die $!;

    my $cnt = 0;
    my $print_cnt = $self->buffer_size * 50;
    while ( my $line = <$in> ) {
        push( @buffer, $line );
        if ( @buffer == $self->buffer_size ) {
            $self->send;
        }
        $cnt++;
        if ( $cnt % $print_cnt == 0 ) {
            my $now   = scalar time;
            my $diff  = $now - $start || 1;
            my $speed = $cnt / $diff;
            my $estimate =
                $speed > 0 ? ( $total - $cnt ) / $speed : 'Infinity';
            printf( "  % 6i/%i (%.2f/s) time left: %i sec\n",
                $cnt, $total, $speed, $estimate );
        }
    }
    $self->send;
}

sub send {
    my $self = shift;
    $log->debugf( "Sending %i lines to influx", scalar @buffer );
    my $res = Hijk::request(
        {   method       => "POST",
            host         => $self->influx_host,
            port         => $self->influx_port,
            path         => "/write",
            query_string => "db=" . $self->influx_db,
            body         => join( '', @buffer ),
        }
    );
    if ( $res->{status} != 204 ) {
        $log->errorf(
            "Could not send %i lines to influx: %s",
            scalar @buffer,
            $res->{body}
        );
        open( my $fh, ">>", $self->file . '.err' ) || die $!;
        print $fh join( '', @buffer );
        close $fh;
        print 'X';
    }
    else {
        print '.';
    }
    @buffer = ();
}

__PACKAGE__->meta->make_immutable;
1;
