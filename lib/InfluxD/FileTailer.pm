package InfluxD::FileTailer;
use strict;
use warnings;
use feature 'say';

use Moose;
use IO::Async::File;
use IO::Async::FileStream;
use IO::Async::Loop;
use Hijk ();
use Carp qw(croak);
#use Measure::Everything::InfluxDB::Utils qw(line2data data2line);
use InfluxDB::LineProtocol qw(line2data data2line);
use Log::Any qw($log);
use File::Spec::Functions;

has 'dir'         => ( is => 'ro', isa => 'Str', required => 1 );
has 'influx_host' => ( is => 'ro', isa => 'Str', required => 1 );
has 'influx_port' =>
    ( is => 'ro', isa => 'Int', default => 8086, required => 1 );
has 'influx_db' => ( is => 'ro', isa => 'Str', required => 1 );

has 'flush_size' =>
    ( is => 'ro', isa => 'Int', required => 1, default => 1000 );
has 'flush_interval' =>
    ( is => 'ro', isa => 'Int', required => 1, default => 30 );
has 'tags' => ( is => 'ro', isa => 'HashRef', predicate => 'has_tags' );
has '_files' => ( is => 'ro', isa => 'HashRef', default => sub { {} } );
has '_loop' => ( is => 'ro', isa => 'IO::Async::Loop', lazy_build => 1 );

sub _build__loop {
    return IO::Async::Loop->new;
}

my @buffer;

sub run {
    my $self = shift;

    unless ( -d $self->dir ) {
        croak "Not a directory: " . $self->dir;
    }

    $log->infof( "Starting InfluxD::FileTailer in directory %s", $self->dir );

    $self->watch_dir;

    my $dir = IO::Async::File->new(
        filename         => $self->dir,
        on_mtime_changed => sub {
            $self->watch_dir;
        },
    );

    $self->_loop->add($dir);

    my $timer = IO::Async::Timer::Periodic->new(    # could be Countdown
        interval => $self->flush_interval,
        on_tick  => sub {
            $self->send;
        },
    );
    $timer->start;
    $self->_loop->add($timer);

    $self->_loop->run;
}

sub watch_dir {
    my ($self) = @_;

    $log->infof( "Checking for new files to watch in %s", $self->dir );
    opendir( my $dh, $self->dir );
    while ( my $f = readdir($dh) ) {
        next unless $f =~ /\.stats$/;
        if ( my $watcher =
            $self->setup_file_watcher( catfile( $self->dir, $f ) ) ) {
            $self->_loop->add($watcher);
        }
    }
    closedir($dh);
}

sub setup_file_watcher {
    my ( $self, $file ) = @_;

    $file =~ /(\d+)\.stats/;
    my $pid = $1;
    my $is_running = kill 0, $pid;
    unless ($is_running) {
        if ( my $w = $self->_files->{$file} ) {
            $self->_loop->remove($w);
            undef $w;
            delete $self->_files->{$file};
            $log->infof( "Removed watcher for %s because pid %i is not more",
                $file, $pid );
        }
        else {
            $log->debugf(
                "Skipping file %s because pid %i seems to be not running.",
                $file, $pid );
        }
        return;
    }

    if ( $self->_files->{$file} ) {
        $log->debugf( "Already watching file %s", $file );
        return;
    }

    if ( open( my $fh, "<", $file ) ) {
        my $filestream = IO::Async::FileStream->new(
            read_handle => $fh,
            on_initial  => sub {
                my ($stream) = @_;
                $stream->seek_to_last("\n");    # TODO remember last position?
            },

            on_read => sub {
                my ( $stream, $buffref ) = @_;

                while ( $$buffref =~ s/^(.*\n)// ) {
                    my $line = $1;
                    if ( $self->has_tags ) {
                        $line = $self->add_tags_to_line($line);
                    }
                    push( @buffer, $line );
                }

                if ( @buffer > $self->flush_size ) {
                    $self->send;
                }

                return 0;
            },
        );
        $log->infof( "Tailing file %s", $file );
        $self->_files->{$file} = $filestream;
        return $filestream;
    }
    else {
        $log->errorf( "Could not open file %s: %s", $file, $! );
        return;
    }
}

sub send {
    my ($self) = @_;
    return unless @buffer;

    $log->debugf( "Sending %i lines to influx", scalar @buffer );
    my $res = Hijk::request(
        {   method       => "POST",
            host         => $self->influx_host,
            port         => $self->influx_port,
            path         => "/write",
            query_string => "db=" . $self->influx_db,
            body         => join( "\n", @buffer ),
        }
    );
    if ( $res->{status} != 204 ) {
        $log->errorf(
            "Could not send %i lines to influx: %s",
            scalar @buffer,
            $res->{body}
        );
    }
    @buffer = ();
}

sub add_tags_to_line {
    my ( $self, $line ) = @_;

    my ( $measurement, $values, $tags, $timestamp ) = line2data($line);
    my $combined_tags;
    if ($tags) {
        $combined_tags = { %$tags, %{ $self->tags } };
    }
    else {
        $combined_tags = $tags;
    }
    return data2line( $measurement, $values, $combined_tags, $timestamp );
}

__PACKAGE__->meta->make_immutable;
1;
