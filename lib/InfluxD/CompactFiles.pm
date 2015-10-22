package InfluxD::CompactFiles;
use strict;
use warnings;
use feature 'say';

use Moose;
use Carp qw(croak);
use Log::Any qw($log);
use File::Spec::Functions;
use Sys::Hostname qw(hostname);
use InfluxDB::LineProtocol qw(line2data data2line);


has 'dir'    => ( is => 'ro', isa => 'Str',     required  => 1 );
has 'tags'   => ( is => 'ro', isa => 'HashRef', predicate => 'has_tags' );
has 'delete' => ( is => 'ro', isa => 'Bool',    default   => 1 );

sub run {
    my $self = shift;

    unless ( -d $self->dir ) {
        croak "Not a directory: " . $self->dir;
    }

    my $now = `date -Iseconds`;
    chomp($now);
    my $outfile = join('_',hostname(),'stats',$now) . '.compacted';

    my $target = catfile( $self->dir, $outfile );
    open( my $out, ">>", $target ) || die $!;
    $log->infof( "Starting InfluxD::CompactFiles of directory %s into %s",
        $self->dir, $target );

    opendir( my $dh, $self->dir );
    while ( my $file = readdir($dh) ) {
        next unless $file =~ /\.stats$/;
        $file =~ /(\d+)\.stats/;
        my $pid = $1;
        my $is_running = kill 0, $pid;
        if ($is_running) {
            $log->debugf( "Skip file %s because pid %i is still running",
                $file, $pid );
            next;
        }
        else {
            $log->infof( "Append file %s to %s", $file, $outfile );
            my $source = catfile( $self->dir, $file );
            open( my $fh, "<", $source );
            while ( my $line = <$fh> ) {
                if ( $self->has_tags ) {
                    $line = $self->add_tags_to_line($line);
                }
                say $out $line;
            }
            if ( $self->delete ) {
                unlink($source) || die "$!";
            }
        }
    }
    system('gzip', $target);
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
