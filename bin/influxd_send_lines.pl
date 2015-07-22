#!/usr/bin/env perl
use strict;
use warnings;
use lib::projectroot qw(lib local::lib=local extra=Measure-Everything);

package Runner;
use Moose;
extends 'InfluxD::SendLines';
with 'MooseX::Getopt';

use Log::Any::Adapter ('Stderr');

my $runner = Runner->new_with_options->run;

