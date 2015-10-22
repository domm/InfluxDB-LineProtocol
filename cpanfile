requires 'Moose';
requires 'MooseX::Getopt';
requires 'IO::Async::FileStream';
requires 'IO::Async';
requires 'Hijk';
requires 'lib::projectroot' => '1.004';
requires 'Log::Any::Adapter';

requires 'Time::HiRes';

on 'test' => sub {
  requires 'Test::Most';
}
