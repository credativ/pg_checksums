#!/usr/bin/env perl

use strict;
use warnings;
use Cwd;
use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 19;

program_help_ok('pg_checksums');
program_version_ok('pg_checksums');
program_options_handling_ok('pg_checksums');

my $tempdir = TestLib::tempdir;

# Initialize node
my $node = get_new_node('main');
$node->init(allows_streaming => 1);

$node->start;
my $pgdata = $node->data_dir;

$node->command_fails(['pg_checksums', '-c'],
        'pg_checksums needs needs target directory specified');

$node->command_fails(['pg_checksums', '-c', '-D', $pgdata],
        'pg_checksums needs to run against offfline cluster');

my $checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'off', 'checksums are disabled');

$node->stop;

$node->command_ok(['pg_checksums', '-a', '-D', $pgdata],
        'pg_checksums are activated in offline cluster');

$node->start;

$checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'on', 'checksums are enabled');

$node->stop;

$node->command_ok(['pg_checksums', '-b', '-D', $pgdata],
        'pg_checksums are deactivated in offline cluster');

$node->start;

$checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'off', 'checksums are disabled');

$node->stop;

$node->command_ok(['pg_checksums', '-a', '-D', $pgdata],
        'pg_checksums are again activated in offline cluster');

$node->start;

# create table to corrupt and get their relfilenode
my $file_corrupt = $node->safe_psql('postgres',
        q{SELECT a INTO corrupt1 FROM generate_series(1,10000) AS a; ALTER TABLE corrupt1 SET (autovacuum_enabled=false); SELECT pg_relation_filepath('corrupt1')}
);

# set page header and block sizes
my $pageheader_size = 24;
my $block_size = $node->safe_psql('postgres', 'SHOW block_size;');

# induce corruption
$node->stop;
open my $file, '+<', "$pgdata/$file_corrupt";
seek($file, $pageheader_size, 0);
syswrite($file, '\0\0\0\0\0\0\0\0\0');
close $file;

$node->command_checks_all([ 'pg_checksums', '-c', '-D', $pgdata],
        1,
        [qr/Bad checksums:  1/s],
        [qr/checksum verification failed/s],
        'pg_checksums reports checksum mismatch'
);
