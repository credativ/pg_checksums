#!/usr/bin/env perl

use strict;
use warnings;
use Cwd;
use Config;
use PostgresNode;
use TestLib;
use Test::More tests => 73;

program_help_ok('pg_checksums');
program_version_ok('pg_checksums');
program_options_handling_ok('pg_checksums');

my $tempdir = TestLib::tempdir;

# Initialize node
my $node = get_new_node('main');
$node->init;

$node->start;
my $pgdata = $node->data_dir;

$node->command_fails(['pg_checksums', '-c'],
        'pg_checksums needs needs target directory specified');

$node->command_fails(['pg_checksums', '-a', '-D', $pgdata],
        'pg_checksums -a needs to run against offfline cluster');

my $checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'off', 'checksums are disabled');

$node->stop;

$node->command_ok(['pg_checksums', '-a', '-D', $pgdata],
        'pg_checksums are activated in offline cluster');

$node->start;

$checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'on', 'checksums are enabled');

# Add set of dummy files with some contents.  These should not be scanned
# by the tool.
append_to_file "$pgdata/global/123.", "foo";
append_to_file "$pgdata/global/123_", "foo";
append_to_file "$pgdata/global/123_.", "foo";
append_to_file "$pgdata/global/123.12t", "foo";
append_to_file "$pgdata/global/foo", "foo2";
append_to_file "$pgdata/global/t123", "bar";
append_to_file "$pgdata/global/123a", "bar2";
append_to_file "$pgdata/global/.123", "foobar";
append_to_file "$pgdata/global/_fsm", "foobar2";
append_to_file "$pgdata/global/_init", "foobar3";
append_to_file "$pgdata/global/_vm.123", "foohoge";
append_to_file "$pgdata/global/123_vm.123t", "foohoge2";

# Those are correct but empty files, so they should pass through.
append_to_file "$pgdata/global/99999", "";
append_to_file "$pgdata/global/99999.123", "";
append_to_file "$pgdata/global/99999_fsm", "";
append_to_file "$pgdata/global/99999_init", "";
append_to_file "$pgdata/global/99999_vm", "";
append_to_file "$pgdata/global/99999_init.123", "";
append_to_file "$pgdata/global/99999_fsm.123", "";
append_to_file "$pgdata/global/99999_vm.123", "";

$node->stop;

$node->command_ok(['pg_checksums', '-b', '-D', $pgdata],
        'pg_checksums are deactivated in offline cluster');

$node->start;

$checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'off', 'checksums are disabled');

$node->stop;

$node->command_ok(['pg_checksums', '-a', '-D', $pgdata],
        'pg_checksums are again activated in offline cluster');

#exit 0;
$node->start;

$node->command_ok(['pg_checksums', '-c', '-D', $pgdata],
        'pg_checksums can be verified in online cluster');

# create table to corrupt and get their relfilenode
create_corruption($node, 'corrupt1', 'pg_default');

$node->command_checks_all([ 'pg_checksums', '-c', '-D', $pgdata],
        1,
        [qr/Bad checksums:  1/s],
        [qr/checksum verification failed/s],
        'pg_checksums reports checksum mismatch'
);

# drop corrupt table again and make sure there is no more corruption
$node->safe_psql('postgres', 'DROP TABLE corrupt1;');
$node->command_ok(['pg_checksums', '-c', '-D', $pgdata],
        'pg_checksums can be verified in online cluster: '.getcwd());


# create table to corrupt in a non-default tablespace and get their relfilenode
my $tablespace_dir = getcwd()."/tmp_check/ts_corrupt_dir";
mkdir ($tablespace_dir);
$node->safe_psql('postgres', "CREATE TABLESPACE ts_corrupt LOCATION '".$tablespace_dir."';");
create_corruption($node, 'corrupt2', 'ts_corrupt');

$node->command_checks_all([ 'pg_checksums', '-c', '-D', $pgdata],
        1,
        [qr/Bad checksums:  1/s],
        [qr/checksum verification failed/s],
        'pg_checksums reports checksum mismatch on non-default tablespace'
);

# drop corrupt table again and make sure there is no more corruption
$node->safe_psql('postgres', 'DROP TABLE corrupt2;');
$node->command_ok(['pg_checksums', '-c', '-D', $pgdata],
        'pg_checksums can be verified in online cluster');

# Utility routine to create a table with corrupted checksums.
# It stops the node (if running), and starts it again.
sub create_corruption
{
	my $node = shift;
	my $table = shift;
	my $tablespace = shift;

	my $query = "SELECT a INTO ".$table." FROM generate_series(1,10000) AS a; ALTER TABLE ".$table." SET (autovacuum_enabled=false), SET TABLESPACE ".$tablespace."; SELECT pg_relation_filepath('".$table."')";
	my $file_name = $node->safe_psql('postgres', $query);

	# set page header and block sizes
	my $pageheader_size = 24;
	my $block_size = $node->safe_psql('postgres', 'SHOW block_size;');

	$node->stop;

	open my $file, '+<', "$pgdata/$file_name";
	seek($file, $pageheader_size, 0);
	syswrite($file, '\0\0\0\0\0\0\0\0\0');
	close $file;

	$node->start;

	return;
}

# Utility routine to check that pg_checksums is able to detect
# correctly-named relation files filled with some corrupted data.
sub fail_corrupt
{
	my $node = shift;
	my $file = shift;
	my $pgdata = $node->data_dir;

	# Create the file with some dummy data in it.
	my $file_name = "$pgdata/global/$file";
	append_to_file $file_name, "foo";

	$node->stop;
	# If the instance is offline, the whole file is skipped and this is
	# considered to be an error.
	$node->command_checks_all([ 'pg_checksums', '-c', '-D', $pgdata],
						1,
						[qr/Files skipped:.*1/],
						[qr/could not read block 0 in file.*$file\":/],
						"skips file for corrupted data in $file when offline");

	$node->start;
	# If the instance is online, the block is skipped and this is not
	# considered to be an error
	$node->command_checks_all([ 'pg_checksums', '-c', '-D', $pgdata],
						0,
						[qr/Blocks skipped:.*1/],
						[qr/^$/],
						"skips block for corrupted data in $file when online");

	# Remove file to prevent future lookup errors on conflicts.
	unlink $file_name;
	return;
}

# Authorized relation files filled with corrupted data cause the
# checksum checks to fail.
fail_corrupt($node, "99990");
fail_corrupt($node, "99990.123");
fail_corrupt($node, "99990_fsm");
fail_corrupt($node, "99990_init");
fail_corrupt($node, "99990_vm");
fail_corrupt($node, "99990_init.123");
fail_corrupt($node, "99990_fsm.123");
fail_corrupt($node, "99990_vm.123");

# Stop node again at the end of tests
$node->stop;
