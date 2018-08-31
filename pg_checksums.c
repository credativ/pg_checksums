/*
 * pg_checksums
 *
 * Activate/deactivate/verify data checksums
 *
 *	Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 *	pg_checksums.c
 */

#define FRONTEND 1

#include "postgres.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#if PG_VERSION_NUM >=  90600
#include "common/controldata_utils.h"
#endif
#if PG_VERSION_NUM >= 110000
#include "common/file_perm.h"
#else
#define pg_file_create_mode 0600
#endif
#if PG_VERSION_NUM >= 100000
#include "common/file_utils.h"
#endif
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"

#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>

#if PG_VERSION_NUM <  90400
#include <unistd.h>
#include <getopt.h>
extern char *optarg;
#else
#include "pg_getopt.h"
#endif

#if PG_VERSION_NUM < 100000
#define PG_CONTROL_FILE_SIZE PG_CONTROL_SIZE
#endif

#if PG_VERSION_NUM <  90500
#define INIT_CRC32C INIT_CRC32
#define COMP_CRC32C COMP_CRC32
#define FIN_CRC32C FIN_CRC32
#define INT64_MODIFIER "l"
#endif

static int64 files = 0;
static int64 blocks = 0;
static int64 badblocks = 0;
static ControlFileData *ControlFile;

static char *only_relfilenode = NULL;
static bool debug = false;
static bool verbose = false;
static bool verify = false;
static bool activate = false;
static bool deactivate = false;
static bool show_progress = false;

static const char *progname;

/*
 * Progress status information.
 */
int64		total_size = 0;
int64		current_size = 0;
pg_time_t	last_progress_update;
pg_time_t	scan_started;

static void updateControlFile(char *DataDir, ControlFileData *ControlFile);
#if PG_VERSION_NUM < 100000
#define MAXCMDLEN (2 * MAXPGPATH)
char		initdb_path[MAXPGPATH];

static void findInitDB(const char *argv0);
static void syncDataDir(char *DataDir);
#endif
#if PG_VERSION_NUM < 90600
static ControlFileData *getControlFile(char *DataDir);
#endif

static void
usage()
{
	printf(_("%s activates/deactivates/verifies page level checksums in PostgreSQL database cluster.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION] [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D] DATADIR    data directory\n"));
	printf(_("  -a,            activate checksums\n"));
	printf(_("  -b,            deactivate checksums\n"));
	printf(_("  -c,            verify checksums\n"));
	printf(_("  -r relfilenode check only relation with specified relfilenode\n"));
	printf(_("  -d, --debug    debug output\n"));
	printf(_("  -v, --verbose  output verbose messages\n"));
	printf(_("  -P             show progress information\n"));
	printf(_("  -V, --version  output version information, then exit\n"));
	printf(_("  -?, --help     show this help, then exit\n"));
	printf(_("\nOne of -a, -b or -c is mandatory. If no data directory "
			 "(DATADIR) is specified,\nthe environment variable "
			 "PGDATA is used.\n\n"));
	printf(_("Report bugs to https://github.com/credativ/pg_checksums/issues/new.\n"));
}

static const char *skip[] = {
	"pg_control",
	"pg_filenode.map",
	"pg_internal.init",
	"pg_stat_statements.stat",
	"PG_VERSION",
	"pgsql_tmp",
	NULL,
};

static void
enable_progress_report(int signo,
					   siginfo_t * siginfo,
					   void *context)
{

	/* we handle SIGUSR1 only */
	if (signo == SIGUSR1)
	{
		show_progress = true;
	}

}

/*
 * Report current progress status. Parts borrowed from
 * PostgreSQLs' src/bin/pg_basebackup.c
 */
static void
report_progress(bool force)
{
	pg_time_t	now = time(NULL);
	int			total_percent = 0;

	char		totalstr[32];
	char		currentstr[32];
	char		currspeedstr[32];

	/* Make sure we just report at least once a second */
	if ((now == last_progress_update) && !force)
		return;

	/* Save current time */
	last_progress_update = now;

	/*
	 * Calculate current percent done, based on KiB...
	 */
	total_percent = total_size ? (int64) ((current_size / 1024) * 100 / (total_size / 1024)) : 0;

	/* Don't display larger than 100% */
	if (total_percent > 100)
		total_percent = 100;

	/* The same for total size */
	if (current_size > total_size)
		total_size = current_size / 1024;

	snprintf(totalstr, sizeof(totalstr), INT64_FORMAT,
			 total_size / 1024);
	snprintf(currentstr, sizeof(currentstr), INT64_FORMAT,
			 current_size / 1024);
	snprintf(currspeedstr, sizeof(currspeedstr), INT64_FORMAT,
			 (current_size / 1024) / (((time(NULL) - scan_started) == 0) ? 1 : (time(NULL) - scan_started)));
	fprintf(stderr, "%s/%s kB (%d%%, %s kB/s)",
			currentstr, totalstr, total_percent, currspeedstr);

	if (isatty(fileno(stderr)))
		fprintf(stderr, "\r");
	else
		fprintf(stderr, "\n");
}

static bool
skipfile(char *fn)
{
	const char **f;

	if (strcmp(fn, ".") == 0 ||
		strcmp(fn, "..") == 0)
		return true;

	for (f = skip; *f; f++)
		if (strcmp(*f, fn) == 0)
			return true;
	return false;
}

static void
scan_file(char *fn, int segmentno, bool sizeonly)
{
	char	   *buf = palloc(BLCKSZ);
	PageHeader	header = (PageHeader) buf;
	int			f;
	int			blockno;
	bool		block_retry = false;

	f = open(fn, O_RDWR);
	if (f < 0)
	{
		fprintf(stderr, _("%s: could not open file \"%s\": %m\n"), progname, fn);
		exit(1);
	}

	files++;

	for (blockno = 0;; blockno++)
	{
		uint16		csum;
		int			r = read(f, buf, BLCKSZ);

		if (debug && block_retry)
			fprintf(stderr, _("%s: retrying block %d in file \"%s\"\n"),
					progname, blockno, fn);

		if (r == 0)
		{
			if (debug && block_retry)
				fprintf(stderr, _("%s: block %d in file \"%s\" is EOF, ignoring\n"),
						progname, blockno, fn);
			break;
		}

		if (r != BLCKSZ)
		{
			if (block_retry)
			{
				/* We already tried once to reread the block, bail out */
				fprintf(stderr, _("%s: short read of block %d in file \"%s\", got only %d bytes\n"),
						progname, blockno, fn, r);
				exit(1);
			}

			if (debug)
				fprintf(stderr, _("%s: short read of block %d in file \"%s\", got only %d bytes\n"),
						progname, blockno, fn, r);

			/*
			 * Retry the block. It's possible that we read the block while it
			 * was extended or shrinked, so it it ends up looking torn to us.
			 * If there were less than 10 bad blocks so far, we wait for 500ms
			 * before we retry.
			 */
			if (badblocks < 10)
				pg_usleep(500);

			/*
			 * Seek back by the amount of bytes we read to the beginning of
			 * the failed block
			 */
			if (lseek(f, -r, SEEK_CUR) == -1)
			{
				fprintf(stderr, _("%s: could not lseek in file \"%s\": %m\n"),
						progname, fn);
				exit(1);
			}

			/* Set flag so we know a retry was attempted */
			block_retry = true;

			/* Reset loop to validate the block again */
			blockno--;

			continue;
		}
		blocks++;

		/* New pages have no checksum yet */
		if (PageIsNew(buf))
		{
			if (debug && block_retry)
				fprintf(stderr, _("%s: block %d in file \"%s\" is new, ignoring\n"),
						progname, blockno, fn);
			block_retry = false;
			continue;
		}

		csum = pg_checksum_page(buf, blockno + segmentno * RELSEG_SIZE);
		current_size += r;

		if (verify)
		{
			if (csum != header->pd_checksum)
			{
				/*
				 * Retry the block on the first failure.  It's possible that
				 * we read the first 4K page of the block just before postgres
				 * updated the entire block so it ends up looking torn to us.
				 * If there were less than 10 bad blocks so far, we wait for
				 * 500ms before we retry.
				 */
				if (block_retry == false)
				{
					if (badblocks < 10)
						pg_usleep(500);

					/* Seek to the beginning of the failed block */
					if (lseek(f, -BLCKSZ, SEEK_CUR) == -1)
					{
						fprintf(stderr, _("%s: could not lseek in file \"%s\": %m\n"),
								progname, fn);
						exit(1);
					}

					/* Set flag so we know a retry was attempted */
					block_retry = true;

					if (debug)
						fprintf(stderr, _("%s: checksum verification failed on first attempt in file \"%s\", block %d: calculated checksum %X but expected %X\n"),
								progname, fn, blockno, csum, header->pd_checksum);

					/* Reset loop to validate the block again */
					blockno--;
					blocks--;
					current_size -= r;

					continue;
				}

				if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION)
					fprintf(stderr, _("%s: checksum verification failed in file \"%s\", block %d: calculated checksum %X but expected %X\n"),
							progname, fn, blockno, csum, header->pd_checksum);
				badblocks++;
			}
			else if (block_retry && verbose)
				fprintf(stderr, _("%s: block %d in file \"%s\" verified ok on recheck\n"),
						progname, blockno, fn);

			block_retry = false;

			if (show_progress)
				report_progress(false);
		}
		else if (activate)
		{
			if (debug)
				fprintf(stderr, _("%s: checksum set in file \"%s\", block %d: %X\n"),
						progname, fn, blockno, csum);

			/* Set checksum in page header */
			header->pd_checksum = csum;

			/* Seek back to beginning of block */
			if (lseek(f, -BLCKSZ, SEEK_CUR) == -1)
			{
				fprintf(stderr, _("%s: seek failed: %ld\n"), progname, lseek(f, 0, SEEK_CUR));
				exit(1);
			}

			/* Write block with checksum */
			if (write(f, buf, BLCKSZ) == -1)
			{
				fprintf(stderr, _("%s: write failed: %s\n"), progname, strerror(errno));
				exit(1);
			}

			if (show_progress)
				report_progress(false);
		}
	}

	if (verify)
	{
		if (verbose)
			fprintf(stderr,
				_("%s: checksums verified in file \"%s\"\n"), progname, fn);
	}
	else if (activate)
	{
		if (verbose)
			fprintf(stderr,
				_("%s: checksums activated in file \"%s\"\n"), progname, fn);
	}

	close(f);

	pfree(buf);
}

static int64
scan_directory(char *basedir, char *subdir, bool sizeonly)
{
	int64		dirsize = 0;
	char		path[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(path, sizeof(path), "%s/%s", basedir, subdir);
	dir = opendir(path);
	if (!dir)
	{
		fprintf(stderr, _("%s: could not open directory \"%s\": %m\n"),
				progname, path);
		exit(1);
	}
	while ((de = readdir(dir)) != NULL)
	{
		char		fn[MAXPGPATH + 1];
		struct stat st;

		if (skipfile(de->d_name))
			continue;

		snprintf(fn, sizeof(fn), "%s/%s", path, de->d_name);
		if (lstat(fn, &st) < 0)
		{
			if (errno == ENOENT)
			{
				/* File was removed in the meantime */
				if (debug)
					fprintf(stderr, _("%s: ignoring deleted file \"%s\"\n"),
							progname, fn);
				continue;
			}

			fprintf(stderr, _("%s: could not stat file \"%s\": %m\n"),
					progname, fn);
			exit(1);
		}
		if (S_ISREG(st.st_mode))
		{
			char	   *forkpath,
					   *segmentpath;
			int			segmentno = 0;

			/*
			 * Cut off at the segment boundary (".") to get the segment number
			 * in order to mix it into the checksum. Then also cut off at the
			 * fork boundary, to get the relfilenode the file belongs to for
			 * filtering.
			 */
			segmentpath = strchr(de->d_name, '.');
			if (segmentpath != NULL)
			{
				*segmentpath++ = '\0';
				segmentno = atoi(segmentpath);
				if (segmentno == 0)
				{
					fprintf(stderr, _("%s: invalid segment number %d in filename \"%s\"\n"),
							progname, segmentno, fn);
					exit(1);
				}
			}

			forkpath = strchr(de->d_name, '_');
			if (forkpath != NULL)
				*forkpath++ = '\0';

			if (only_relfilenode && strcmp(only_relfilenode, de->d_name) != 0)
				/* Relfilenode not to be included */
				continue;

			dirsize += st.st_size;

			if (!sizeonly)
			{
				scan_file(fn, segmentno, sizeonly);
			}
		}
#ifndef WIN32
		else if (S_ISDIR(st.st_mode) || S_ISLNK(st.st_mode))
#else
		else if (S_ISDIR(st.st_mode) || pgwin32_is_junction(fn))
#endif
			dirsize += scan_directory(path, de->d_name, sizeonly);
	}
	closedir(dir);
	return dirsize;
}

/*
 * getControlFile() needs our own implementation in
 * PostgreSQL versiones below 9.6. If built against newer version,
 * use the get_controlfile() function provided there.
 */
#if PG_VERSION_NUM < 90600

/*
 * Read in the control file.
 */
static ControlFileData *
getControlFile(char *DataDir)
{
	ControlFileData *ControlFile;
	int			fd;
	char		ControlFilePath[MAXPGPATH];

	ControlFile = palloc(sizeof(ControlFileData));
	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, ControlFilePath, strerror(errno));
		exit(1);
	}

	if (read(fd, ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
	{
		fprintf(stderr, _("%s: could not read file \"%s\": %s\n"),
				progname, ControlFilePath, strerror(errno));
		exit(1);
	}

	close(fd);

	return ControlFile;
}

#endif

/*
 * Update the control file.
 */
static void
updateControlFile(char *DataDir, ControlFileData *ControlFile)
{
	int			fd;
	char		buffer[PG_CONTROL_FILE_SIZE];
	char		ControlFilePath[MAXPGPATH];

	/*
	 * For good luck, apply the same static assertions as in backend's
	 * WriteControlFile().
	 */
#if PG_VERSION_NUM >= 100000
	StaticAssertStmt(sizeof(ControlFileData) <= PG_CONTROL_MAX_SAFE_SIZE,
					 "pg_control is too large for atomic disk writes");
#endif
	StaticAssertStmt(sizeof(ControlFileData) <= PG_CONTROL_FILE_SIZE,
					 "sizeof(ControlFileData) exceeds PG_CONTROL_FILE_SIZE");

	/* Recalculate CRC of control file */
	INIT_CRC32C(ControlFile->crc);
	COMP_CRC32C(ControlFile->crc,
				(char *) ControlFile,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(ControlFile->crc);

	/*
	 * Write out PG_CONTROL_FILE_SIZE bytes into pg_control by zero-padding
	 * the excess over sizeof(ControlFileData), to avoid premature EOF related
	 * errors when reading it.
	 */
	memset(buffer, 0, PG_CONTROL_FILE_SIZE);
	memcpy(buffer, ControlFile, sizeof(ControlFileData));

	snprintf(ControlFilePath, sizeof(ControlFilePath), "%s/%s", DataDir, XLOG_CONTROL_FILE);

	if (debug)
		fprintf(stderr, _("%s: pg_control is file \"%s\"\n"), progname, ControlFilePath);

	unlink(ControlFilePath);

	fd = open(ControlFilePath,
			  O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
			  pg_file_create_mode);
	if (fd < 0)
	{
		fprintf(stderr, _("%s: could not create pg_control file: %s\n"),
				progname, strerror(errno));
		exit(1);
	}

	errno = 0;
	if (write(fd, buffer, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		fprintf(stderr, _("%s: could not write pg_control file: %s\n"),
				progname, strerror(errno));
		exit(1);
	}

	if (fsync(fd) != 0)
	{
		fprintf(stderr, _("%s: fsync error: %s\n"), progname, strerror(errno));
		exit(1);
	}

	if (close(fd) < 0)
	{
		fprintf(stderr, _("%s: could not close control file: %s\n"), progname, strerror(errno));
		exit(1);
	}
}

/*
 * syncDataDir() and findInitDB are used on PostgreSQL releases < 10, only
 */
#if PG_VERSION_NUM < 100000

static void
findInitDB(const char *argv0)
{
	int			ret;

	/* locate initdb binary */
	if ((ret = find_other_exec(argv0, "initdb",
							   "initdb (PostgreSQL) " PG_VERSION "\n",
							   initdb_path)) < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv0, full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
		{
			fprintf(stderr, _("%s: program \"initdb\" not found\n"), progname);
			exit(1);
		}
		else
		{
			fprintf(stderr, _("%s: program \"initdb\" has different version\n"), progname);
			exit(1);
		}
	}

}

/*
 * Sync target data directory to ensure that modifications are safely on disk.
 *
 * We do this once, for the whole data directory, for performance reasons.  At
 * the end of pg_rewind's run, the kernel is likely to already have flushed
 * most dirty buffers to disk. Additionally initdb -S uses a two-pass approach
 * (only initiating writeback in the first pass), which often reduces the
 * overall amount of IO noticeably.
 */
static void
syncDataDir(char *DataDir)
{
	char		cmd[MAXCMDLEN];


	/* finally run initdb -S */
	if (debug)
		snprintf(cmd, MAXCMDLEN, "\"%s\" -D \"%s\" -S",
				 initdb_path, DataDir);
	else
		snprintf(cmd, MAXCMDLEN, "\"%s\" -D \"%s\" -S > \"%s\"",
				 initdb_path, DataDir, DEVNULL);

	if (system(cmd) != 0)
	{
		fprintf(stderr, _("%s: sync of target directory failed\n"), progname);
		exit(1);
	}
}

#endif

int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"debug", no_argument, NULL, 'd'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	char	   *DataDir = NULL;
	int			c;
	int			option_index;
#if PG_VERSION_NUM >= 100000
	bool		crc_ok;
#endif

	struct sigaction act;		/* to turn progress status info on */

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_checksums"));

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_checksums (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:abcr:dvP", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'a':
				activate = true;
				break;
			case 'b':
				deactivate = true;
				break;
			case 'c':
				verify = true;
				break;
			case 'd':
				debug = true;
				verbose = true;
				break;
			case 'D':
				DataDir = optarg;
				break;
			case 'r':
				if (atoi(optarg) <= 0)
				{
					fprintf(stderr, _("%s: invalid relfilenode: %s\n"), progname, optarg);
					exit(1);
				}
				only_relfilenode = pstrdup(optarg);
				break;
			case 'P':
				show_progress = true;
				break;
			case 'v':
				verbose = true;
				break;
			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	if (DataDir == NULL)
	{
		if (optind < argc)
			DataDir = argv[optind++];
		else
			DataDir = getenv("PGDATA");

		/* If no DataDir was specified, and none could be found, error out */
		if (DataDir == NULL)
		{
			fprintf(stderr, _("%s: no data directory specified\n"), progname);
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Complain if no action has been requested */
	if (!verify && !activate && !deactivate)
	{
		fprintf(stderr, _("%s: No action has been specified\n"),
				progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Disallow execution by the root user */
	if (geteuid() == 0)
	{
		fprintf(stderr, _("%s: cannot be executed by \"root\"\n"),
				progname);
		exit(1);
	}

	/* Check if cluster is running */
#if PG_VERSION_NUM >= 100000
	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
	if (!crc_ok)
	{
		fprintf(stderr, _("%s: pg_control CRC value is incorrect.\n"), progname);
		exit(1);
	}
#elif PG_VERSION_NUM >= 90600
	ControlFile = get_controlfile(DataDir, progname);
#else
	ControlFile = getControlFile(DataDir);
#endif

	/*
	 * Cluster must be shut down for activation/deactivation of checksums, but
	 * online verification is supported.
	 */
	if (ControlFile->state != DB_SHUTDOWNED &&
		ControlFile->state != DB_SHUTDOWNED_IN_RECOVERY &&
		!verify)
	{
		fprintf(stderr, _("%s: cluster must be shut down.\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == 0 && !activate)
	{
		fprintf(stderr, _("%s: data checksums are not enabled in cluster.\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == 1 && activate)
	{
		fprintf(stderr, _("%s: data checksums are already enabled in cluster.\n"), progname);
		exit(1);
	}

	/*
	 * Check that the PGDATA blocksize is the same as the one pg_checksums
	 * was compiled against (BLCKSZ).
	 */
	if (ControlFile->blcksz != BLCKSZ)
	{
		fprintf(stderr, _("%s: data directory block size %d is different to compiled-in block size %d.\n"),
				progname, ControlFile->blcksz, BLCKSZ);
		exit(1);
	}

	if (activate || verify)
	{
#if PG_VERSION_NUM < 100000
		/* Check for initdb */
		if (activate)
			findInitDB(argv[0]);
#endif

		/*
		 * Assign SIGUSR1 signal handler to turn progress status information
		 * on in case someone has forgotten -P (and doesn't want to
		 * restart)...
		 */
		memset(&act, '\0', sizeof(act));
		act.sa_sigaction = &enable_progress_report;
		act.sa_flags = SA_SIGINFO;

		/*
		 * Enable signal handler, but don't treat it as severe if we don't
		 * succeed here. Just give a message on STDERR.
		 */
		if (sigaction(SIGUSR1, &act, NULL) < 0)
		{
			fprintf(stderr, "could not set signal handler to turn on progress bar\n");
		}

		/*
		 * Iff progress status information is requested, we need to scan the
		 * directory tree(s) twice, once to get the idea how much data we need
		 * to scan and finally to do the real legwork.
		 */
		total_size = scan_directory(DataDir, "global", true);
		total_size += scan_directory(DataDir, "base", true);
		total_size += scan_directory(DataDir, "pg_tblspc", true);

		/*
		 * Remember start time. Required to calculate the current speed in
		 * report_progress()
		 */
		scan_started = time(NULL);

		/* Scan all files */
		scan_directory(DataDir, "global", false);
		scan_directory(DataDir, "base", false);
		scan_directory(DataDir, "pg_tblspc", false);

		/*
		 * Done. Move to next line in case progress information was shown.
		 * Otherwise we clutter the summary output.
		 */
		if (show_progress)
		{
			report_progress(true);
			if (isatty(fileno(stderr)))
				fprintf(stderr, "\n");
		}

		/*
		 * Print summary and we're done.
		 */
		printf(_("Checksum scan completed\n"));
		printf(_("Files scanned:  %" INT64_MODIFIER "d\n"), files);
		printf(_("Blocks scanned: %" INT64_MODIFIER "d\n"), blocks);

		if (verify)
			printf(_("Bad checksums:  %" INT64_MODIFIER "d\n"), badblocks);
		else
		{
			printf(_("Syncing data directory\n"));
#if PG_VERSION_NUM >= 100000
			fsync_pgdata(DataDir, progname, PG_VERSION_NUM);
#else
			syncDataDir(DataDir);
#endif
		}
	}

	if (activate || deactivate)
	{
		if (activate)
		{
			ControlFile->data_checksum_version = 1;
			updateControlFile(DataDir, ControlFile);
			printf(_("Checksums activated\n"));
		}
		else
		{
			ControlFile->data_checksum_version = 0;
			updateControlFile(DataDir, ControlFile);
			printf(_("Checksums deactivated\n"));
		}

		/* Re-read pg_control */
#if PG_VERSION_NUM >= 100000
		ControlFile = get_controlfile(DataDir, progname, &crc_ok);
#elif PG_VERSION_NUM >= 90600
		ControlFile = get_controlfile(DataDir, progname);
#else
		ControlFile = getControlFile(DataDir);
#endif
	}

	printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);

	if (verify && badblocks > 0)
		return 1;

	return 0;
}
