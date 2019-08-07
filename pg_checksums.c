/*-------------------------------------------------------------------------
 *
 * pg_checksums.c
 *	  Checks, enables or disables page level checksums for a cluster
 *
 * Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  pg_checksums.c
 *
 *-------------------------------------------------------------------------
 */

#define PG_CHECKSUMS_VERSION "0.12devel"

#include "postgres_fe.h"

#include "port.h"

#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "catalog/pg_control.h"
#include "portability/instr_time.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"


static int64 files = 0;
static int64 skippedfiles = 0;
static int64 blocks = 0;
static int64 skippedblocks = 0;
static int64 badblocks = 0;
static double maxrate = 0;
static ControlFileData *ControlFile;
static XLogRecPtr checkpointLSN;

static char *only_filenode = NULL;
static bool do_sync = true;
static bool debug = false;
static bool verbose = false;
static bool showprogress = false;
static bool online = false;

char *DataDir = NULL;

typedef enum
{
	PG_MODE_CHECK,
	PG_MODE_DISABLE,
	PG_MODE_ENABLE
} PgChecksumMode;

/*
 * Filename components.
 *
 * XXX: fd.h is not declared here as frontend side code is not able to
 * interact with the backend-side definitions for the various fsync
 * wrappers.
 */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

static PgChecksumMode mode = PG_MODE_CHECK;

static const char *progname;

/*
 * Progress status information.
 */
int64		total_size = 0;
int64		current_size = 0;
instr_time	last_progress_report;
instr_time	last_throttle;
instr_time	scan_started;

static void
usage(void)
{
	printf(_("%s enables, disables, or verifies data checksums in a PostgreSQL database cluster.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR    data directory\n"));
	printf(_("  -c, --check              check data checksums (default)\n"));
	printf(_("  -d, --disable            disable data checksums\n"));
	printf(_("  -e, --enable             enable data checksums\n"));
	printf(_("  -f, --filenode=FILENODE  check only relation with specified filenode\n"));
	printf(_("  -N, --no-sync            do not wait for changes to be written safely to disk\n"));
	printf(_("  -P, --progress           show progress information\n"));
	printf(_("      --max-rate=RATE      maximum I/O rate to verify or enable checksums\n"));
	printf(_("                           (in MB/s)\n"));
	printf(_("      --debug              debug output\n"));
	printf(_("  -v, --verbose            output verbose messages\n"));
	printf(_("  -V, --version            output version information, then exit\n"));
	printf(_("  -?, --help               show this help, then exit\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("Report bugs to https://github.com/credativ/pg_checksums/issues/new.\n"));
}

/*
 * List of files excluded from checksum validation.
 */
static const char *const skip[] = {
	"pg_control",
	"pg_filenode.map",
	"pg_internal.init",
	"PG_VERSION",
#ifdef EXEC_BACKEND
	"config_exec_params",
	"config_exec_params.new",
#endif
	NULL,
};

static void
update_checkpoint_lsn(void)
{

#if PG_VERSION_NUM >= 100000
	bool	crc_ok;

#if PG_VERSION_NUM >= 120000
	ControlFile = get_controlfile(DataDir, &crc_ok);
#else
	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
#endif
	if (!crc_ok)
	{
		pg_log_error("pg_control CRC value is incorrect");
		exit(1);
	}
#elif PG_VERSION_NUM >= 90600
	ControlFile = get_controlfile(DataDir, progname);
#else
	ControlFile = getControlFile(DataDir);
#endif

	/* Update checkpointLSN with the current value */
	checkpointLSN = ControlFile->checkPoint;
}

static void
toggle_progress_report(int signum)
{

	/* we handle SIGUSR1 only, and toggle the value of showprogress */
	if (signum == SIGUSR1)
		showprogress = !showprogress;

}

/*
 * Report current progress status and/or throttle. Parts borrowed from
 * PostgreSQL's src/bin/pg_basebackup.c.
 */
static void
progress_report_or_throttle(bool force)
{
	double		elapsed;
	double		wait;
	double		percent;
	double		current_rate;
	bool		skip_progress = false;
	char		total_size_str[32];
	char		current_size_str[32];
	instr_time	now;

	Assert(showprogress);

	INSTR_TIME_SET_CURRENT(now);

	/* Make sure we throttle at most once every 50 milliseconds */
	if ((INSTR_TIME_GET_MILLISEC(now) -
		 INSTR_TIME_GET_MILLISEC(last_throttle) < 50) && !force)
		return;

	/* Make sure we report at most once every 250 milliseconds */
	if ((INSTR_TIME_GET_MILLISEC(now) -
		 INSTR_TIME_GET_MILLISEC(last_progress_report) < 250) && !force)
		skip_progress = true;

	/* Save current time */
	last_throttle = now;

	/* Elapsed time in milliseconds since start of scan */
	elapsed = (INSTR_TIME_GET_MILLISEC(now) -
			   INSTR_TIME_GET_MILLISEC(scan_started));

	/* Adjust total size if current_size is larger */
	if (current_size > total_size)
		total_size = current_size;

	/* Calculate current percentage of size done */
	percent = total_size ? 100.0 * current_size / total_size : 0.0;

#define MEGABYTES (1024 * 1024)

	/*
	 * Calculate current speed, converting current_size from bytes to megabytes
	 * and elapsed from milliseconds to seconds.
	 */
	current_rate = (current_size / MEGABYTES) / (elapsed / 1000);

	snprintf(total_size_str, sizeof(total_size_str), INT64_FORMAT,
			 total_size / MEGABYTES);
	snprintf(current_size_str, sizeof(current_size_str), INT64_FORMAT,
			 current_size / MEGABYTES);

	/* Throttle if desired */
	if (maxrate > 0 && current_rate > maxrate)
	{
		/*
		 * Calculate time to sleep in milliseconds.  Convert maxrate to MB/ms
		 * in order to get a better resolution.
		 */
		wait = (current_size / MEGABYTES / (maxrate / 1000)) - elapsed;
		if (debug)
			pg_log_debug("waiting for %f ms due to throttling", wait);
		pg_usleep(wait * 1000);
		/* Recalculate current rate */
		INSTR_TIME_SET_CURRENT(now);
		elapsed = INSTR_TIME_GET_MILLISEC(now) - INSTR_TIME_GET_MILLISEC(scan_started);
		current_rate = (int64)(current_size / MEGABYTES) / (elapsed / 1000);
	}

	/* Report progress if desired */
	if (showprogress && !skip_progress)
	{
		/*
		 * Print five blanks at the end so the end of previous lines which were
		 * longer don't remain partly visible.
		 */
		fprintf(stderr, "%s/%s MB (%d%%, %.0f MB/s)%5s",
				current_size_str, total_size_str, (int)percent, current_rate, "");

		/* Stay on the same line if reporting to a terminal */
		fprintf(stderr, isatty(fileno(stderr)) ? "\r" : "\n");
		last_progress_report = now;
	}
}

static bool
skipfile(const char *fn)
{
	const char *const *f;

	for (f = skip; *f; f++)
		if (strcmp(*f, fn) == 0)
			return true;

	return false;
}

static void
scan_file(const char *fn, BlockNumber segmentno)
{
	PGAlignedBlock buf;
	PageHeader	header = (PageHeader) buf.data;
	int			i;
	int			f;
	BlockNumber blockno;
	int			flags;
	bool		block_retry = false;
	bool		all_zeroes;
	size_t	    *pagebytes;

	Assert(mode == PG_MODE_ENABLE ||
		   mode == PG_MODE_CHECK);

	flags = (mode == PG_MODE_ENABLE) ? O_RDWR : O_RDONLY;
	f = open(fn, PG_BINARY | flags, 0);

	if (f < 0)
	{
		if (online && errno == ENOENT)
		{
			/* File was removed in the meantime */
			return;
		}

		pg_log_error("could not open file \"%s\": %m", fn);
		exit(1);
	}

	files++;

	for (blockno = 0;; blockno++)
	{
		uint16		csum;
		int			r = read(f, buf.data, BLCKSZ);

		if (debug && block_retry)
			pg_log_debug("retrying block %u in file \"%s\"", blockno, fn);

		if (r == 0)
			break;
		if (r < 0)
		{
			skippedfiles++;
			pg_log_error("could not read block %u in file \"%s\": %m", blockno, fn);
			return;
		}
		if (r != BLCKSZ)
		{
			if (online)
			{
				if (block_retry)
				{
					/* We already tried once to reread the block, skip to the next block */
					skippedblocks++;
					if (debug)
						pg_log_debug("retrying block %u in file \"%s\" failed, skipping to next block",
									 blockno, fn);

					if (lseek(f, BLCKSZ-r, SEEK_CUR) == -1)
					{
						pg_log_error("could not lseek to next block in file \"%s\": %m", fn);
						return;
					}
					continue;
				}

				/*
				 * Retry the block. It's possible that we read the block while it
				 * was extended or shrinked, so it it ends up looking torn to us.
				 */

				/*
				 * Seek back by the amount of bytes we read to the beginning of
				 * the failed block.
				 */
				if (lseek(f, -r, SEEK_CUR) == -1)
				{
					skippedfiles++;
					pg_log_error("could not lseek to in file \"%s\": %m", fn);
					return;
				}

				/* Set flag so we know a retry was attempted */
				block_retry = true;

				/* Reset loop to validate the block again */
				blockno--;

				continue;
			}
			else
			{
				/* Directly skip file if offline */
				skippedfiles++;
				pg_log_error("could not read block %u in file \"%s\": read %d of %d",
							 blockno, fn, r, BLCKSZ);
				return;
			}
		}
		blocks++;

		/* New pages have no checksum yet */
		if (PageIsNew(header))
		{
			/* Check for an all-zeroes page */
			all_zeroes = true;
			pagebytes = (size_t *) buf.data;
			for (i = 0; i < (BLCKSZ / sizeof(size_t)); i++)
			{
				if (pagebytes[i] != 0)
				{
					all_zeroes = false;
					break;
				}
			}
			if (!all_zeroes)
			{
				pg_log_error("checksum verification failed in file \"%s\", block %u: pd_upper is zero but block is not all-zero",
							 fn, blockno);
				badblocks++;
			}
			else
			{
				if (debug && block_retry)
					pg_log_debug("block %u in file \"%s\" is new, ignoring", blockno, fn);
				skippedblocks++;
			}
			continue;
		}

		csum = pg_checksum_page(buf.data, blockno + segmentno * RELSEG_SIZE);
		current_size += r;
		if (mode == PG_MODE_CHECK)
		{
			if (csum != header->pd_checksum)
			{
				if (online)
				{
					/*
					 * Retry the block on the first failure if online.  If the
					 * verification is done while the instance is online, it is
					 * possible that we read the first 4K page of the block
					 * just before postgres updated the entire block so it ends
					 * up looking torn to us.  We only need to retry once
					 * because the LSN should be updated to something we can
					 * ignore on the next pass.  If the error happens again
					 * then it is a true validation failure.
					 */
					if (!block_retry)
					{
						/* Seek to the beginning of the failed block */
						if (lseek(f, -BLCKSZ, SEEK_CUR) == -1)
						{
							skippedfiles++;
							pg_log_error("could not lseek in file \"%s\": %m", fn);
							return;
						}

						/* Set flag so we know a retry was attempted */
						block_retry = true;

						if (debug)
							pg_log_debug("checksum verification failed on first attempt in file \"%s\", block %u: calculated checksum %X but block contains %X",
										 fn, blockno, csum, header->pd_checksum);

						/* Reset loop to validate the block again */
						blockno--;
						blocks--;
						current_size -= r;

						/*
						 * Update the checkpoint LSN now. If we get a failure
						 * on re-read, we would need to do this anyway, and
						 * doing it now lowers the probability that we see the
						 * same torn page on re-read.
						 */
						update_checkpoint_lsn();

						continue;
					}

					/*
					 * The checksum verification failed on retry as well.  Check if
					 * the page has been modified since the checkpoint and skip it
					 * in this case. As a sanity check, demand that the upper
					 * 32 bits of the LSN are identical in order to skip as a
					 * guard against a corrupted LSN in the pageheader.
					 */
					if ((PageGetLSN(buf.data) > checkpointLSN) &&
						(PageGetLSN(buf.data) >> 32 == checkpointLSN >> 32))
					{
						if (debug)
							pg_log_debug("block %u in file \"%s\" with LSN %X/%X is newer than checkpoint LSN %X/%X, ignoring",
										 blockno, fn, (uint32) (PageGetLSN(buf.data) >> 32), (uint32) PageGetLSN(buf.data), (uint32) (checkpointLSN >> 32), (uint32) checkpointLSN);
						block_retry = false;
						skippedblocks++;
						continue;
					}
				}

				if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION)
					pg_log_error("checksum verification failed in file \"%s\", block %u: calculated checksum %X but block contains %X",
								 fn, blockno, csum, header->pd_checksum);
				badblocks++;
			}
			else if (block_retry && debug)
				pg_log_debug("block %u in file \"%s\" verified ok on recheck", blockno, fn);

			block_retry = false;

		}
		else if (mode == PG_MODE_ENABLE)
		{
			/* Set checksum in page header */
			header->pd_checksum = csum;

			/* Seek back to beginning of block */
			if (lseek(f, -BLCKSZ, SEEK_CUR) < 0)
			{
				pg_log_error("seek failed for block %u in file \"%s\": %m", blockno, fn);
				exit(1);
			}

			/* Write block with checksum */
			if (write(f, buf.data, BLCKSZ) != BLCKSZ)
			{
				pg_log_error("could not write block %u in file \"%s\": %m",
							 blockno, fn);
				exit(1);
			}
		}

		/* Report progress or throttle every 1024 blocks */
		if ((showprogress || maxrate > 0) && (blockno % 1024 == 0))
			progress_report_or_throttle(false);
	}

	if (verbose)
	{
		if (mode == PG_MODE_CHECK)
			pg_log_info("checksums verified in file \"%s\"", fn);
		if (mode == PG_MODE_ENABLE)
			pg_log_info("checksums enabled in file \"%s\"", fn);
	}

	/* Make sure progress is reported at least once per file */
	if (showprogress || maxrate > 0)
		progress_report_or_throttle(false);

	close(f);
}

/*
 * Scan the given directory for items which can be checksummed and
 * operate on each one of them.  If "sizeonly" is true, the size of
 * all the items which have checksums is computed and returned back
 * to the caller without operating on the files.  This is used to compile
 * the total size of the data directory for progress reports.
 */
static int64
scan_directory(const char *basedir, const char *subdir, bool sizeonly)
{
	int64		dirsize = 0;
	char		path[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(path, sizeof(path), "%s/%s", basedir, subdir);
	dir = opendir(path);
	if (!dir)
	{
		pg_log_error("could not open directory \"%s\": %m", path);
		exit(1);
	}
	while ((de = readdir(dir)) != NULL)
	{
		char		fn[MAXPGPATH];
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		/* Skip temporary files */
		if (strncmp(de->d_name,
					PG_TEMP_FILE_PREFIX,
					strlen(PG_TEMP_FILE_PREFIX)) == 0)
			continue;

		/* Skip temporary folders */
		if (strncmp(de->d_name,
					PG_TEMP_FILES_DIR,
					strlen(PG_TEMP_FILES_DIR)) == 0)
			continue;

		snprintf(fn, sizeof(fn), "%s/%s", path, de->d_name);
		if (lstat(fn, &st) < 0)
		{
			if (online && errno == ENOENT)
			{
				/* File was removed in the meantime */
				if (debug)
					pg_log_debug("ignoring deleted file \"%s\"", fn);
				continue;
			}

			pg_log_error("could not stat file \"%s\": %m", fn);
			exit(1);
		}
		if (S_ISREG(st.st_mode))
		{
			char		fnonly[MAXPGPATH];
			char	   *forkpath,
					   *segmentpath;
			BlockNumber segmentno = 0;

			if (skipfile(de->d_name))
				continue;

			/*
			 * Cut off at the segment boundary (".") to get the segment number
			 * in order to mix it into the checksum. Then also cut off at the
			 * fork boundary, to get the filenode the file belongs to for
			 * filtering.
			 */
			strlcpy(fnonly, de->d_name, sizeof(fnonly));
			segmentpath = strchr(fnonly, '.');
			if (segmentpath != NULL)
			{
				*segmentpath++ = '\0';
				segmentno = atoi(segmentpath);
				if (segmentno == 0)
					continue;
			}

			forkpath = strchr(fnonly, '_');
			if (forkpath != NULL)
				*forkpath++ = '\0';

			if (only_filenode && strcmp(only_filenode, fnonly) != 0)
				/* filenode not to be included */
				continue;

			dirsize += st.st_size;

			/*
			 * No need to work on the file when calculating only the size of
			 * the items in the data folder.
			 */
			if (!sizeonly)
				scan_file(fn, segmentno);
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

int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"check", no_argument, NULL, 'c'},
		{"pgdata", required_argument, NULL, 'D'},
		{"disable", no_argument, NULL, 'd'},
		{"enable", no_argument, NULL, 'e'},
		{"filenode", required_argument, NULL, 'f'},
		{"no-sync", no_argument, NULL, 'N'},
		{"progress", no_argument, NULL, 'P'},
		{"max-rate", required_argument, NULL, 1},
		{"verbose", no_argument, NULL, 'v'},
		{"debug", no_argument, NULL, 2},
		{NULL, 0, NULL, 0}
	};

	int			c;
	int			option_index;
#if PG_VERSION_NUM >= 100000
	bool		crc_ok;
#endif

	pg_logging_init(argv[0]);
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
			puts("pg_checksums " PG_CHECKSUMS_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "abcD:def:NPv", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'a':
				mode = PG_MODE_ENABLE; /* compat */
				break;
			case 'b':
				mode = PG_MODE_DISABLE; /* compat */
				break;
			case 'c':
				mode = PG_MODE_CHECK;
				break;
			case 'd':
				mode = PG_MODE_DISABLE;
				break;
			case 'e':
				mode = PG_MODE_ENABLE;
				break;
			case 'f':
				if (atoi(optarg) == 0)
				{
					pg_log_error("invalid filenode specification, must be numeric: %s", optarg);
					exit(1);
				}
				only_filenode = pstrdup(optarg);
				break;
			case 'N':
				do_sync = false;
				break;
			case 'v':
				verbose = true;
				break;
			case 'D':
				DataDir = optarg;
				break;
			case 'P':
				showprogress = true;
				break;
			case 1:
				if (atof(optarg) == 0)
				{
					pg_log_error("invalid max-rate specification, must be numeric: %s", optarg);
					exit(1);
				}
				maxrate = atof(optarg);
				break;
			case 2:
				debug = true;
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
			pg_log_error("no data directory specified");
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* filenode checking only works in --check mode */
	if (mode != PG_MODE_CHECK && only_filenode)
	{
		pg_log_error("option -f/--filenode can only be used with --check");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Read the control file and check compatibility */
#if PG_VERSION_NUM >= 100000
#if PG_VERSION_NUM >= 120000
	ControlFile = get_controlfile(DataDir, &crc_ok);
#else
	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
#endif
	if (!crc_ok)
	{
		pg_log_error("pg_control CRC value is incorrect");
		exit(1);
	}
#elif PG_VERSION_NUM >= 90600
	ControlFile = get_controlfile(DataDir, progname);
#else
	ControlFile = getControlFile(DataDir);
#endif

	if (ControlFile->pg_control_version != PG_CONTROL_VERSION)
	{
		pg_log_error("cluster is not compatible with this version of pg_checksums");
		exit(1);
	}

	if (ControlFile->blcksz != BLCKSZ)
	{
		pg_log_error("database cluster is not compatible");
		fprintf(stderr, _("The database cluster was initialized with block size %u, but pg_checksums was compiled with block size %u.\n"),
				ControlFile->blcksz, BLCKSZ);
		exit(1);
	}

	/*
	 * Cluster must be shut down for activation/deactivation of checksums, but
	 * online verification is supported.
	 */
	if (ControlFile->state != DB_SHUTDOWNED &&
		ControlFile->state != DB_SHUTDOWNED_IN_RECOVERY)
	{
		if (mode != PG_MODE_CHECK)
		{
			pg_log_error("cluster must be shut down");
			exit(1);
		}
		online = true;
	}

	if (debug)
	{
		if (online)
			pg_log_debug("online mode");
		else
			pg_log_debug("offline mode");
	}

	if (ControlFile->data_checksum_version == 0 &&
		mode == PG_MODE_CHECK)
	{
		pg_log_error("data checksums are not enabled in cluster");
		exit(1);
	}

	if (ControlFile->data_checksum_version == 0 &&
		mode == PG_MODE_DISABLE)
	{
		pg_log_error("data checksums are already disabled in cluster");
		exit(1);
	}

	if (ControlFile->data_checksum_version > 0 &&
		mode == PG_MODE_ENABLE)
	{
		pg_log_error("data checksums are already enabled in cluster");
		exit(1);
	}

	/* Get checkpoint LSN */
	checkpointLSN = ControlFile->checkPoint;

	/* Operate on all files if checking or enabling checksums */
	if (mode == PG_MODE_CHECK || mode == PG_MODE_ENABLE)
	{
#ifndef WIN32
		/*
		 * Assign SIGUSR1 signal handler to toggle progress status information.
		 */
		pqsignal(SIGUSR1, toggle_progress_report);
#endif

		/*
		 * As progress status information may be requested even after start of
		 * operation, we need to scan the directory tree(s) twice, once to get
		 * the idea how much data we need to scan and finally to do the real
		 * legwork.
		 */
		if (debug)
			pg_log_debug("acquiring data for progress reporting");

		total_size = scan_directory(DataDir, "global", true);
		total_size += scan_directory(DataDir, "base", true);
		total_size += scan_directory(DataDir, "pg_tblspc", true);

		/*
		 * Remember start time. Required to calculate the current rate in
		 * progress_report_or_throttle().
		 */
		if (debug)
			pg_log_debug("starting scan");
		INSTR_TIME_SET_CURRENT(scan_started);

		(void) scan_directory(DataDir, "global", false);
		(void) scan_directory(DataDir, "base", false);
		(void) scan_directory(DataDir, "pg_tblspc", false);

		/*
		 * Done. Move to next line in case progress information was shown.
		 * Otherwise we clutter the summary output.
		 */
		if (showprogress)
		{
			progress_report_or_throttle(true);
			if (isatty(fileno(stderr)))
				fprintf(stderr, "\n");
		}

		printf(_("Checksum operation completed\n"));
		printf(_("Files scanned:  %s\n"), psprintf(INT64_FORMAT, files));
		if (skippedfiles > 0)
			printf(_("Files skipped:  %s\n"), psprintf(INT64_FORMAT, skippedfiles));
		printf(_("Blocks scanned: %s\n"), psprintf(INT64_FORMAT, blocks));
		if (skippedblocks > 0)
			printf(_("Blocks skipped: %s\n"), psprintf(INT64_FORMAT, skippedblocks));;

		if (mode == PG_MODE_CHECK)
		{
			printf(_("Bad checksums:  %s\n"), psprintf(INT64_FORMAT, badblocks));
			printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);

			if (badblocks > 0)
				exit(1);

			/* skipped blocks or files are considered an error if offline */
			if (!online)
				if (skippedblocks > 0 || skippedfiles > 0)
					exit(1);
		}
	}

	/*
	 * Finally make the data durable on disk if enabling or disabling
	 * checksums.  Flush first the data directory for safety, and then update
	 * the control file to keep the switch consistent.
	 */
	if (mode == PG_MODE_ENABLE || mode == PG_MODE_DISABLE)
	{
		ControlFile->data_checksum_version =
			(mode == PG_MODE_ENABLE) ? PG_DATA_CHECKSUM_VERSION : 0;

		if (do_sync)
		{
			pg_log_info("syncing data directory");
#if PG_VERSION_NUM >= 120000
			fsync_pgdata(DataDir, PG_VERSION_NUM);
#else
			fsync_pgdata(DataDir, progname, PG_VERSION_NUM);
#endif
		}
		pg_log_info("updating control file");
		updateControlFile(DataDir, ControlFile, do_sync);

		if (verbose)
			printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);
		if (mode == PG_MODE_ENABLE)
			printf(_("Checksums enabled in cluster\n"));
		else
			printf(_("Checksums disabled in cluster\n"));
	}

	return 0;
}
