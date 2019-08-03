/*
 * pg_checksums
 *
 * Verifies/enables/disables data checksums
 *
 *	Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *
 *	pg_checksums.c
 */
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

#define PG_CHECKSUMS_VERSION "0.11devel"

#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

static int64 files = 0;
static int64 skippedfiles = 0;
static int64 blocks = 0;
static int64 skippedblocks = 0;
static int64 badblocks = 0;
static double maxrate = 0;
static ControlFileData *ControlFile;
static XLogRecPtr checkpointLSN;

static char *only_relfilenode = NULL;
static bool debug = false;
static bool verbose = false;
static bool show_progress = false;
static bool online = false;

char *DataDir = NULL;

typedef enum
{
        PG_MODE_CHECK,
        PG_MODE_DISABLE,
        PG_MODE_ENABLE
} PgChecksumMode;

static PgChecksumMode mode = PG_MODE_CHECK;

static const char *progname;

/*
 * Progress status information.
 */
int64		total_size = 0;
int64		current_size = 0;
instr_time	last_progress_update;
instr_time	last_throttle;
instr_time	scan_started;

static void
usage(void)
{
	printf(_("%s enables, disables or verifies data checksums in a PostgreSQL\n"), progname);
	printf(_("database cluster.\n\n"));
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -c, --check            check data checksums.  This is the default\n"));
	printf(_("                         mode if nothing is specified.\n"));
	printf(_("  -d, --disable          disable data checksums\n"));
	printf(_("  -e, --enable           enable data checksums\n"));
	printf(_("  -r RELFILENODE         check only relation with specified relfilenode\n"));
	printf(_("  -P, --progress         show progress information\n"));
	printf(_("      --max-rate=RATE    maximum I/O rate to verify or enable checksums (in MB/s)\n"));
	printf(_("      --debug            debug output\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nIf no other action is specified, checksums are verified. If no "
			 "data directory\n(DATADIR) is specified, the environment "
			 "variable PGDATA is used.\n\n"));
	printf(_("Report bugs to https://github.com/credativ/pg_checksums/issues/new.\n"));
}

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
		fprintf(stderr, _("%s: pg_control CRC value is incorrect\n"), progname);
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

/*
 * List of files excluded from checksum validation.
 *
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
toggle_progress_report(int signum)
{

	/* we handle SIGUSR1 only, and toggle the value of show_progress */
	if (signum == SIGUSR1)
		show_progress = !show_progress;

}

/*
 * Report current progress status and/or throttle. Parts borrowed from
 * PostgreSQLs' src/bin/pg_basebackup.c
 */
static void
report_progress_or_throttle(bool force)
{
	instr_time	now;
	double		elapsed;
	double		wait;
	double		total_percent;
	double		current_rate;
	bool		skip_progress = false;

	char		totalstr[32];
	char		currentstr[32];

	INSTR_TIME_SET_CURRENT(now);

	/* Make sure we throttle at most once every 50 milliseconds */
	if ((INSTR_TIME_GET_MILLISEC(now) -
		 INSTR_TIME_GET_MILLISEC(last_throttle) < 50) && !force)
		return;

	/* Make sure we report at most once every 250 milliseconds */
	if ((INSTR_TIME_GET_MILLISEC(now) -
		 INSTR_TIME_GET_MILLISEC(last_progress_update) < 250) && !force)
		skip_progress = true;

	/* Save current time */
	last_throttle = now;

	/* Elapsed time in milliseconds since start of scan */
	elapsed = (INSTR_TIME_GET_MILLISEC(now) -
			   INSTR_TIME_GET_MILLISEC(scan_started));

	/* Adjust total size if current_size is larger */
	if (current_size > total_size)
		total_size = current_size;

	/* Calculate current percent done */
	total_percent = total_size ? 100.0 * current_size / total_size : 0.0;

#define MEGABYTES (1024 * 1024)

	/*
	 * Calculate current speed, converting current_size from bytes to megabytes
	 * and elapsed from milliseconds to seconds.
	 */
	current_rate = (current_size / MEGABYTES) / (elapsed / 1000);

	snprintf(totalstr, sizeof(totalstr), INT64_FORMAT,
			 total_size / MEGABYTES);
	snprintf(currentstr, sizeof(currentstr), INT64_FORMAT,
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
			fprintf(stderr, _("%s: waiting for %f ms due to throttling\n"),
					progname, wait);
		pg_usleep(wait * 1000);
		/* Recalculate current rate */
		INSTR_TIME_SET_CURRENT(now);
		elapsed = INSTR_TIME_GET_MILLISEC(now) - INSTR_TIME_GET_MILLISEC(scan_started);
		current_rate = (int64)(current_size / MEGABYTES) / (elapsed / 1000);
	}

	/* Report progress if desired */
	if (show_progress && !skip_progress)
	{
		/*
		 * Print five blanks at the end so the end of previous lines which were
		 * longer don't remain partly visible.
		 */
		fprintf(stderr, "%s/%s MB (%d%%, %.0f MB/s)%5s",
				currentstr, totalstr, (int)total_percent, current_rate, "");

		/* Stay on the same line if reporting to a terminal */
		fprintf(stderr, isatty(fileno(stderr)) ? "\r" : "\n");
		last_progress_update = now;
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
	PGAlignedBlock	buf;
	PageHeader	header = (PageHeader) buf.data;
	int			f;
	int			flags;
	BlockNumber	blockno;
	bool		block_retry = false;
	bool		all_zeroes;
	size_t	       *pagebytes;

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

		fprintf(stderr, _("%s: could not open file \"%s\": %s\n"),
				progname, fn, strerror(errno));
		exit(1);
	}

	files++;

	for (blockno = 0;; blockno++)
	{
		uint16		csum;
		int			r = read(f, buf.data, BLCKSZ);

		if (debug && block_retry)
			fprintf(stderr, _("%s: retrying block %d in file \"%s\"\n"),
					progname, blockno, fn);

		if (r == 0)
			break;
		if (r < 0)
		{
			skippedfiles++;
			fprintf(stderr, _("%s: could not read block %u in file \"%s\": %s\n"),
					progname, blockno, fn, strerror(errno));
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
						fprintf(stderr, _("%s: retrying block %d in file \"%s\" failed, skipping to next block\n"),
								progname, blockno, fn);

					if (lseek(f, BLCKSZ-r, SEEK_CUR) == -1)
					{
						fprintf(stderr, _("%s: could not lseek to next block in file \"%s\": %m\n"),
								progname, fn);
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
					fprintf(stderr, _("%s: could not lseek in file \"%s\": %m\n"),
							progname, fn);
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
				fprintf(stderr, _("%s: could not read block %u in file \"%s\": read %d of %d\n"),
						progname, blockno, fn, r, BLCKSZ);
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
			for (int i = 0; i < (BLCKSZ / sizeof(size_t)); i++)
			{
				if (pagebytes[i] != 0)
				{
					all_zeroes = false;
					break;
				}
			}
			if (!all_zeroes)
			{
				fprintf(stderr, _("%s: checksum verification failed in file \"%s\", block %u: pd_upper is zero but block is not all-zero\n"),
						progname, fn, blockno);
				badblocks++;
			}
			else
			{
				if (debug && block_retry)
					fprintf(stderr, _("%s: block %d in file \"%s\" is new, ignoring\n"),
							progname, blockno, fn);
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
							fprintf(stderr, _("%s: could not lseek in file \"%s\": %m\n"),
									progname, fn);
							return;
						}

						/* Set flag so we know a retry was attempted */
						block_retry = true;

						if (debug)
							fprintf(stderr, _("%s: checksum verification failed on first attempt in file \"%s\", block %d: calculated checksum %X but block contains %X\n"),
									progname, fn, blockno, csum, header->pd_checksum);

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
							fprintf(stderr, _("%s: block %d in file \"%s\" with LSN %X/%X is newer than checkpoint LSN %X/%X, ignoring\n"),
									progname, blockno, fn, (uint32) (PageGetLSN(buf.data) >> 32), (uint32) PageGetLSN(buf.data), (uint32) (checkpointLSN >> 32), (uint32) checkpointLSN);
						block_retry = false;
						skippedblocks++;
						continue;
					}
				}

				if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION)
					fprintf(stderr, _("%s: checksum verification failed in file \"%s\", block %d: calculated checksum %X but block contains %X\n"),
							progname, fn, blockno, csum, header->pd_checksum);
				badblocks++;
			}
			else if (block_retry && debug)
				fprintf(stderr, _("%s: block %d in file \"%s\" verified ok on recheck\n"),
						progname, blockno, fn);

			block_retry = false;

		}
		else if (mode == PG_MODE_ENABLE)
		{
			/* Set checksum in page header */
			header->pd_checksum = csum;

			/* Seek back to beginning of block */
			if (lseek(f, -BLCKSZ, SEEK_CUR) < 0)
			{
				fprintf(stderr, _("%s: seek failed for block %d in file \"%s\": %s\n"), progname, blockno, fn, strerror(errno));
				exit(1);
			}

			/* Write block with checksum */
			if (write(f, buf.data, BLCKSZ) != BLCKSZ)
			{
				fprintf(stderr, "%s: could not update checksum of block %d in file \"%s\": %s\n",
						progname, blockno, fn, strerror(errno));
				exit(1);
			}

		}
		/* Report progress or throttle every 1024 blocks */
		if ((show_progress || maxrate > 0) && (blockno % 1024 == 0))
			report_progress_or_throttle(false);
	}

	if (verbose)
	{
		if (mode == PG_MODE_CHECK)
			fprintf(stderr, _("%s: checksums verified in file \"%s\"\n"), progname, fn);
		if (mode == PG_MODE_ENABLE)
			fprintf(stderr, _("%s: checksums enabled in file \"%s\"\n"), progname, fn);
	}

	/* Make sure progress is reported at least once per file */
	if (show_progress || maxrate > 0)
		report_progress_or_throttle(false);

	close(f);
}

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
		fprintf(stderr, _("%s: could not open directory \"%s\": %s\n"),
				progname, path, strerror(errno));
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
					fprintf(stderr, _("%s: ignoring deleted file \"%s\"\n"),
							progname, fn);
				continue;
			}

			fprintf(stderr, _("%s: could not stat file \"%s\": %s\n"),
					progname, fn, strerror(errno));
			exit(1);
		}
		if (S_ISREG(st.st_mode))
		{
			char		fnonly[MAXPGPATH];
			char	   *forkpath,
					   *segmentpath;
			BlockNumber	segmentno = 0;

			if (skipfile(de->d_name))
				continue;

			/*
			 * Cut off at the segment boundary (".") to get the segment number
			 * in order to mix it into the checksum. Then also cut off at the
			 * fork boundary, to get the relfilenode the file belongs to for
			 * filtering.
			 */
			strlcpy(fnonly, de->d_name, sizeof(fnonly));
			segmentpath = strchr(fnonly, '.');
			if (segmentpath != NULL)
			{
				*segmentpath++ = '\0';
				segmentno = atoi(segmentpath);
				if (segmentno == 0)
				{
					fprintf(stderr, _("%s: invalid segment number %d in file name \"%s\"\n"),
							progname, segmentno, fn);
					exit(1);
				}
			}

			forkpath = strchr(fnonly, '_');
			if (forkpath != NULL)
				*forkpath++ = '\0';

			if (only_relfilenode && strcmp(only_relfilenode, fnonly) != 0)
				/* Relfilenode not to be included */
				continue;

			dirsize += st.st_size;

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
		{"verbose", no_argument, NULL, 'v'},
		{"progress", no_argument, NULL, 'P'},
		{"max-rate", required_argument, NULL, 1},
		{"debug", no_argument, NULL, 2},
		{NULL, 0, NULL, 0}
	};

	int			c;
	int			option_index;
#if PG_VERSION_NUM >= 100000
	bool		crc_ok;
#endif

#if PG_VERSION_NUM >= 120000
	pg_logging_init(argv[0]);
#endif
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

	while ((c = getopt_long(argc, argv, "D:abcder:vP", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'a':
				mode = PG_MODE_ENABLE;
				break;
			case 'b':
				mode = PG_MODE_DISABLE;
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
			case 'D':
				DataDir = optarg;
				break;
			case 'r':
				if (atoi(optarg) == 0)
				{
					fprintf(stderr, _("%s: invalid relfilenode specification, must be numeric: %s\n"), progname, optarg);
					exit(1);
				}
				only_relfilenode = pstrdup(optarg);
				break;
			case 'P':
				show_progress = true;
				break;
			case 1:
				if (atof(optarg) == 0)
				{
					fprintf(stderr, _("%s: invalid max-rate specification, must be numeric: %s\n"), progname, optarg);
					exit(1);
				}
				maxrate = atof(optarg);
				break;
			case 2:
				debug = true;
				verbose = true;
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

	/* Relfilenode checking only works in check mode */
	if (mode != PG_MODE_CHECK &&
		only_relfilenode)
	{
		fprintf(stderr, _("%s: relfilenode option only possible with --check\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Check if cluster is running */
#if PG_VERSION_NUM >= 100000
#if PG_VERSION_NUM >= 120000
	ControlFile = get_controlfile(DataDir, &crc_ok);
#else
	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
#endif
	if (!crc_ok)
	{
		fprintf(stderr, _("%s: pg_control CRC value is incorrect\n"), progname);
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
		ControlFile->state != DB_SHUTDOWNED_IN_RECOVERY)
	{
		if (mode != PG_MODE_CHECK)
		{
			fprintf(stderr, _("%s: cluster must be shut down\n"), progname);
			exit(1);
		}
		online = true;
	}

	if (debug)
	{
		if (online)
			fprintf(stderr, _("%s: online mode\n"), progname);
		else
			fprintf(stderr, _("%s: offline mode\n"), progname);
	}

	if (ControlFile->data_checksum_version == 0 &&
		mode == PG_MODE_CHECK)
	{
		fprintf(stderr, _("%s: data checksums are not enabled in cluster\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == 0 &&
		mode == PG_MODE_DISABLE)
	{
		fprintf(stderr, _("%s: data checksums are already disabled in cluster\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION &&
		mode == PG_MODE_ENABLE)
	{
		fprintf(stderr, _("%s: data checksums are already enabled in cluster\n"), progname);
		exit(1);
	}

	/* Get checkpoint LSN */
	checkpointLSN = ControlFile->checkPoint;

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
			fprintf(stderr, _("%s: acquiring data for progress reporting\n"), progname);

		total_size = scan_directory(DataDir, "global", true);
		total_size += scan_directory(DataDir, "base", true);
		total_size += scan_directory(DataDir, "pg_tblspc", true);

		/*
		 * Remember start time. Required to calculate the current rate in
		 * report_progress_or_throttle().
		 */
		if (debug)
			fprintf(stderr, _("%s: starting scan\n"), progname);
		INSTR_TIME_SET_CURRENT(scan_started);

		/* Operate on all files */
		scan_directory(DataDir, "global", false);
		scan_directory(DataDir, "base", false);
		scan_directory(DataDir, "pg_tblspc", false);

		/*
		 * Done. Move to next line in case progress information was shown.
		 * Otherwise we clutter the summary output.
		 */
		if (show_progress)
		{
			report_progress_or_throttle(true);
			if (isatty(fileno(stderr)))
				fprintf(stderr, "\n");
		}

		/*
		 * Print summary and we're done.
		 */
		printf(_("Checksum operation completed\n"));
		printf(_("Files scanned:  %" INT64_MODIFIER "d\n"), files);
		if (skippedfiles > 0)
			printf(_("Files skipped: %" INT64_MODIFIER "d\n"), skippedfiles);
		printf(_("Blocks scanned: %" INT64_MODIFIER "d\n"), blocks);
		if (skippedblocks > 0)
			printf(_("Blocks skipped: %" INT64_MODIFIER "d\n"), skippedblocks);

		if (mode == PG_MODE_CHECK)
		{
			printf(_("Bad checksums:  %" INT64_MODIFIER "d\n"), badblocks);
			printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);
			if (badblocks > 0)
				return 1;

			/* skipped blocks or files are considered an error if offline */
			if (!online)
				if (skippedblocks > 0 || skippedfiles > 0)
					return 1;
		}
	}

	if (mode == PG_MODE_ENABLE || mode == PG_MODE_DISABLE)
	{
		/* Update control file */
		ControlFile->data_checksum_version =
			(mode == PG_MODE_ENABLE) ? PG_DATA_CHECKSUM_VERSION : 0;
		updateControlFile(DataDir, ControlFile);
#if PG_VERSION_NUM >= 120000
		fsync_pgdata(DataDir, PG_VERSION_NUM);
#else
		fsync_pgdata(DataDir, progname, PG_VERSION_NUM);
#endif
		if (verbose)
			printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);
		if (mode == PG_MODE_ENABLE)
			printf(_("Checksums enabled in cluster\n"));
		else
			printf(_("Checksums disabled in cluster\n"));
	}

	return 0;
}
