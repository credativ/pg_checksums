/*
 * pg_checksums
 *
 * Verifies/enables/disables data checksums
 *
 *	Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 *	pg_checksums.c
 */
#include "postgres_fe.h"

#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#if PG_VERSION_NUM <  90400
#include <unistd.h>
#include <getopt.h>
extern char *optarg;
#else
#include "pg_getopt.h"
#endif

#include "postgres.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#if PG_VERSION_NUM >=  90600
#include "common/controldata_utils.h"
#include "common/relpath.h"
#endif
#if PG_VERSION_NUM >= 110000
#include "common/file_perm.h"
#else
#define pg_file_create_mode 0600
#endif
#if PG_VERSION_NUM >= 100000
#include "common/file_utils.h"
#endif
#include "portability/instr_time.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"

#define PG_CHECKSUMS_VERSION "0.8devel"

#if PG_VERSION_NUM < 100000
#define PG_CONTROL_FILE_SIZE PG_CONTROL_SIZE
#endif

#if PG_VERSION_NUM <  90500
#define INIT_CRC32C INIT_CRC32
#define COMP_CRC32C COMP_CRC32
#define FIN_CRC32C FIN_CRC32
#define INT64_MODIFIER "l"
#endif

#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

/*
 * pg_xlog has been renamed to pg_wal in version 10.
 */
#define MINIMUM_VERSION_FOR_PG_WAL      100000

static int64 files = 0;
static int64 skippedfiles = 0;
static int64 blocks = 0;
static int64 skippedblocks = 0;
static int64 badblocks = 0;
static int64 maxrate = 0;               /* no limit by default */
static ControlFileData *ControlFile;
static XLogRecPtr checkpointLSN;

static char *only_relfilenode = NULL;
static bool debug = false;
static bool verbose = false;
static bool show_progress = false;
static bool online = false;

typedef enum
{
        PG_ACTION_CHECK,
        PG_ACTION_DISABLE,
        PG_ACTION_ENABLE
} ChecksumAction;

static ChecksumAction action = PG_ACTION_CHECK;

static const char *progname;

/*
 * Progress status information.
 */
int64		total_size = 0;
int64		current_size = 0;
instr_time	last_progress_update;
instr_time	scan_started;

static void updateControlFile(char *DataDir, ControlFileData *ControlFile);

#if PG_VERSION_NUM < 90600
static ControlFileData *getControlFile(char *DataDir);
#endif

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
	printf(_("      --max-rate=RATE    maximum I/O rate to verify or enable checksums (in kB/s)\n"));
	printf(_("      --debug            debug output\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nIf no other action is specified, checksums are verified. If no "
			 "data directory\n(DATADIR) is specified, the environment "
			 "variable PGDATA is used.\n\n"));
	printf(_("Report bugs to https://github.com/credativ/pg_checksums/issues/new.\n"));
}

/*
 * isRelFileName
 *
 * Check if the given file name is authorized for checksum verification.
 */
static bool
isRelFileName(const char *fn)
{
	int			pos;

	/*----------
	 * Only files including data checksums are authorized for verification.
	 * This is guessed based on the file name by reverse-engineering
	 * GetRelationPath() so make sure to update both code paths if any
	 * updates are done.  The following file name formats are allowed:
	 * <digits>
	 * <digits>.<segment>
	 * <digits>_<forkname>
	 * <digits>_<forkname>.<segment>
	 *
	 * Note that temporary files, beginning with 't', are also skipped.
	 *
	 *----------
	 */

	/* A non-empty string of digits should follow */
	for (pos = 0; isdigit((unsigned char) fn[pos]); ++pos)
		;
	/* leave if no digits */
	if (pos == 0)
		return false;
	/* good to go if only digits */
	if (fn[pos] == '\0')
		return true;

	/* Authorized fork files can be scanned */
	if (fn[pos] == '_')
	{
		int			forkchar = forkname_chars(&fn[pos + 1], NULL);

		if (forkchar <= 0)
			return false;

		pos += forkchar + 1;
	}

	/* Check for an optional segment number */
	if (fn[pos] == '.')
	{
		int			segchar;

		for (segchar = 1; isdigit((unsigned char) fn[pos + segchar]); ++segchar)
			;

		if (segchar <= 1)
			return false;
		pos += segchar;
	}

	/* Now this should be the end */
	if (fn[pos] != '\0')
		return false;
	return true;
}

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
	int			wait;
	int			total_percent = 0;
	int64		current_rate = 0;

	char		totalstr[32];
	char		currentstr[32];
	char		currratestr[32];

	INSTR_TIME_SET_CURRENT(now);

	/* Make sure we report at most once every tenth of a second */
	if ((INSTR_TIME_GET_MILLISEC(now) - INSTR_TIME_GET_MILLISEC(last_progress_update) < 100) && !force)
		return;

	/* Save current time */
	last_progress_update = now;

	/* Elapsed time in milliseconds since start of scan */
	elapsed = INSTR_TIME_GET_MILLISEC(now) - INSTR_TIME_GET_MILLISEC(scan_started);

	/* Calculate current percent done, based on KiB... */
	total_percent = total_size ? (int64) ((current_size / 1024) * 100 / (total_size / 1024)) : 0;

	/* Don't display larger than 100% */
	if (total_percent > 100)
		total_percent = 100;

	/* The same for total size */
	if (current_size > total_size)
		total_size = current_size / 1024;

	/* Current rate in kB/s */
	current_rate = (current_size / 1024) / ((elapsed / 1000) == 0 ? 1 : (elapsed / 1000));

	snprintf(totalstr, sizeof(totalstr), INT64_FORMAT,
			 total_size / 1024);
	snprintf(currentstr, sizeof(currentstr), INT64_FORMAT,
			 current_size / 1024);
	snprintf(currratestr, sizeof(currratestr), INT64_FORMAT,
			 current_rate);

	/* Throttle if desired */
	if (maxrate > 0 && current_rate > maxrate)
	{
		/* Calculate time to sleep */
		wait = (current_size / 1024 / (maxrate / 1000)) - elapsed;
		if (debug)
			fprintf(stderr, _("%s: waiting for %ld ms due to throttling\n"),
					progname, wait);
		pg_usleep(wait * 1000);
		/* Recalculate current rate */
		INSTR_TIME_SET_CURRENT(now);
		elapsed = INSTR_TIME_GET_MILLISEC(now) - INSTR_TIME_GET_MILLISEC(scan_started);
		current_rate = (current_size / 1024) / (elapsed / 1000);
		snprintf(currratestr, sizeof(currratestr), INT64_FORMAT,
			 current_rate);
	}

	/* Report progress if desired */
	if (show_progress)
	{
		fprintf(stderr, "%s/%s kB (%d%%, %s kB/s)",
				currentstr, totalstr, total_percent, currratestr);

		/*
		 * If we are reporting to a terminal, send a carriage return so that we
		 * stay on the same line.  If not, send a newline.
		 */
		if (isatty(fileno(stderr)))
			fprintf(stderr, "\r");
		else
			fprintf(stderr, "\n");
	}
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

	Assert(action == PG_ACTION_ENABLE ||
		   action == PG_ACTION_CHECK);

	flags = (action == PG_ACTION_ENABLE) ? O_RDWR : O_RDONLY;
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

		/* New pages have no checksum yet */
		if (PageIsNew(header))
		{
			if (debug && block_retry)
				fprintf(stderr, _("%s: block %d in file \"%s\" is new, ignoring\n"),
						progname, blockno, fn);
			skippedblocks++;
			continue;
		}

		blocks++;

		csum = pg_checksum_page(buf.data, blockno + segmentno * RELSEG_SIZE);
		current_size += r;

		if (action == PG_ACTION_CHECK)
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
						current_size -= r;

						continue;
					}

					/*
					 * The checksum verification failed on retry as well.  Check if
					 * the page has been modified since the checkpoint and skip it
					 * in this case.
					 */
					if (PageGetLSN(buf.data) > checkpointLSN)
					{
						if (debug)
							fprintf(stderr, _("%s: block %d in file \"%s\" with LSN %X/%X is newer than checkpoint LSN %X/%X, ignoring\n"),
									progname, blockno, fn, (uint32) (PageGetLSN(buf.data) >> 32), (uint32) PageGetLSN(buf.data), (uint32) (checkpointLSN >> 32), (uint32) checkpointLSN);
						block_retry = false;
						blocks--;
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

			if (show_progress || maxrate > 0)
				report_progress_or_throttle(false);
		}
		else if (action == PG_ACTION_ENABLE)
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

			if (show_progress || maxrate > 0)
				report_progress_or_throttle(false);
		}
	}

	if (verbose)
	{
		if (action == PG_ACTION_CHECK)
			fprintf(stderr, _("%s: checksums verified in file \"%s\"\n"), progname, fn);
		if (action == PG_ACTION_ENABLE)
			fprintf(stderr, _("%s: checksums enabled in file \"%s\"\n"), progname, fn);
	}

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

			/*
			 * Only normal relation files can be analyzed.  Note that this
			 * skips temporary relations.
			 */
			if (!isRelFileName(de->d_name))
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

	Assert(action == PG_ACTION_ENABLE ||
	       action == PG_ACTION_DISABLE);

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

	fd = open(ControlFilePath, O_WRONLY | PG_BINARY,
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

/* syncDataDir() and findInitDB() are used on PostgreSQL releases < 10, only */
#if PG_VERSION_NUM < 100000

/*
 * fsync_fname -- Try to fsync a file or directory
 *
 * Ignores errors trying to open unreadable files, or trying to fsync
 * directories on systems where that isn't allowed/required.  Reports
 * other errors non-fatally.
 */
static int
fsync_fname(const char *fname, bool isdir, const char *progname)
{
	int			fd;
	int			flags;
	int			returncode;

	/*
	 * Some OSs require directories to be opened read-only whereas other
	 * systems don't allow us to fsync files opened read-only; so we need both
	 * cases here.  Using O_RDWR will cause us to fail to fsync files that are
	 * not writable by our userid, but we assume that's OK.
	 */
	flags = PG_BINARY;
	if (!isdir)
		flags |= O_RDWR;
	else
		flags |= O_RDONLY;

	/*
	 * Open the file, silently ignoring errors about unreadable files (or
	 * unsupported operations, e.g. opening a directory under Windows), and
	 * logging others.
	 */
	fd = open(fname, flags, 0);
	if (fd < 0)
	{
		if (errno == EACCES || (isdir && errno == EISDIR))
			return 0;
		fprintf(stderr, _("%s: could not open file \"%s\": %s\n"),
				progname, fname, strerror(errno));
		return -1;
	}

	returncode = fsync(fd);

	/*
	 * Some OSes don't allow us to fsync directories at all, so we can ignore
	 * those errors. Anything else needs to be reported.
	 */
	if (returncode != 0 && !(isdir && (errno == EBADF || errno == EINVAL)))
	{
		fprintf(stderr, _("%s: could not fsync file \"%s\": %s\n"),
				progname, fname, strerror(errno));
		(void) close(fd);
		return -1;
	}

	(void) close(fd);
	return 0;
}
/*
 * walkdir: recursively walk a directory, applying the action to each
 * regular file and directory (including the named directory itself).
 *
 * If process_symlinks is true, the action and recursion are also applied
 * to regular files and directories that are pointed to by symlinks in the
 * given directory; otherwise symlinks are ignored.  Symlinks are always
 * ignored in subdirectories, ie we intentionally don't pass down the
 * process_symlinks flag to recursive calls.
 *
 * Errors are reported but not considered fatal.
 *
 * See also walkdir in fd.c, which is a backend version of this logic.
 */
static void
walkdir(const char *path,
		int (*action) (const char *fname, bool isdir, const char *progname),
		bool process_symlinks, const char *progname)
{
	DIR		   *dir;
	struct dirent *de;

	dir = opendir(path);
	if (dir == NULL)
	{
		fprintf(stderr, _("%s: could not open directory \"%s\": %s\n"),
				progname, path, strerror(errno));
		return;
	}

	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		char		subpath[MAXPGPATH * 2];
		struct stat fst;
		int			sret;

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(subpath, sizeof(subpath), "%s/%s", path, de->d_name);

		if (process_symlinks)
			sret = stat(subpath, &fst);
		else
			sret = lstat(subpath, &fst);

		if (sret < 0)
		{
			fprintf(stderr, _("%s: could not stat file \"%s\": %s\n"),
					progname, subpath, strerror(errno));
			continue;
		}

		if (S_ISREG(fst.st_mode))
			(*action) (subpath, false, progname);
		else if (S_ISDIR(fst.st_mode))
			walkdir(subpath, action, false, progname);
	}

	if (errno)
		fprintf(stderr, _("%s: could not read directory \"%s\": %s\n"),
				progname, path, strerror(errno));

	(void) closedir(dir);

	/*
	 * It's important to fsync the destination directory itself as individual
	 * file fsyncs don't guarantee that the directory entry for the file is
	 * synced.  Recent versions of ext4 have made the window much wider but
	 * it's been an issue for ext3 and other filesystems in the past.
	 */
	(*action) (path, true, progname);
}


/*
 * Issue fsync recursively on PGDATA and all its contents.
 *
 * We fsync regular files and directories wherever they are, but we follow
 * symlinks only for pg_wal (or pg_xlog) and immediately under pg_tblspc.
 * Other symlinks are presumed to point at files we're not responsible for
 * fsyncing, and might not have privileges to write at all.
 *
 * serverVersion indicates the version of the server to be fsync'd.
 *
 * Errors are reported but not considered fatal.
 */
static void
fsync_pgdata(const char *pg_data,
			 const char *progname,
			 int serverVersion)
{
	bool		xlog_is_symlink;
	char		pg_wal[MAXPGPATH];
	char		pg_tblspc[MAXPGPATH];

	/* handle renaming of pg_xlog to pg_wal in post-10 clusters */
	snprintf(pg_wal, MAXPGPATH, "%s/%s", pg_data,
			 serverVersion < MINIMUM_VERSION_FOR_PG_WAL ? "pg_xlog" : "pg_wal");
	snprintf(pg_tblspc, MAXPGPATH, "%s/pg_tblspc", pg_data);

	/*
	 * If pg_wal is a symlink, we'll need to recurse into it separately,
	 * because the first walkdir below will ignore it.
	 */
	xlog_is_symlink = false;

#ifndef WIN32
	{
		struct stat st;

		if (lstat(pg_wal, &st) < 0)
			fprintf(stderr, _("%s: could not stat file \"%s\": %s\n"),
					progname, pg_wal, strerror(errno));
		else if (S_ISLNK(st.st_mode))
			xlog_is_symlink = true;
	}
#else
	if (pgwin32_is_junction(pg_wal))
		xlog_is_symlink = true;
#endif

	/*
	 * If possible, hint to the kernel that we're soon going to fsync the data
	 * directory and its contents.
	 */
#ifdef PG_FLUSH_DATA_WORKS
	walkdir(pg_data, pre_sync_fname, false, progname);
	if (xlog_is_symlink)
		walkdir(pg_wal, pre_sync_fname, false, progname);
	walkdir(pg_tblspc, pre_sync_fname, true, progname);
#endif

	/*
	 * Now we do the fsync()s in the same order.
	 *
	 * The main call ignores symlinks, so in addition to specially processing
	 * pg_wal if it's a symlink, pg_tblspc has to be visited separately with
	 * process_symlinks = true.  Note that if there are any plain directories
	 * in pg_tblspc, they'll get fsync'd twice.  That's not an expected case
	 * so we don't worry about optimizing it.
	 */
	walkdir(pg_data, fsync_fname, false, progname);
	if (xlog_is_symlink)
		walkdir(pg_wal, fsync_fname, false, progname);
	walkdir(pg_tblspc, fsync_fname, true, progname);
}


#endif

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

	char	   *DataDir = NULL;
	int			c;
	int			option_index;
#if PG_VERSION_NUM >= 100000
	bool		crc_ok;
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
				action = PG_ACTION_ENABLE;
				break;
			case 'b':
				action = PG_ACTION_DISABLE;
				break;
			case 'c':
				action = PG_ACTION_CHECK;
				break;
			case 'd':
				action = PG_ACTION_DISABLE;
				break;
			case 'e':
				action = PG_ACTION_ENABLE;
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
				if (atoi(optarg) == 0)
				{
					fprintf(stderr, _("%s: invalid max-rate specification, must be numeric: %s\n"), progname, optarg);
					exit(1);
				}
				maxrate = atoi(optarg);
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
	if (action != PG_ACTION_CHECK &&
		only_relfilenode)
	{
		fprintf(stderr, _("%s: relfilenode option only possible with --check\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Check if cluster is running */
#if PG_VERSION_NUM >= 100000
	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
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
		if (action != PG_ACTION_CHECK)
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
		action == PG_ACTION_CHECK)
	{
		fprintf(stderr, _("%s: data checksums are not enabled in cluster\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == 0 &&
		action == PG_ACTION_DISABLE)
	{
		fprintf(stderr, _("%s: data checksums are already disabled in cluster\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION &&
		action == PG_ACTION_ENABLE)
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

	if (action == PG_ACTION_CHECK || action == PG_ACTION_ENABLE)
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

		if (action == PG_ACTION_CHECK)
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

	if (action == PG_ACTION_ENABLE || action == PG_ACTION_DISABLE)
	{
		/* Update control file */
		ControlFile->data_checksum_version =
			(action == PG_ACTION_ENABLE) ? PG_DATA_CHECKSUM_VERSION : 0;
		updateControlFile(DataDir, ControlFile);
		fsync_pgdata(DataDir, progname, PG_VERSION_NUM);
		if (verbose)
			printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);
		if (action == PG_ACTION_ENABLE)
			printf(_("Checksums enabled in cluster\n"));
		else
			printf(_("Checksums disabled in cluster\n"));
	}

	return 0;
}
