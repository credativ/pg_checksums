/*
 * pg_checksums
 *
 * Verifies/enables/disables data checksums
 *
 *	Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *
 *	port.c
 */

#include "postgres_fe.h"

#include "port.h"

#include <dirent.h>
#include <sys/stat.h>

#if PG_VERSION_NUM < 90600

/*
 * Read in the control file.
 */
ControlFileData *
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

#endif /* PG_VERSION_NUM < 90600 */

/*
 * Update the control file.
 */
void
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

#if PG_VERSION_NUM < 100000

/*
 * fsync_fname -- Try to fsync a file or directory
 *
 * Ignores errors trying to open unreadable files, or trying to fsync
 * directories on systems where that isn't allowed/required.  Reports
 * other errors non-fatally.
 */
int
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
void
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
void
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

#endif /* PG_VERSION_NUM < 100000 */
