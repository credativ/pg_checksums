/*
 * pg_checksums_ext
 *
 * Verifies/enables/disables data checksums
 *
 *	Copyright (c) 2010-2024, PostgreSQL Global Development Group
 *
 *	port.c
 */

#include "postgres_fe.h"

#include "port.h"

#include <dirent.h>
#include <sys/stat.h>

/*
 * Update the control file.
 */
void
updateControlFile(char *DataDir, ControlFileData *ControlFile, bool do_sync)
{
	int			fd;
	char		buffer[PG_CONTROL_FILE_SIZE];
	char		ControlFilePath[MAXPGPATH];

	/*
	 * For good luck, apply the same static assertions as in backend's
	 * WriteControlFile().
	 */
	StaticAssertStmt(sizeof(ControlFileData) <= PG_CONTROL_MAX_SAFE_SIZE,
					 "pg_control is too large for atomic disk writes");
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

	if (do_sync)
	{
		if (fsync(fd) != 0)
		{
			fprintf(stderr, _("%s: fsync error: %s\n"), progname, strerror(errno));
			exit(1);
		}
	}

	if (close(fd) < 0)
	{
		fprintf(stderr, _("%s: could not close control file: %s\n"), progname, strerror(errno));
		exit(1);
	}
}

#if PG_VERSION_NUM < 120000

enum pg_log_level __pg_log_level;

static int	log_flags;

static void (*log_pre_callback) (void);
static void (*log_locus_callback) (const char **, uint64 *);

static const char *sgr_error = NULL;
static const char *sgr_warning = NULL;
static const char *sgr_locus = NULL;

#define SGR_ERROR_DEFAULT "01;31"
#define SGR_WARNING_DEFAULT "01;35"
#define SGR_LOCUS_DEFAULT "01"

#define ANSI_ESCAPE_FMT "\x1b[%sm"
#define ANSI_ESCAPE_RESET "\x1b[0m"

/*
 * This should be called before any output happens.
 */
void
pg_logging_init(const char *argv0)
{
	const char *pg_color_env = getenv("PG_COLOR");
	char	   *token;
	bool		log_color = false;

	/* usually the default, but not on Windows */
	setvbuf(stderr, NULL, _IONBF, 0);

	progname = get_progname(argv0);
	__pg_log_level = PG_LOG_INFO;

	if (pg_color_env)
	{
		if (strcmp(pg_color_env, "always") == 0 ||
			(strcmp(pg_color_env, "auto") == 0 && isatty(fileno(stderr))))
			log_color = true;
	}

	if (log_color)
	{
		const char *pg_colors_env = getenv("PG_COLORS");

		if (pg_colors_env)
		{
			char	   *colors = strdup(pg_colors_env);

			if (colors)
			{
				for (token = strtok(colors, ":"); token; token = strtok(NULL, ":"))
				{
					char	   *e = strchr(token, '=');

					if (e)
					{
						char	   *name;
						char	   *value;

						*e = '\0';
						name = token;
						value = e + 1;

						if (strcmp(name, "error") == 0)
							sgr_error = strdup(value);
						if (strcmp(name, "warning") == 0)
							sgr_warning = strdup(value);
						if (strcmp(name, "locus") == 0)
							sgr_locus = strdup(value);
					}
				}

				free(colors);
			}
		}
		else
		{
			sgr_error = SGR_ERROR_DEFAULT;
			sgr_warning = SGR_WARNING_DEFAULT;
			sgr_locus = SGR_LOCUS_DEFAULT;
		}
	}
}

void
pg_log_generic(enum pg_log_level level, const char *pg_restrict fmt,...)
{
        va_list         ap;

        va_start(ap, fmt);
        pg_log_generic_v(level, fmt, ap);
        va_end(ap);
}

void
pg_log_generic_v(enum pg_log_level level, const char *pg_restrict fmt, va_list ap)
{
        int                     save_errno = errno;
        const char *filename = NULL;
        uint64          lineno = 0;
        va_list         ap2;
        size_t          required_len;
        char       *buf;

        Assert(progname);
        Assert(level);
        Assert(fmt);
        Assert(fmt[strlen(fmt) - 1] != '\n');

        /*
         * Flush stdout before output to stderr, to ensure sync even when stdout
         * is buffered.
         */
        fflush(stdout);

        if (log_pre_callback)
                log_pre_callback();

        if (log_locus_callback)
                log_locus_callback(&filename, &lineno);

        fmt = _(fmt);

        if (!(log_flags & PG_LOG_FLAG_TERSE) || filename)
        {
                if (sgr_locus)
                        fprintf(stderr, ANSI_ESCAPE_FMT, sgr_locus);
                if (!(log_flags & PG_LOG_FLAG_TERSE))
                        fprintf(stderr, "%s:", progname);
                if (filename)
                {
                        fprintf(stderr, "%s:", filename);
                        if (lineno > 0)
                                fprintf(stderr, UINT64_FORMAT ":", lineno);
                }
                fprintf(stderr, " ");
                if (sgr_locus)
                        fprintf(stderr, ANSI_ESCAPE_RESET);
        }

        if (!(log_flags & PG_LOG_FLAG_TERSE))
        {
                switch (level)
                {
                        case PG_LOG_FATAL:
                                if (sgr_error)
                                        fprintf(stderr, ANSI_ESCAPE_FMT, sgr_error);
                                fprintf(stderr, _("fatal: "));
                                if (sgr_error)
                                        fprintf(stderr, ANSI_ESCAPE_RESET);
                                break;
                        case PG_LOG_ERROR:
                                if (sgr_error)
                                        fprintf(stderr, ANSI_ESCAPE_FMT, sgr_error);
                                fprintf(stderr, _("error: "));
                                if (sgr_error)
                                        fprintf(stderr, ANSI_ESCAPE_RESET);
                                break;
                        case PG_LOG_WARNING:
                                if (sgr_warning)
                                        fprintf(stderr, ANSI_ESCAPE_FMT, sgr_warning);
                                fprintf(stderr, _("warning: "));
                                if (sgr_warning)
                                        fprintf(stderr, ANSI_ESCAPE_RESET);
                                break;
                        default:
                                break;
                }
        }

        errno = save_errno;

        va_copy(ap2, ap);
        required_len = vsnprintf(NULL, 0, fmt, ap2) + 1;
        va_end(ap2);

        buf = pg_malloc(required_len);

        errno = save_errno;                     /* malloc might change errno */

        if (!buf)
        {
                /* memory trouble, just print what we can and get out of here */
                vfprintf(stderr, fmt, ap);
                return;
        }

        vsnprintf(buf, required_len, fmt, ap);

        /* strip one newline, for PQerrorMessage() */
        if (required_len >= 2 && buf[required_len - 2] == '\n')
                buf[required_len - 2] = '\0';

        fprintf(stderr, "%s\n", buf);

        free(buf);
}

#endif /* PG_VERSION_NUM < 120000 */

/*
 * Look at the version string stored in PG_VERSION and decide if this utility
 * can be run safely or not (adpated from pg_resetwal).
 */
void
CheckDataVersion(char *DataDir)
{
	const char *ver_file = "PG_VERSION";
	char		ver_filepath[MAXPGPATH];
	FILE	   *ver_fd;
	char		rawline[64];
	int			len;

	strcpy(ver_filepath, DataDir);
	strcat(ver_filepath, "/");
	strcat(ver_filepath, ver_file);

	if ((ver_fd = fopen(ver_filepath, "r")) == NULL)
	{
		pg_log_error("could not open file \"%s\" for reading: %m",
					 ver_filepath);
		exit(1);
	}

	/* version number has to be the first line read */
	if (!fgets(rawline, sizeof(rawline), ver_fd))
	{
		if (!ferror(ver_fd))
			pg_log_error("unexpected empty file \"%s\"", ver_filepath);
		else
			pg_log_error("could not read file \"%s\": %m", ver_filepath);
		exit(1);
	}

	/* strip trailing newline and carriage return */
	len = strlen(rawline);
	if (len > 0 && rawline[len - 1] == '\n')
	{
		rawline[--len] = '\0';
		if (len > 0 && rawline[len - 1] == '\r')
			rawline[--len] = '\0';
	}

	if (strcmp(rawline, PG_MAJORVERSION) != 0)
	{
		pg_log_error("data directory is of wrong version");
		pg_log_info("File \"%s\" contains \"%s\", which is not compatible with this program's version \"%s\".",
					ver_file, rawline, PG_MAJORVERSION);
		exit(1);
	}

	fclose(ver_fd);
}

#if PG_VERSION_NUM >= 170000
/*
 * Provide strictly harmonized handling of the --sync-method option.
 */
bool
parse_sync_method(const char *optarg, DataDirSyncMethod *sync_method)
{
        if (strcmp(optarg, "fsync") == 0)
                *sync_method = DATA_DIR_SYNC_METHOD_FSYNC;
        else if (strcmp(optarg, "syncfs") == 0)
        {
#ifdef HAVE_SYNCFS
                *sync_method = DATA_DIR_SYNC_METHOD_SYNCFS;
#else
                pg_log_error("this build does not support sync method \"%s\"",
                                         "syncfs");
                return false;
#endif
        }
        else
        {
                pg_log_error("unrecognized sync method: %s", optarg);
                return false;
        }

        return true;
}
#endif /* PG_VERSION_NUM >= 170000 */
