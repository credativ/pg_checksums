/*
 * pg_checksums_ext
 *
 * Verifies/enables/disables data checksums
 *
 *	Copyright (c) 2010-2024, PostgreSQL Global Development Group
 *
 *	port.h
 */

#include <unistd.h>

#include "catalog/pg_control.h"
#include "common/file_perm.h"

#if PG_VERSION_NUM >= 120000 
#include "common/logging.h" 
#endif 
#if PG_VERSION_NUM >= 140000
#include "fe_utils/option_utils.h"
#endif

#if PG_VERSION_NUM < 170000
/*
 * Filename components.
 *
 * XXX: fd.h is not declared here as frontend side code is not able to
 * interact with the backend-side definitions for the various fsync
 * wrappers.
 */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"
#endif

/*
 * pg_xlog has been renamed to pg_wal in version 10.
 */
#define MINIMUM_VERSION_FOR_PG_WAL      100000

/*
 * The control file (relative to $PGDATA)
 */
#define XLOG_CONTROL_FILE	"global/pg_control"

extern char *DataDir;

static const char *progname;

void updateControlFile(char *DataDir, ControlFileData *ControlFile, bool do_sync);

#if PG_VERSION_NUM >=  170000
extern bool parse_sync_method(const char *optarg,
			      DataDirSyncMethod *sync_method);
#endif

#if PG_VERSION_NUM < 120000
enum pg_log_level
{
        PG_LOG_NOTSET = 0,
        PG_LOG_DEBUG,
        PG_LOG_INFO,
        PG_LOG_WARNING,
        PG_LOG_ERROR,
        PG_LOG_FATAL,
        PG_LOG_OFF,
};

extern enum pg_log_level __pg_log_level;

#define PG_LOG_FLAG_TERSE	1

void		pg_logging_init(const char *argv0);
void		pg_log_generic(enum pg_log_level level, const char *pg_restrict fmt,...) pg_attribute_printf(2, 3);
void		pg_log_generic_v(enum pg_log_level level, const char *pg_restrict fmt, va_list ap) pg_attribute_printf(2, 0);

#define pg_log_fatal(...) do { \
		if (likely(__pg_log_level <= PG_LOG_FATAL)) pg_log_generic(PG_LOG_FATAL, __VA_ARGS__); \
	} while(0)

#define pg_log_error(...) do { \
		if (likely(__pg_log_level <= PG_LOG_ERROR)) pg_log_generic(PG_LOG_ERROR, __VA_ARGS__); \
	} while(0)

#define pg_log_warning(...) do { \
		if (likely(__pg_log_level <= PG_LOG_WARNING)) pg_log_generic(PG_LOG_WARNING, __VA_ARGS__); \
	} while(0)

#define pg_log_info(...) do { \
		if (likely(__pg_log_level <= PG_LOG_INFO)) pg_log_generic(PG_LOG_INFO, __VA_ARGS__); \
	} while(0)

#define pg_log_debug(...) do { \
		if (unlikely(__pg_log_level <= PG_LOG_DEBUG)) pg_log_generic(PG_LOG_DEBUG, __VA_ARGS__); \
	} while(0)
#endif

void CheckDataVersion(char *DataDir);
