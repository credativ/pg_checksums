/*
 * pg_checksums
 *
 * Verifies/enables/disables data checksums
 *
 *	Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *
 *	port.h
 */

#include <unistd.h>

#include "catalog/pg_control.h"

#if PG_VERSION_NUM <  90400
#include <unistd.h>
#include <getopt.h>
extern char *optarg;
#else
#include "pg_getopt.h"
#endif

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
#if PG_VERSION_NUM >= 120000 
#include "common/logging.h" 
#endif 

#if PG_VERSION_NUM < 100000
#define PG_CONTROL_FILE_SIZE PG_CONTROL_SIZE
#endif

#if PG_VERSION_NUM <  90500
#define INIT_CRC32C INIT_CRC32
#define COMP_CRC32C COMP_CRC32
#define FIN_CRC32C FIN_CRC32
#define INT64_MODIFIER "l"

#define pg_attribute_printf(f,a) __attribute__((format(PG_PRINTF_ATTRIBUTE, f, a)))
#endif

#if PG_VERSION_NUM <  90600
typedef enum ForkNumber
{
        InvalidForkNumber = -1,
        MAIN_FORKNUM = 0,
        FSM_FORKNUM,
        VISIBILITYMAP_FORKNUM,
        INIT_FORKNUM

} ForkNumber;

#endif

#if PG_VERSION_NUM < 100000
#if __GNUC__ >= 3
#define likely(x)       __builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)       ((x) != 0)
#define unlikely(x) ((x) != 0)
#endif
#endif

#if PG_VERSION_NUM < 110000
#define pg_restrict __restrict
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

#if PG_VERSION_NUM < 90600
extern int      forkname_chars(const char *str, ForkNumber *fork);
#endif

#if PG_VERSION_NUM < 90600
ControlFileData *getControlFile(char *DataDir);
#endif

#if PG_VERSION_NUM < 100000
int fsync_fname(const char *fname, bool isdir, const char *progname);
void walkdir(const char *path,
		int (*action) (const char *fname, bool isdir, const char *progname),
		bool process_symlinks, const char *progname);
void fsync_pgdata(const char *pg_data, const char *progname, int serverVersion);
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
