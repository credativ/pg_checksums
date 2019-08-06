/*
 * pg_checksums
 *
 * Verifies/enables/disables data checksums
 *
 *	Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *
 *	port.h
 */

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
