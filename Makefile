#-------------------------------------------------------------------------
#
# Makefile for pg_checksums
#
# Copyright (c) 1998-2018, PostgreSQL Global Development Group
#
# pg_checksums/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_checksums - Activate/deactivate/verify data checksums in an offline cluster"
PGAPPICON=win32

#MODULE_big = pg_checksums

OBJS= pg_checksums.o $(WIN32RES)
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: pg_checksums

pg_checksums: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
#
#install: all installdirs
#	$(INSTALL_PROGRAM) pg_checksums$(X) '$(DESTDIR)$(bindir)/pg_checksums$(X)'
#
#installdirs:
#	$(MKDIR_P) '$(DESTDIR)$(bindir)'
#
#uninstall:
#	rm -f '$(DESTDIR)$(bindir)/pg_checksums$(X)'
#
#clean distclean maintainer-clean:
#	rm -f pg_checksums$(X) $(OBJS)
#	rm -rf tmp_check
