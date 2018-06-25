#-------------------------------------------------------------------------
#
# Makefile for pg_checksums
#
# Copyright (c) 1998-2018, PostgreSQL Global Development Group
#
# pg_checksums/Makefile
#
#-------------------------------------------------------------------------

PROGRAM = pg_checksums
PGFILEDESC = "pg_checksums - Activate/deactivate/verify data checksums in an offline cluster"
PGAPPICON=win32


OBJS= pg_checksums.o $(WIN32RES)
EXTRA_CLEAN = tmp_check

PG_CONFIG ?= pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# avoid linking against all libs that the server links against (xml, selinux, ...)
LIBS = $(libpq_pgport)

all: pg_checksums

prove_installcheck:
	rm -rf $(CURDIR)/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)

installcheck: prove_installcheck
