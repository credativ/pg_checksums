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
PG_LIBS = $(libpq_pgport)
EXTRA_CLEAN = tmp_check

PG_CONFIG ?= pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: pg_checksums

prove_installcheck: install
	rm -rf $(CURDIR)/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)

installcheck: prove_installcheck
