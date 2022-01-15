#-------------------------------------------------------------------------
#
# Makefile for pg_checksums_ext
#
# Copyright (c) 1998-2019, PostgreSQL Global Development Group
#
# pg_checksums_ext/Makefile
#
#-------------------------------------------------------------------------

PROGRAM = pg_checksums_ext
PGFILEDESC = "pg_checksums_ext - Checks, enables or disables page level checksums for a cluster"
PGAPPICON=win32

OBJS= pg_checksums_ext.o port.o $(WIN32RES)
EXTRA_CLEAN = tmp_check doc/man1

PG_CONFIG ?= pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# avoid linking against all libs that the server links against (xml, selinux, ...)
LIBS = $(libpq_pgport)

PROVE_FLAGS += -I./t/perl

all: pg_checksums_ext

man: doc/man1/pg_checksums_ext.1

doc/man1/pg_checksums_ext.1: doc/pg_checksums.sgml
	(cd doc && xsltproc stylesheet-man.xsl pg_checksums.sgml)

prove_installcheck:
	rm -rf $(CURDIR)/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)

installcheck: prove_installcheck
