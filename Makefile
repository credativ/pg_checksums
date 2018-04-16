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

PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: pg_checksums
