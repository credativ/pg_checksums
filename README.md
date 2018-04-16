pg_checksums - verify and (de)activate checksums in offline clusters
====================================================================

`pg_checksums` is a small tool based on the `pg_verify_checksums` program
available in PostgreSQL from version 11 on. In addition to verifying checksums,
it can activate and deactivate them.  The former requires all database blocks
to be read and all page headers to be updated, so can take a long time on a
large database.

The database cluster needs to be shutdown cleanly in order for the program to
operate.
