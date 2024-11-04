EXTENSION  = pgtt_rsl
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
	       sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PGFILEDESC = "pgtt_rsl - Global Temporary Tables for PostgreSQL with Row Security Level"

PG_CONFIG = pg_config

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LDFLAGS = -L$(libpq_builddir) -lpq

SHLIB_LINK = $(libpq)

DOCS = $(wildcard doc/README*)
MODULES = pgtt_rsl
MODULE_big = pgtt_bgw
OBJS = pgtt_bgw.o

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
TESTS      = $(wildcard test/*.sql)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

