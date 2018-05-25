EXTENSION  = pgtt
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
	       sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PGFILEDESC = "pgtt - Global Temporary Tables for PostgreSQL"

PG_CONFIG = pg_config

# Test that we can install a bgworker, PG >= 9.4
PG94 = $(shell $(PG_CONFIG) --version | egrep " 8\.| 9\.[0-3]" > /dev/null && echo no || echo yes)
PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LDFLAGS = -L$(libpq_builddir) -lpq

SHLIB_LINK = $(libpq)

ifeq ($(PG94),yes)
DOCS = $(wildcard doc/*.md)
MODULES = pgtt
MODULE_big = pgtt_bgw
OBJS = pgtt_bgw.o

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
TESTS      = $(wildcard test/*.sql)

else
	$(error Minimum version of PostgreSQL required is 9.4.0)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

