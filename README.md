* [Description](#description)
* [Installation](#installation)
* [Configuration](#configuration)
* [Use of the extension](#use-of-the-extension)
* [How the extension really works](#how-the-extension-really-works)
* [Authors](#authors)

## PostgreSQL Global Temporary Tables - RSL

### [Description](#description)

pgtt_rsl is a PostgreSQL extension to create and manage Oracle or DB2 style
Global Temporary Tables. It is based on unlogged tables, Row Security
Level and views. A background worker is responsible to periodically
remove obsolete rows and tables. This implementation is designed to avoid
catalog bloating when creating a lot of temporary tables. If you are looking
for Oracle style Global Temporary Tables but with high performances
you should look at [pgtt](https://github.com/darold/pgtt) which is
based on temporary tables but will not address the bloat problem.

PostgreSQL native temporary tables are automatically dropped at the
end of a session, or optionally at the end of the current transaction.
Oracle Global Temporary Tables (GTT) are permanent, they are created
as regular tables visible to all users but their content is relative
to the current session or transaction. Even if the table is persistent
a session or transaction can not see rows written by an other session.

An other difference is that Oracle or DB2 Global Temporary Table can be
created in any schema while it is not possible with PostgreSQL where
temporary table are stored in the pg_temp namespace. This version of the
extension aallow to create the global temporary table in any schema.

Usually this is not a problem, you have learn to deal with the
temporary table behavior of PostgreSQL but the problem comes when
you are migrating an Oracle or DB2 database to PostgreSQL. You have to
rewrite the SQL and plpgsql code to follow the application logic and
use PostgreSQL temporary table, that mean recreating the temporary
table everywhere it is used.

The other advantage of this kind of object when your application
create and delete a lot of temporary tables this will add bloat in
the PostgreSQL catalog and the performances will start to fall.
Using Global Temporary Table will save this catalog bloating. They
are permanent tables and so on permanent indexes, they will be found
by the autovacuum process in contrary of local temporary tables.
The very intensive creation of temporary table can also generate lot 
of replication lag.

Don't expect high performances if you insert huge amount of tuples in
these tables. The use of Row Security Level and cleanup by a background
worker is slower than the use of regular temporary tables. Use it only
if you have the pg_catalog bloating issue.

### [Installation](#installation)

To install the pgtt_rsl extension you need a PostgreSQL version upper than
9.5. Untar the pgtt_rsl tarball anywhere you want then you'll need to
compile it with pgxs. So the pg_config tool must be in your path.

Depending on your installation, you may need to install some devel
package and especially the libpq devel package. Once pg_config is in
your path, do "make", and then "make install".

Once the extension is installed, that shared_preload_library is set and
PostgreSQL restarted you can verify that the extension is working as
expected using:

- `LANG=C psql -f test/test_gtt.sql > result.log 2>&1 && diff result.log test/expected/test_gtt.txt`
- `LANG=C psql -f test/test_gtt2.sql > result.log 2>&1 && diff result.log test/expected/test_gtt2.txt`

### [Configuration](#configuration)

The background worker used to remove obsolete rows from global
temporary tables must be loaded on database start by adding the
library to shared_preload_libraries in postgresql.conf. You also
need to load the pgtt_rsl library defining C function used by the
extension.

* shared_preload_libraries='pgtt_bgw,pgtt_rsl'

You'll be able to set the two following parameters in configuration
file:

* pgtt_bgw.naptime = 5

this is the interval used between each cleaning cycle, 5 seconds
per default.

* pgtt_bgw.analyze = off

Force the background worker to execute an ANALYZE after each run.
Default is off, it is better to let autoanalyze to the work.

To avoid too much performances lost when the background worker is
deleting obsolete rows the limit of rows removed at one time is
defined by the following directive:

* pgtt_bgw.chunk_size = 250000

Default is to remove rows by chunk of 250000 tuples.


Once this configuration is done, restart PostgreSQL.

The background worker will wake up each naptime interval to scan
all database using the pgtt_rsl extension. It will then remove all rows
that don't belong to an existing session or transaction.

### [Use of the extension](#use-of-the-extension)

In all database where you want to use Global Temporary Tables you
will have to create the extension using:

* CREATE EXTENSION pgtt_rsl;

The extension comes with two functions that can be used instead of
the CREATE GLOBAL TEMPORARY TABLE statement. To create a GTT table
named test_table you can use one of the following statements:

* `CREATE GLOBAL TEMPORARY TABLE test_table (id integer, lbl text) ON COMMIT PRESERVE ROWS;`
* `SELECT pgtt_schema.pgtt_create_table('test_table', 'id integer, lbl text', true);`

The first argument of the pgtt_create_table() function is the name
of the permanent temporary table. The second is the definition of
the table. The third parameter is a boolean to indicate if the rows
should be preserved at end of the transaction or deleted at commit.

The CREATE GLOBAL TEMPORARY syntax output a message saying the GLOBAL keyword
is deprecated but this is just a warning. If you don't want to be annoyed by
this messages, comment the GLOBAL keyword like this:

* `CREATE /*GLOBAL*/ TEMPORARY TABLE test_table (id integer, lbl text) ON COMMIT PRESERVE ROWS;`

Once the table is created it can be used by the application like any
temporary table. A session will only see its rows for the time of a
session or a transaction following if the temporary table preserves
the rows at end of the transaction or deleted them at commit.

To drop a Global Temporary Table you can use one of the following statements:

* `DROP TABLE test_table;`
* `SELECT pgtt_schema.pgtt_drop_table('test_table');`

### [How the extension really works](#how-the-extension-really-works)

When you call the pgtt_create_table() function or CREATE GLOBAL TEMPORARY TABLE
the pgtt_rsl extension act as follow:

  1) Create an unlogged table in the pgtt_schema schema and renamed it
     with 'pgtt_' and the oid of the table newly created. Alter the table
     to add an "hidden" column 'pgtt_sessid' of custom type lsid.
     The users must not access to this table directly.
     The pgtt_sessid column has default to get_session_id(), this
     function is part of the pgtt_extension and build a session
     local unique id from the backend start time (epoch) and the pid.
     The custom type use is a C structure of two integers:
```
	typedef struct Lsid {
	    int      backend_start_time;
	    int      backend_pid;
	} Lsid;
```
  2) Create a view with the name and the schema of the origin global
     temporary table. All user access to the underlying table will be
     done through this view.
  3) Create a btree index on column pgtt_sessid of special type lsid.
  4) Grant SELECT,INSERT,UPDATE,DELETE on the table to PUBLIC.
  5) Activate RLS on the table and create two RLS policies to hide rows
     to other sessions or transactions when required.
  6) Force RLS to be active for the owner of the table.
  7) Create an updatable view using the original table name with a
     a WHERE clause to hide the "hidden" column of the table.
  8) Set owner of the view to current_user which might be a superuser,
     grants to other users are the responsability of the administrator.
  9) Insert the relation between the GTT and the view in the catalog
     table pgtt_global_temp.

The pgtt_drop_table() function is responsible to remove all references
to a GTT table and its corresponding view. When it is called it just
execute a "DROP TABLE IF EXISTS pgtt_schema.pgtt_tbname CASCADE;".
Using CASCADE will also drop the associated view.

The "DROP TABLE pgtt_table" statement will drop the view instead, the
corresponding table in extension's schema pgtt_schema will be removed
later by the background worker.

The extension also define four functions to manipulate the 'lsid' type:
    
* get_session_id(): generate the local session id and returns an lsid.
* generate_lsid(int, int): generate a local session id based on a backend
  start time (number of second since epoch) and a pid of the backend.
  Returns an lsid.
* get_session_start_time(lsid): returns the number of second since epoch
  part of an lsid.
* get_session_pid(lsid): returns the pid part of an lsid.

Here is an example of use of these functions:
```
test=# SELECT get_session_id();
   get_session_id   
--------------------
 {1527703231,11007}

test=# SELECT generate_lsid(1527703231,11007);
   generate_lsid    
--------------------
 {1527703231,11007}

test=# SELECT get_session_start_time(get_session_id());
 get_session_start_time 
------------------------
             1527703231

test=# SELECT get_session_pid(get_session_id());
 get_session_pid 
-----------------
           11007
```

The purpose of these functions is for internal and debuging use only.

Behind the background worker
============================

The pgtt_bgw background worker is responsible of removing all rows
that are no more visible to any running session or transaction.

To achieve that it wakes up each naptime (default 5 seconds) and
creates dedicated dynamic background workers to connect to each
database. When the pgtt extension is found in the database it calls
the pgtt_schema.pgtt_maintenance() function.

The first argument of the function is the iteration number, 0 mean
that the background worker was just started so Global Temporary tables
must be truncated. The iteration number is incremented each time
the background worker wakes up.

To avoid too much performances lost when the background worker is
deleting rows the number of rows removed at one time is defined
by the pgtt_rsl.chunk_size GUC. It is passed to the function as second
parameter, default: 250000.

The third parameter is use to force an ANALYZE to be perform just
after obsolete rows have been removed. Default is to leave autoanalyze
do the work.

The pgtt_schema.pgtt_maintenance() has the following actions:

  1) Look for relation id in table pgtt_schema.pgtt_global_temp.
  2) Remove all references to the relation if it has been dropped.
  3) If the relation exists it truncate the GTT table when the worker
     is run at postmaster startup then exit.
  4) For other iteration it delete rows from the Global Temporary table
     where the pid stored in pgtt_sessid doesn't appears in view
     pg_stat_activity or if the xmim is not fount in the same view,
     looking at pid and backend_xid columns. Xmin is verified only if
     the Global Temporary table is declared to delete rows on commit,
     when preserved is false.
  5) When an analyze is asked the function execute an ANALYZE on
     the table after having removed obsolete tuples. THe default is to
     let autoanalyze do is work.

The function returns the total number of rows deleted. It must not be
run manually but only run by pgtt_bgw the background worker.

### [Authors](#authors)

- Gilles Darold

### [License](#license)

This extension is free software distributed under the PostgreSQL
Licence.

        Copyright (c) 2018-2024, Gilles Darold

