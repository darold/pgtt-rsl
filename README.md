PG Global Temporary Tables
==========================

pgtt is a PostgreSQL extension to create and manage Oracle-style
Global Temporary Tables. It is based on unlogged tables, Row Security
Level and views. A background worker is responsible to periodicaly
remove obsolete rows.

PostgreSQL native temporary tables are automatically dropped at the
end of a session, or optionally at the end of the current transaction.
Oracle Global Temporary Tables (GTT) are permanent, they are created
as regular tables visible to all users but their content is relative
to the current session or transaction. Even if the table is persistent
a session or transction can not see rows written by an other session.

An other difference is that Oracle Global Temporary Table can be
created in any schema while it is not possible with PostgreSQL where
temporary table are stored in the pg_temp namespace.

Usually this is not a problem, you have learn to deal with the
temporary table behavior of PostgreSQL but the problem comes when
you are migrating an Oracle database to PostgreSQL. You have to
rewrite the SQL and PlPgSQL code to follow the application logic and
use PostgreSQL temporary table, that mean recreating the temporary
table everywhere it is used.

The other advantage of this kind of object when your application
create and delete a lot of temporary tables this will add bloat in
the PostgreSQL catalog and the performances will start to fall.
Using Global Temporary Table will save this catalog bloating. They
are permanent tables and so on permanent indexes, they will be found
by the autovacuum process in contrary of local temporary tables.

The objective of this extension it to create a POC about the Global
Temporary Table feature. Don't expect high performances if you insert
huge amount of tuple in these tables.

Installation
============

To install the pgtt extension you need a PostgreSQL version upper than
9.4. Untar the pgtt tarball anywhere you want then you'll need to
compile it with pgxs. So the pg_config tool must be in your path.

Depending on your installation, you may need to install some devel
package and especially the libpq devel package. Once pg_config is in
your path, do "make", and then "make install".

Configuration
=============

The background worker used to remove obsolete rows from global
temporary tables must be loaded on database start by adding the
library to shared_preload_libraries in postgresql.conf. You also
need to load the pgtt library defineing C function used by the
extension.

* shared_preload_libraries='pgtt_bgw,pgtt'

You'll be able to set the two following parameters in configuration
file:

* pgtt_bgw.naptime = 5

this is the interval used between each cleaning cycle, 5 seconds
per default.

* pgtt.analyze = off

Force the background worker to execute an ANALYZE after each run.
Default is off, it is better to let autoanalyze to the work.

To avoid too much performances lost when the background worker is
deleting obsolete rows the limit of rows removed at one time is
defined by the following directive:

* pgtt.chunk_size = 250000

Default is to remove rows by chunk of 250000 tuples.


Once this configuration is done, restart PostgreSQL.

The background writer will wake up each naptime interval to scan
all database using the pgtt extension. It will then remove all rows
that don't belong to an existing session or transaction.

Use of the extension
====================

In all database where you want to use Global Temporary Tables you
will have to create the extension using:

* CREATE EXTENSION pgtt;

The extension comes with two functions that must be used instead of
the CREATE GLOBAL TEMPORARY TABLE statement. To create a GTT table
named test_table use the following statement:

* SELECT pgtt_schema.pgtt_create_table('test_table', 'id integer, lbl text', true);

The first argument of the pgtt_create_table() function is the name
of the permanent temporary table. The second is the definition of
the table. The third parameter is a boolean to indicate if the rows
should be preserved at end of the transaction or deleted at commit.

Here it could be like creating a table:

	CREATE GLOBAL TEMPORARY TABLE test_table (
		id integer,
		lbl text
	) ON COMMIT PRESERVE ROWS;


To drop a Global Temporary Table you must use the following function:

* SELECT pgtt_schema.pgtt_drop_table('test_table');

Behind the functions
=====================

How the extension really works? When you call the pgtt_create_table()
function the pgtt extension act as follow:

  1) create an unlogged table of the same name but prefixed with 'pgtt_'
     with the "hidden" column for a GTT (pgtt_sessid).
     The table is stored in extension schema pgtt_schema and the users
     must not access to this table directly. They have to use the view
     instead. The pgtt_sessid column has default to get_session_id(),
     this function is part of the pgtt_extension and build a session
     unique id from the backend start time (epoch) and the pid.
  2) grant SELECT,INSERT,UPDATE,DELETE on the table to PUBLIC.
  3) activate RLS on the table and create two RLS policies to hide rows
     to other sessions or transactions when required.
  4) force RLS to be active for the owner of the table.
  5) create an updatable view using the original table name with a
     a WHERE clause to hide the "hidden" column of the table.
  6) set owner of the view to current_user which might be a superuser,
     grants to other users are the responsability of the administrator.
  7) insert the relation between the gtt and the view in the catalog
     table pgtt_global_temp.

The pgtt_drop_table() function is responsible to remove all references
to a GTT table and its corresponding view. When it is called it just
execute a "DROP TABLE IF EXISTS pgtt_schema.pgtt_tablename CASCADE;".
Using CASCADE will also drop the associated view.

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
by the pgtt_chunk_size GUC. It is passed to the function as second
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
     where pgtt_sessid or xmim doesn't appears in pg_stat_activity
     looking at pid and backend_xid columns. Xmin is verified only if
     the Global Temporary table is declared to delete rows on commit,
     when preserved is false.
  5) When an analyze is asked the function execute an ANALYZE on
     the table after having removed obsolete tuples.

The function returns the total number of rows deleted. It must not be
run manually but only run by pgtt_bgw the background worker.

