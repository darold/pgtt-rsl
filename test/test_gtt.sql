----
-- Regression test to Global Temporary Table RSL implementation with use of
-- pgtt_schema.pgtt_create_table and pgtt_schema.pgtt_drop_table functions.
--
-- LANG=C psql -f test/test_gtt.sql > result.txt 2>&1
-- diff result.txt test/expected/test_gtt.txt
----
DROP DATABASE IF EXISTS gtt_testdb;
DROP ROLE IF EXISTS test_gtt1;
DROP ROLE IF EXISTS test_gtt2;
CREATE ROLE test_gtt1 LOGIN PASSWORD 'test_gtt1';
CREATE ROLE test_gtt2 LOGIN PASSWORD 'test_gtt2';

CREATE DATABASE gtt_testdb OWNER test_gtt1 ;

-- Connect as superuser
\c gtt_testdb

-- Create the PostgreSQL extension
CREATE EXTENSION pgtt_rsl;

-- Create a GTT like table to test ON COMMIT PRESERVE ROWS
SELECT pgtt_schema.pgtt_create_table('t_glob_temptable1', 'id integer, lbl text', true);
-- This syntax is not yet available
-- CREATE GLOBAL TEMPORARY TABLE t_glob_temptable1 (id integer, lbl text) ON COMMIT PRESERVE ROWS;
GRANT ALL ON t_glob_temptable1 TO test_gtt1,test_gtt2;

-- Create a GTT like table to test ON COMMIT DELETE ROWS using CREATE GLOBAL TEMPORARY syntax
SELECT pgtt_schema.pgtt_create_table('t_glob_temptable2', 'SELECT id, lbl FROM t_glob_temptable1', false);
-- This syntax is not yet available
-- CREATE GLOBAL TEMPORARY TABLE t_glob_temptable2 AS (SELECT id, lbl FROM t_glob_temptable1) ON COMMIT DELETE ROWS;
GRANT ALL ON t_glob_temptable2 TO test_gtt1,test_gtt2;

CREATE SCHEMA foo;
GRANT ALL ON SCHEMA foo TO test_gtt1,test_gtt2;
SELECT pgtt_schema.pgtt_create_table('t_glob_temptable1', 'id integer, lbl text', true, 'foo');
GRANT ALL ON foo.t_glob_temptable1 TO test_gtt1,test_gtt2;

CREATE FUNCTION check_pgtt_count (tbname name, nspname name DEFAULT 'public') RETURNS bigint AS $$
DECLARE
	v_relid oid;
	ret bigint;
BEGIN
        EXECUTE format('SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = %L AND n.nspname = %L', tbname, nspname) INTO v_relid;
	EXECUTE format('SELECT relid FROM pgtt_schema.pgtt_global_temp WHERE viewid=%s', v_relid) INTO v_relid;
	EXECUTE 'SELECT count(*) FROM pgtt_schema.pgtt_'||v_relid INTO ret;
	RETURN ret;
END
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION check_pgtt_count TO PUBLIC;

----
-- Test ON COMMIT PRESERVE ROWS feature
----

-- Connect as test_gtt1 user
\c gtt_testdb test_gtt1

-- Insert 3 record in the view
INSERT INTO t_glob_temptable1 SELECT * FROM generate_series(1,3);

-- Should return 3 as we are still in the same session
SELECT count(*) FROM t_glob_temptable1;

BEGIN;
-- Insert 3 record in the view
INSERT INTO foo.t_glob_temptable1 SELECT * FROM generate_series(1,3);

-- Add a SAVEPOINT to increase the transaction id
SAVEPOINT sp_foo;

-- Should return 3 as we are still in the same transaction
SELECT count(*) FROM foo.t_glob_temptable1;

COMMIT;

-- Should return 0 as we are still in the same session
SELECT count(*) FROM foo.t_glob_temptable1;

-- Reconnect as test_gtt2 user, data must not be visible anymore unless to be superuser
\c - test_gtt2

-- From the view it must return 0 row
SELECT count(*) FROM t_glob_temptable1;

-- from the table directly too
SELECT check_pgtt_count('t_glob_temptable1') AS "count";

-- Insert some other records and select them
INSERT INTO t_glob_temptable1 SELECT * FROM generate_series(4,6);

-- must returns the last 3 records saved
SELECT count(*) FROM t_glob_temptable1;

-- Reconnect as test_gtt1 old and test_gtt2 data must not be visible
\c - test_gtt1

-- Insert some other records and select them
INSERT INTO t_glob_temptable1 SELECT * FROM generate_series(7,9);

-- Only last inserted records should be visible, 3 rows
SELECT count(*) FROM t_glob_temptable1;

-- even from the unlogged table
SELECT check_pgtt_count('t_glob_temptable1') AS "count";

-- Verify that from a superuser point of view everything is visible
\c - postgres

-- Everything is visible as superuser are not limited by RLS, 9 rows
SELECT check_pgtt_count('t_glob_temptable1') AS "count";

-- Nothing must appears here as we are looking through the view filter
SELECT count(*) FROM t_glob_temptable1 ;

-- Connect again as test_gtt2 user to test DELETE an UPDATE statements
\c - test_gtt2

-- Nothing must be deleted
DELETE FROM t_glob_temptable1 ;

-- Insert some other records into the view
INSERT INTO t_glob_temptable1 SELECT * FROM generate_series(10,12);

-- then the last three records must be returned
SELECT count(*) FROM t_glob_temptable1 ;

-- Same with a direct select from the unlogged table
SELECT check_pgtt_count('t_glob_temptable1') AS "count";

-- Ok delete every thing from the view, only 3 records may have been deleted
DELETE FROM t_glob_temptable1 ;

-- And a new select must return nothing
SELECT count(*) FROM t_glob_temptable1 ;

-- Insert some new records for update testing
INSERT INTO t_glob_temptable1 SELECT * FROM generate_series(10,12);

-- Increment all visible records from the table, 3 record must be updated
UPDATE t_glob_temptable1 SET id=id+1;

-- Look at individual the records, each must return one line
SELECT count(*) FROM t_glob_temptable1 WHERE id=11;
SELECT count(*) FROM t_glob_temptable1 WHERE id=12;
SELECT count(*) FROM t_glob_temptable1 WHERE id=13;

-- Update directly from table, we must have the same behavior
DO $$
DECLARE
	v_relid oid;
BEGIN
        SELECT c.oid INTO v_relid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = 't_glob_temptable1' AND n.nspname = 'public';
	EXECUTE format('SELECT relid FROM pgtt_schema.pgtt_global_temp WHERE viewid=%s', v_relid) INTO v_relid;
	EXECUTE 'UPDATE pgtt_schema.pgtt_'||v_relid||' SET id=id+1';
END
$$;

SELECT count(*) FROM t_glob_temptable1 WHERE id=12;
SELECT count(*) FROM t_glob_temptable1 WHERE id=13;
SELECT count(*) FROM t_glob_temptable1 WHERE id=14;

\c - postgres

-- Everything is visible as superuser are not limited by RLS, must
-- return 12 records with a hole on id = 10 and id = 11
SELECT check_pgtt_count('t_glob_temptable1') AS "count";

-- Nothing must appears here as we are looking through the view filter
SELECT count(*) FROM t_glob_temptable1 ;

----
-- Test ON COMMIT DELETE ROWS feature
----

-- Nothing must be return by this query as we have not inserted any row 
SELECT check_pgtt_count('t_glob_temptable2') AS "count";

-- Connect as simple user to process some test
\c - test_gtt1

-- Insert some records now in a transaction
BEGIN;
INSERT INTO t_glob_temptable2 SELECT * FROM generate_series(1,3);
-- Must return 3 
SELECT count(*) FROM t_glob_temptable2;
INSERT INTO t_glob_temptable2 SELECT * FROM generate_series(4,6);
-- Both select must return 6 
SELECT check_pgtt_count('t_glob_temptable2') AS "count";
COMMIT;

-- Outside the transaction nothing must be visible
SELECT count(*) FROM t_glob_temptable2;
SELECT check_pgtt_count('t_glob_temptable2') AS "count";

\c - test_gtt2

-- Insert some more records in a transaction
BEGIN;
INSERT INTO t_glob_temptable2 SELECT * FROM generate_series(7,9);
-- must return 3
SELECT count(*) FROM t_glob_temptable2;
DELETE FROM t_glob_temptable2;
-- must return 0
SELECT count(*) FROM t_glob_temptable2;
COMMIT;

-- Outside the transaction nothing must be visible
SELECT count(*) FROM t_glob_temptable2;
SELECT check_pgtt_count('t_glob_temptable2') AS "count";

\c - test_gtt2

-- Insert some more rows to test UPDATE
BEGIN;
INSERT INTO t_glob_temptable2 SELECT * FROM generate_series(10,12);
-- Both select must return 3
SELECT count(*) FROM t_glob_temptable2;
SELECT check_pgtt_count('t_glob_temptable2') AS "count";

-- 3 rows must be updated
UPDATE t_glob_temptable2 SET id=id+1;
-- The following select must return 1 each
SELECT count(*) FROM t_glob_temptable2 WHERE id=11;
SELECT count(*) FROM t_glob_temptable2 WHERE id=12;
SELECT count(*) FROM t_glob_temptable2 WHERE id=13;
COMMIT;

-- Outside the transaction nothing must be visible
SELECT count(*) FROM t_glob_temptable2;
SELECT check_pgtt_count('t_glob_temptable2') AS "count";

-- Check for ROLLBACK but obviously no rows are expected at output
BEGIN;
INSERT INTO t_glob_temptable2 SELECT * FROM generate_series(14,16);
-- Both select must return 3
SELECT count(*) FROM t_glob_temptable2;
SELECT check_pgtt_count('t_glob_temptable2') AS "count";
ROLLBACK;

-- Outside the transaction nothing must be visible
SELECT count(*) FROM t_glob_temptable2;
SELECT check_pgtt_count('t_glob_temptable2') AS "count";

\c - postgres

-- Everything is visible as superuser are not limited by RLS, must
-- return 9 records with a hole on id between 7 and 10
SELECT check_pgtt_count('t_glob_temptable2') AS "count";

-- Nothing must appears here as we are looking through the view filter
SELECT count(*) FROM t_glob_temptable2;

-- Drop the global temporary tables using the two syntax
SELECT pgtt_schema.pgtt_drop_table('t_glob_temptable1');
-- This syntax is not yet available
-- DROP TABLE t_glob_temptable2;
SELECT pgtt_schema.pgtt_drop_table('t_glob_temptable2');
-- This syntax is not yet available
-- DROP TABLE t_glob_temptable2;
SELECT pgtt_schema.pgtt_drop_table('t_glob_temptable1', 'foo');

-- Tests of the LSID related functions
-- Must return {1527703231,11007}
SELECT generate_lsid(1527703231, 11007);
-- Must return 1527703231
SELECT get_session_start_time(generate_lsid(1527703231, 11007));
-- Must return 11007
SELECT get_session_pid(generate_lsid(1527703231, 11007));

-- Tests of the custom operators
-- Must return false
SELECT generate_lsid(1527703231, 11007) > generate_lsid(1527703232, 11007);
-- Must return true
SELECT generate_lsid(1527703231, 11007) >= generate_lsid(1527703231, 11007);
-- Must return true
SELECT generate_lsid(1527703231, 11007) <= generate_lsid(1527703231, 11007);
-- Must return true
SELECT generate_lsid(1527703231,11007) > generate_lsid(1527703230,11007);
-- Must return true
SELECT generate_lsid(1527703231,11007) > generate_lsid(1527703231,11006);
-- Must return false
SELECT generate_lsid(1527703231,11007) > generate_lsid(1527703231,11008);
-- Must return true
SELECT generate_lsid(1527703231,11007) = generate_lsid(1527703231,11007);
-- Must return false
SELECT generate_lsid(1527703231,11007) = generate_lsid(1527703231,11008);
-- Must return false
SELECT generate_lsid(1527703231,11007) = generate_lsid(1527703230,11007);

\c postgres

-- -- Clean all
DROP DATABASE gtt_testdb;
DROP ROLE test_gtt2;
DROP ROLE test_gtt1;

