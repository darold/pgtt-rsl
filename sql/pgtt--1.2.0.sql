-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgtt" to load this file. \quit

-- Create the type used to store the local session id
CREATE TYPE lsid;
CREATE FUNCTION lsid_in(cstring) RETURNS lsid AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION lsid_out(lsid) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION lsid_recv(internal) RETURNS lsid AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION lsid_send(lsid) RETURNS bytea AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE lsid (
        INTERNALLENGTH = 8, -- Composed of 2 int4
        INPUT = lsid_in,
        OUTPUT = lsid_out,
        RECEIVE = lsid_recv,
        SEND = lsid_send,
	ALIGNMENT = int4
);

----
-- Interfacing new lsid type with indexes:
----

-- Define the required operators
CREATE FUNCTION lsid_lt(lsid, lsid) RETURNS bool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION lsid_le(lsid, lsid) RETURNS bool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION lsid_eq(lsid, lsid) RETURNS bool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION lsid_ge(lsid, lsid) RETURNS bool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION lsid_gt(lsid, lsid) RETURNS bool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR < (
	leftarg = lsid, rightarg = lsid, procedure = lsid_lt,
	commutator = > , negator = >= ,
	restrict = scalarltsel, join = scalarltjoinsel
);
CREATE OPERATOR <= (
	leftarg = lsid, rightarg = lsid, procedure = lsid_le,
	commutator = >= , negator = > ,
	restrict = scalarltsel, join = scalarltjoinsel
);
CREATE OPERATOR = (
	leftarg = lsid, rightarg = lsid, procedure = lsid_eq,
	commutator = = , -- leave out negator since we didn't create <> operator
	restrict = eqsel, join = eqjoinsel
);
CREATE OPERATOR >= (
	leftarg = lsid, rightarg = lsid, procedure = lsid_ge,
	commutator = <= , negator = < ,
	restrict = scalargtsel, join = scalargtjoinsel
);

CREATE OPERATOR > (
	leftarg = lsid, rightarg = lsid, procedure = lsid_gt,
	commutator = < , negator = <= ,
	restrict = scalargtsel, join = scalargtjoinsel
);

-- create the support function too
CREATE FUNCTION lsid_cmp(lsid, lsid) RETURNS int4 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

-- now we can make the operator class
CREATE OPERATOR CLASS lsid_ops
    DEFAULT FOR TYPE lsid USING btree AS
        OPERATOR        1       < ,
        OPERATOR        2       <= ,
        OPERATOR        3       = ,
        OPERATOR        4       >= ,
        OPERATOR        5       > ,
        FUNCTION        1       lsid_cmp(lsid, lsid);

CREATE CAST (lsid AS int[]) WITH INOUT AS ASSIGNMENT;
CREATE CAST (int[] AS lsid) WITH INOUT AS ASSIGNMENT;


CREATE FUNCTION get_session_id() RETURNS lsid AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION generate_lsid(int, int) RETURNS lsid AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION get_session_start_time(lsid) RETURNS int AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION get_session_pid(lsid) RETURNS int AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;

----
-- Create schema dedicated to the global temporary table
----
CREATE SCHEMA pgtt_schema;
REVOKE ALL ON SCHEMA pgtt_schema FROM PUBLIC;
GRANT USAGE ON SCHEMA pgtt_schema TO PUBLIC;

SET LOCAL search_path TO pgtt_schema,pg_catalog;

----
-- Table for meta information about Global Temporary Table.
--     - relid   : oid of the GTT table
--     - relname : name of the GTT table
--     - viewid  : oid of the corresponding view acceeded by the users
--     - viewname: name of the view pointing to the GTT
--     - datcrea : creation date of the GTT relation
----
CREATE TABLE pgtt_global_temp (
	relid oid NOT NULL,
	viewid oid NOT NULL,
	datcrea timestamp NOT NULL,
	preserved boolean,
	UNIQUE (relid, viewid)
);

-- Include tables into pg_dump
SELECT pg_catalog.pg_extension_config_dump('pgtt_global_temp', '');

----
-- Global temporary tables are created using function
--     pgtt_create_table(name, code, preserved)
-- Parameters:
--     tb_name  : name of the global temporary table limited to 54 characters
--     code     : columns definition or SQL query
--     preserved:
--          true : Rows are deleted after session ends (ON COMMIT PRESERVE ROWS)
--          false: Rows are deleted after COMMIT (ON COMMIT DELETE ROWS) - Default
-- This function must be called only one time at database schema creation
-- or after a call to pgtt_drop_table() to be recreated. It use RLS to
-- restrict access to session and transaction id, we FORCE RLS so that the
-- database owner will have restriction too. As superuser you may be able to
-- see all rows, as database owner use NO FORCE RLS before to see all rows.
--
-- It is possible to create indexes on GTT, statistics are maintained.
-- Global temporary tables are truncated at postmaster startup.
--
-- This function can be used in replacement of CREATE GLOBAL TEMPORARY TABLE
-- syntax until GLOBAL TEMPORARY will be supported and not reported as obsolete.
-- It is also already possible to use [ ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP } ]
-- The pgtt extenstion allow intercepting this syntax in the processUtility hook and
-- create the GTT.
----
CREATE FUNCTION pgtt_create_table (tb_name varchar(54), code text, preserved boolean DEFAULT false)
RETURNS boolean
AS $$
DECLARE
	qbased text DEFAULT NULL;
	column_list text;
BEGIN
	-- Compute the query to create the global temporary table

	-- Look if the table is created from a SELECT/WITH statement or not
	SELECT REGEXP_MATCHES(code, E'^(SELECT|WITH)', 'i') INTO qbased;
	IF qbased IS NULL THEN
		-- With a basic declaration
		EXECUTE format('CREATE UNLOGGED TABLE pgtt_schema.pgtt_%s (%s)', tb_name, code);
	ELSE
		-- With a declaration based on a query
		EXECUTE format('CREATE UNLOGGED TABLE pgtt_schema.pgtt_%s AS %s', tb_name, code);
	END IF;
	-- Append pgtt_sessid "internal" column to the GTT table
	EXECUTE format('ALTER TABLE pgtt_schema.pgtt_%s ADD COLUMN pgtt_sessid lsid DEFAULT get_session_id()', tb_name);

	-- Create an index on pgtt_sessid column, this will slow
	-- down insert but this will help for select from the view
	EXECUTE format('CREATE INDEX ON pgtt_schema.pgtt_%s (pgtt_sessid)', tb_name);

	-- Allow all on the global temporary table except
	-- truncate and drop to everyone
	EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON pgtt_schema.pgtt_%s TO PUBLIC', tb_name);

	-- Activate RLS to set policy based on session
	EXECUTE format('ALTER TABLE pgtt_schema.pgtt_%s ENABLE ROW LEVEL SECURITY', tb_name);
	-- if ON COMMIT PRESERVE ROWS is enabled
	IF preserved THEN
		-- Create the policy that must be applied on the table
		-- to show only rows where pgtt_sessid is the same as
		-- current pid.
		EXECUTE format('CREATE POLICY pgtt_rls_session ON pgtt_schema.pgtt_%s USING (pgtt_sessid = get_session_id()) WITH CHECK (true)', tb_name);
	ELSE
		-- Create the policy that must be applied on the table
		-- to show only rows where pgtt_sessid is the same as
		-- current pid and rows that have been created in the
		-- current transaction.
		EXECUTE format('CREATE POLICY pgtt_rls_transaction ON pgtt_schema.pgtt_%s USING (pgtt_sessid = get_session_id() AND xmin::text = txid_current()::text) WITH CHECK (true)', tb_name);
	END IF;
	-- Force policy to be active for the owner of the table
	EXECUTE format('ALTER TABLE pgtt_schema.pgtt_%s FORCE ROW LEVEL SECURITY', tb_name);

	-- Collect all visible column of the table attnum >= 1 as
	-- column pgtt_sessid must not be reported by the view.
	SELECT string_agg(attname, ',' ORDER BY attnum) INTO column_list FROM pg_attribute WHERE attrelid=('pgtt_schema.pgtt_'||tb_name)::regclass AND attname != 'pgtt_sessid' AND attnum >= 1 AND NOT attisdropped;

	-- Create a view named as the table name given by the user
	-- so that he will only deal with this name, and never the
	-- internal name of the corresponding table prefixed with
	-- pgtt_. The view is also used to hide the pgtt_sessid column.
	IF preserved THEN
		EXECUTE format('CREATE VIEW %s WITH (security_barrier) AS SELECT %s from pgtt_schema.pgtt_%s WHERE pgtt_sessid=get_session_id()', tb_name, column_list, tb_name);
	ELSE
		EXECUTE format('CREATE VIEW %s WITH (security_barrier) AS SELECT %s from pgtt_schema.pgtt_%s WHERE pgtt_sessid=get_session_id() AND xmin::text = txid_current()::text', tb_name, column_list, tb_name);
	END IF;

	-- Set owner of the view to current user, not the function definer (superuser)
	EXECUTE format('ALTER VIEW %s OWNER TO %s', tb_name, current_user);

	-- Allow read+write to every one on this view - disable here because the
	-- owner is responsible of setting privilege on this view
	-- EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON %s TO PUBLIC', tb_name);

	-- Register the link between the view and the unlogged table
	EXECUTE format('INSERT INTO pgtt_schema.pgtt_global_temp (relid, viewid, datcrea, preserved) VALUES (%s, %s, now(), %L)',
		(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = 'pgtt_'||tb_name AND n.nspname = 'pgtt_schema'),
		(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = tb_name AND n.nspname = current_schema), preserved);

	RETURN true;
END;
$$
LANGUAGE plpgsql SECURITY DEFINER;

----
-- Global temporary tables are droped using function
--     pgtt_drop_table(name)
-- Parameters:
--     tb_name : name of the global temporary table
-- This function can be called at any time. Associated indexes
-- will be automatically removed.
--
-- This function can be use in replacement of "DROP TABLE tbname"
-- but it is not necessary, the ProcessUtility hook used in pgtt extension
-- take care of removing everything.
----
CREATE FUNCTION pgtt_schema.pgtt_drop_table (tb_name name)
RETURNS boolean
AS $$
BEGIN
        -- Unregister the table/view relation from pgtt_schema.pgtt_global_temp table.
        EXECUTE format('DELETE FROM pgtt_schema.pgtt_global_temp WHERE relid=%s',
                (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = 'pgtt_'||tb_name AND n.nspname = 'pgtt_schema'));

	-- Compute the query to remove the global temporary table and
	-- related indexes, with CASCADE associated view will be removed.
	EXECUTE format('DROP TABLE IF EXISTS pgtt_schema.pgtt_%s CASCADE', tb_name);

	RETURN true;
END;
$$
LANGUAGE plpgsql SECURITY DEFINER;

----
-- This function is called internally by the background worker pgtt_bgw
-- to remove obsolete rows from all global temporary tables.
-- Rows are removed by chunk of chunk_size (250000 irows by default) to
-- try to preserve the performances.
--
-- When the function is called at postmaster startup (iter = 0) it will
-- just truncate all the global temporary tables.
----
CREATE OR REPLACE FUNCTION pgtt_maintenance (iter bigint DEFAULT 1, chunk_size integer DEFAULT 250000, analyze_table boolean DEFAULT false)
RETURNS bigint
AS $$
DECLARE
	cur_gtt_tables CURSOR FOR SELECT relid,preserved FROM pgtt_schema.pgtt_global_temp;
	class_info RECORD;
	query text;
	rec RECORD;
	nrows bigint;
	total_nrows bigint;
	alive integer;
BEGIN
	total_nrows := 0;

	-- For all global temporary tables defined in pgtt_schema.pgtt_global_temp
	OPEN cur_gtt_tables;
	LOOP
		FETCH NEXT FROM cur_gtt_tables INTO class_info;
		EXIT WHEN NOT FOUND;
		-- Check that the table have not been removed with a direct DROP
		-- in this case regclass doesn't change oid to table name.
		IF (class_info.relid::regclass::text = class_info.relid::text) THEN
			-- Cleanup all references to this table
			EXECUTE 'DELETE FROM pgtt_schema.pgtt_global_temp WHERE relid=' || class_info.relid;
			CONTINUE;
		END IF;

		-- At startup iter = 0 then we just have to truncate the table
		IF (iter = 0) THEN
			EXECUTE 'TRUNCATE ' || class_info.relid::regclass;
			-- RAISE LOG 'GTT table "%" has been truncated at startup.', class_info.relid::regclass; 
			CONTINUE;
		END IF;

		-- With a GTT that preserves tuples in an entire session
		IF (class_info.preserved) THEN
			-- delete rows from the GTT table that do not belong to an active session
			EXECUTE 'DELETE FROM ' || class_info.relid::regclass || ' WHERE ctid = ANY(ARRAY(SELECT ctid FROM ' || class_info.relid::regclass || ' WHERE NOT (pgtt_sessid = ANY(ARRAY(SELECT generate_lsid(extract(epoch from backend_start)::int, pid) FROM pg_stat_activity))) LIMIT ' || chunk_size || '))';

			GET DIAGNOSTICS nrows = ROW_COUNT;
			total_nrows := total_nrows + nrows;
		-- With GTT where tuples do not survive a transaction
		ELSE
			-- delete rows from the GTT table that do not belong to an active transaction
			EXECUTE 'DELETE FROM ' || class_info.relid::regclass || ' WHERE ctid = ANY(ARRAY(SELECT ctid FROM ' || class_info.relid::regclass || ' WHERE NOT (xmin = ANY(ARRAY(SELECT backend_xid FROM pg_stat_activity))) LIMIT ' || chunk_size || '))';
			GET DIAGNOSTICS nrows = ROW_COUNT;
			total_nrows := total_nrows + nrows;
		END IF;

		-- Force an analyze of the table if required
		IF analyze_table AND total_nrows > 0 THEN
			EXECUTE 'ANALYZE ' || class_info.relid::regclass;
		END IF;
	END LOOP;
	CLOSE cur_gtt_tables;

	RETURN total_nrows;
END;
$$
LANGUAGE plpgsql SECURITY DEFINER;
