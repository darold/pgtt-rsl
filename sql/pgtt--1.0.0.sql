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
	EXECUTE format('ALTER TABLE pgtt_schema.pgtt_%s ADD COLUMN pgtt_sessid integer DEFAULT pg_backend_pid()', tb_name);

	-- Create an index on pgtt_sessid column, this will slow donw insert
	-- but this will help lot for select with the view
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
		EXECUTE format('CREATE POLICY pgtt_rls_session ON pgtt_schema.pgtt_%s USING (pgtt_sessid = pg_backend_pid()) WITH CHECK (true)', tb_name);
	ELSE
		-- Create the policy that must be applied on the table
		-- to show only rows where pgtt_sessid is the same as
		-- current pid and rows that have been created in the
		-- current transaction.
		EXECUTE format('CREATE POLICY pgtt_rls_transaction ON pgtt_schema.pgtt_%s USING (pgtt_sessid = pg_backend_pid() AND xmin::text = txid_current()::text) WITH CHECK (true)', tb_name);
	END IF;
	-- Force policy to be active for the owner of the table
	EXECUTE format('ALTER TABLE pgtt_schema.pgtt_%s FORCE ROW LEVEL SECURITY', tb_name);

	-- Collect all visible column of the table attnum > 2 as columns
	-- pgtt_sessid and pgtt_txid must not be reported by the view.
	SELECT string_agg(attname, ',' ORDER BY attnum) INTO column_list FROM pg_attribute WHERE attrelid=('pgtt_schema.pgtt_'||tb_name)::regclass AND attname != 'pgtt_sessid' AND attnum >= 1 AND NOT attisdropped;

	-- Create a view to hide the GTT's columns and named
	-- as the table name given by the user so that he will
	-- only deal with this name, not the internal name of
	-- the corresponding table prefixed with pgtt_.
	IF preserved THEN
		EXECUTE format('CREATE VIEW %s WITH (security_barrier) AS SELECT %s from pgtt_schema.pgtt_%s WHERE pgtt_sessid=pg_backend_pid()', tb_name, column_list, tb_name);
	ELSE
		EXECUTE format('CREATE VIEW %s WITH (security_barrier) AS SELECT %s from pgtt_schema.pgtt_%s WHERE pgtt_sessid=pg_backend_pid() AND xmin::text = txid_current()::text', tb_name, column_list, tb_name);
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
	-- Compute the query to remove the global temporary table
	-- and related indexes with CASCADE associated view will
	-- be removed.
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
			EXECUTE 'DELETE FROM ' || class_info.relid::regclass || ' WHERE ctid = ANY(ARRAY(SELECT ctid FROM ' || class_info.relid::regclass || ' WHERE NOT (pgtt_sessid = ANY(ARRAY(SELECT pid FROM pg_stat_activity))) LIMIT ' || chunk_size || '))';

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
