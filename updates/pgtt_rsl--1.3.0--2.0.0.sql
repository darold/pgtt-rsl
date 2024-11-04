CREATE OR REPLACE FUNCTION pgtt_schema.pgtt_create_table (tb_name name, code text, preserved boolean DEFAULT false, relnspname name DEFAULT 'public')
RETURNS boolean
AS $$
DECLARE
	qbased text DEFAULT NULL;
	column_list text;
	relid oid;
BEGIN
	-- Compute the query to create the global temporary table

	-- Look if the table is created from a SELECT/WITH statement or not
	SELECT REGEXP_MATCHES(code, E'^(SELECT|WITH)', 'i') INTO qbased;
	IF qbased IS NULL THEN
		-- With a basic declaration
		EXECUTE format('CREATE UNLOGGED TABLE pgtt_schema.%I (%s)', tb_name, code);
	ELSE
		-- With a declaration based on a query
		EXECUTE format('CREATE UNLOGGED TABLE pgtt_schema.%I AS %s', tb_name, code);
	END IF;
	-- Append pgtt_sessid "internal" column to the GTT table
	EXECUTE format('ALTER TABLE pgtt_schema.%I ADD COLUMN pgtt_sessid lsid DEFAULT get_session_id()', tb_name);

	-- Get the oid of the relation and rename it using this oid.
	SELECT c.oid INTO relid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = tb_name AND n.nspname = 'pgtt_schema';
	EXECUTE format('ALTER TABLE pgtt_schema.%I RENAME TO pgtt_%s', tb_name, relid);

	-- Create an index on pgtt_sessid column, this will slow
	-- down insert but this will help for select from the view
	EXECUTE format('CREATE INDEX ON pgtt_schema.pgtt_%s (pgtt_sessid)', relid);

	-- Allow all on the global temporary table except
	-- truncate and drop to everyone
	EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON pgtt_schema.pgtt_%s TO PUBLIC', relid);

	-- Activate RLS to set policy based on session
	EXECUTE format('ALTER TABLE pgtt_schema.pgtt_%s ENABLE ROW LEVEL SECURITY', relid);

	-- if ON COMMIT PRESERVE ROWS is enabled
	IF preserved THEN
		-- Create the policy that must be applied on the table
		-- to show only rows where pgtt_sessid is the same as
		-- current pid.
		EXECUTE format('CREATE POLICY pgtt_rls_session ON pgtt_schema.pgtt_%s USING (pgtt_sessid = get_session_id()) WITH CHECK (true)', relid);
	ELSE
		-- Create the policy that must be applied on the table
		-- to show only rows where pgtt_sessid is the same as
		-- current pid and rows that have been created in the
		-- current transaction.
		EXECUTE format('CREATE POLICY pgtt_rls_transaction ON pgtt_schema.pgtt_%s USING (pgtt_sessid = get_session_id() AND xmin::text >= txid_current()::text) WITH CHECK (true)', relid);
	END IF;
	-- Force policy to be active for the owner of the table
	EXECUTE format('ALTER TABLE pgtt_schema.pgtt_%s FORCE ROW LEVEL SECURITY', relid);

	-- Collect all visible column of the table attnum >= 1 as
	-- column pgtt_sessid must not be reported by the view.
	SELECT string_agg(attname, ',' ORDER BY attnum) INTO column_list FROM pg_attribute WHERE attrelid=('pgtt_schema.pgtt_'||relid)::regclass AND attname != 'pgtt_sessid' AND attnum >= 1 AND NOT attisdropped;

	-- Create a view named as the table name given by the user
	-- so that he will only deal with this name, and never the
	-- internal name of the corresponding table prefixed with
	-- pgtt_. The view is also used to hide the pgtt_sessid column.
	IF preserved THEN
		EXECUTE format('CREATE VIEW %I.%I WITH (security_barrier) AS SELECT %s from pgtt_schema.pgtt_%s WHERE pgtt_sessid=get_session_id()', relnspname, tb_name, column_list, relid);
	ELSE
		EXECUTE format('CREATE VIEW %I.%I WITH (security_barrier) AS SELECT %s from pgtt_schema.pgtt_%s WHERE pgtt_sessid=get_session_id() AND xmin::text >= txid_current()::text', relnspname, tb_name, column_list, relid);
	END IF;

	-- Set owner of the view to session user, not the function definer (superuser)
	EXECUTE format('ALTER VIEW %I.%I OWNER TO %s', relnspname, tb_name, session_user);

	-- Allow read+write to every one on this view - disable here because the
	-- owner is responsible of setting privilege on this view
	-- EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON %s TO PUBLIC', tb_name);

	-- Register the link between the view and the unlogged table
	EXECUTE format('INSERT INTO pgtt_schema.pgtt_global_temp (relid, viewid, datcrea, preserved) VALUES (%s, %s, now(), %L)',
		(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = 'pgtt_'||relid AND n.nspname = 'pgtt_schema'),
		(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = tb_name AND n.nspname = relnspname), preserved);

	RETURN true;
END;
$$
LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pgtt_schema.pgtt_drop_table (tb_name name, nspname name DEFAULT 'public')
RETURNS boolean
AS $$
DECLARE
	relid oid;
BEGIN
	-- Get the view Oid
        EXECUTE format('SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = %L AND n.nspname = %L', tb_name, nspname) INTO relid;
        -- Unregister the table/view relation from pgtt_schema.pgtt_global_temp table and return the relation oid
        EXECUTE format('DELETE FROM pgtt_schema.pgtt_global_temp WHERE viewid=%s RETURNING relid', relid) INTO relid;
	-- Compute the query to remove the global temporary table and
	-- related objects, with CASCADE associated view will be removed.
	EXECUTE format('DROP TABLE IF EXISTS pgtt_schema.pgtt_%s CASCADE', relid);

	RETURN true;
END;
$$
LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pgtt_schema.pgtt_maintenance (iter integer DEFAULT 1, chunk_size integer DEFAULT 250000, analyze_table boolean DEFAULT false)
RETURNS bigint
AS $$
DECLARE
	cur_gtt_tables CURSOR FOR SELECT viewid,relid,preserved FROM pgtt_schema.pgtt_global_temp;
	class_info RECORD;
	query text;
	rec RECORD;
	nrows bigint;
	total_nrows bigint;
	alive integer;
	relid oid;
BEGIN
	total_nrows := 0;

	-- For all global temporary tables defined in pgtt_schema.pgtt_global_temp
	OPEN cur_gtt_tables;
	LOOP
		FETCH NEXT FROM cur_gtt_tables INTO class_info;
		EXIT WHEN NOT FOUND;
		-- Check if the table have been removed with a direct DROP+CASCADE
		EXECUTE 'SELECT oid FROM pg_class WHERE oid=' || class_info.relid INTO relid;
		IF relid IS NULL THEN
			-- Cleanup all references to this table in our GTT registery table
			EXECUTE 'DELETE FROM pgtt_schema.pgtt_global_temp WHERE relid=' || class_info.relid;
			CONTINUE;
		END IF;
		-- Check if the view have been removed with a direct DROP
		EXECUTE 'SELECT oid FROM pg_class WHERE oid=' || class_info.viewid INTO relid;
		IF relid IS NULL THEN
			-- Drop the table if it is not already the case
			EXECUTE 'DROP TABLE pgtt_schema.pgtt_' || class_info.relid;
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
			EXECUTE 'DELETE FROM ' || class_info.relid::regclass || ' WHERE ctid = ANY(ARRAY(SELECT ctid FROM ' || class_info.relid::regclass || ' WHERE NOT (xmin = ANY(ARRAY(SELECT DISTINCT backend_xmin FROM pg_stat_activity WHERE backend_xmin IS NOT NULL))) LIMIT ' || chunk_size || '))';
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
