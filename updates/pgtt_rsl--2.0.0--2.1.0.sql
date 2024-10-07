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
	EXECUTE format('INSERT INTO pgtt_schema.pgtt_global_temp (relid, viewid, datcrea, preserved, relname, relnspname) VALUES (%s, %s, now(), %L, %L, %L)',
		(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = 'pgtt_'||relid AND n.nspname = 'pgtt_schema'),
		(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = tb_name AND n.nspname = relnspname), preserved, tb_name, relnspname);

	RETURN true;
END;
$$
LANGUAGE plpgsql SECURITY DEFINER;

