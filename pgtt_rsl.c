/*-------------------------------------------------------------------------
 *
 * pgtt.c
 *	Add support to Oracle-style Global Temporary Table in PostgreSQL.
 *	You need PostgreSQL >= 9.4 as this extension use UNLOGGED tables
 *	and a background worker.
 *
 * Author: Gilles Darold <gilles@darold.net>
 * Licence: PostgreSQL
 * Copyright (c) 2018-2022, Gilles Darold,
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <unistd.h>
#include "funcapi.h"
#include "tcop/utility.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "access/parallel.h"
#include "catalog/catalog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "utils/formatting.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "commands/tablecmds.h"
#include "access/htup_details.h"
#include "executor/spi.h"
#include "storage/proc.h"
#include "utils/builtins.h"

#if (PG_VERSION_NUM >= 120000)
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/pg_class.h"
#endif

#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif

#if PG_VERSION_NUM < 90500
#error Minimum version of PostgreSQL required is 9.5
#endif

#define CATALOG_GLOBAL_TEMP_REL	"pgtt_global_temp"
#define Anum_pgtt_relid   1
#define Anum_pgtt_viewid  2
#define Anum_pgtt_datcrea 3

PG_MODULE_MAGIC;

PGDLLEXPORT Datum   get_session_id(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_session_id);
PGDLLEXPORT Datum   generate_lsid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(generate_lsid);

PGDLLEXPORT Datum   get_session_start_time(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_session_start_time);

PGDLLEXPORT Datum   get_session_pid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(get_session_pid);

PG_FUNCTION_INFO_V1(lsid_in);
PG_FUNCTION_INFO_V1(lsid_out);
PG_FUNCTION_INFO_V1(lsid_recv);
PG_FUNCTION_INFO_V1(lsid_send);

typedef struct Lsid {
    int      backend_start_time;
    int      backend_pid;
} Lsid;

/* Default schema where GTT objects are saved */
#define PGTT_NSPNAME "pgtt_schema"

/* Define ProcessUtility hook proto/parameters following the PostgreSQL version */
#if PG_VERSION_NUM >= 140000
#define GTT_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
					bool readOnlyTree, \
                                        ProcessUtilityContext context, ParamListInfo params, \
                                        QueryEnvironment *queryEnv, DestReceiver *dest, \
                                        QueryCompletion *qc
#define GTT_PROCESSUTILITY_ARGS pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc
#else
#if PG_VERSION_NUM >= 130000
#define GTT_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
                                        ProcessUtilityContext context, ParamListInfo params, \
                                        QueryEnvironment *queryEnv, DestReceiver *dest, \
                                        QueryCompletion *qc
#define GTT_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, qc
#else
#if PG_VERSION_NUM >= 100000
#define GTT_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
                                        ProcessUtilityContext context, ParamListInfo params, \
                                        QueryEnvironment *queryEnv, DestReceiver *dest, \
                                        char *completionTag
#define GTT_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, completionTag
#elif PG_VERSION_NUM >= 90300
#define GTT_PROCESSUTILITY_PROTO Node *parsetree, const char *queryString, \
                                        ProcessUtilityContext context, ParamListInfo params, \
                                        DestReceiver *dest, char *completionTag
#define GTT_PROCESSUTILITY_ARGS parsetree, queryString, context, params, dest, completionTag
#else
#define GTT_PROCESSUTILITY_PROTO Node *parsetree, const char *queryString, \
                                        ParamListInfo params, bool isTopLevel, \
                                        DestReceiver *dest, char *completionTag
#define GTT_PROCESSUTILITY_ARGS parsetree, queryString, params, isTopLevel, dest, completionTag
#endif
#endif
#endif

/* Saved hook values in case of unload */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
/* Hook to intercept CREATE GLOBAL TEMPORARY TABLE query */
static void gtt_ProcessUtility(GTT_PROCESSUTILITY_PROTO);
static bool gtt_check_command(GTT_PROCESSUTILITY_PROTO);

/* Function declarations */

void	_PG_init(void);
void	_PG_fini(void);

int strpos(char *hay, char *needle, int offset);
static void gtt_override_create_table(GTT_PROCESSUTILITY_PROTO);
static void gtt_drop_table_statement(Oid relid, const char *relname);
static int search_relation(char *relname);
static void gtt_unregister_global_temporary_table(Oid relid, const char *relname);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* do not execute anything in parallel processes */
	if (ParallelWorkerNumber >= 0)
		return;

	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the gtt_* functions to be created even when the module isn't active.
	 * The functions must protect themselves against being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
 	 * Define (or redefine) custom GUC variables.
	 * No custom GUC variable at this time
	 */

	/*
	 * Install hooks.
	 */

	/* Disable hook for the moment */
	/*
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = gtt_ProcessUtility;
	*/
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	/*
	ProcessUtility_hook = prev_ProcessUtility;
	*/
}

static void
gtt_ProcessUtility(GTT_PROCESSUTILITY_PROTO)
{
	/* only in the top process */
	if (ParallelWorkerNumber == -1)
	{
		/*
		 * Check if we have a CREATE GLOBAL TEMPORARY TABLE, in
		 * this case do more work than the simple table creation
		 */
		(void) gtt_check_command(GTT_PROCESSUTILITY_ARGS);
	}

	elog(DEBUG1, "GTT DEBUG: restore ProcessUtility");

	/* Excecute the utility command, we are not concerned */
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(GTT_PROCESSUTILITY_ARGS);
		else
			standard_ProcessUtility(GTT_PROCESSUTILITY_ARGS);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Look at utility command
 */
static bool
gtt_check_command(GTT_PROCESSUTILITY_PROTO)
{
	bool	preserved = true;
	bool    work_done = false;
	Oid     schemaOid;
	char	*name = NULL;
#if PG_VERSION_NUM >= 100000
	Node    *parsetree = pstmt->utilityStmt;
#endif

	Assert(queryString != NULL);
	Assert(parsetree != NULL);

	elog(DEBUG1, "GTT DEBUG: processUtility query %s", queryString);

	/*
	 * Check that the pgtt extension is available in this database by looking
	 * for the pgtt schema otherwise get out of here we have nothing to do.
	 */
	schemaOid = get_namespace_oid(PGTT_NSPNAME, true);
	if (!OidIsValid(schemaOid))
		return work_done;

	switch (nodeTag(parsetree))
	{
		case T_CreateStmt:
		{
			/* CREATE TABLE statement */
			CreateStmt *stmt = (CreateStmt *)parsetree;
			name = stmt->relation->relname;

			/*
			 * Be sure to have GLOBAL TEMPORARY definition but RELPERSISTENCE_UNLOGGED
			 * might be more appropriate. Actually parser should translate GLOBAL TEMPORARY
			 * into persistant UNLOGGED table.
			 */
			if (stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
				break;
			/* 
			 * This condition must be replaced by a remove of GLOBAL keyword deprecation.
			 */
			if (strstr(asc_toupper(queryString, strlen(queryString)), "GLOBAL") == NULL ||
					strpos(asc_toupper(queryString, strlen(queryString)), "GLOBAL", 0) != 7)
				break;

			/*
			 * What to do at commit time for global temporary relations
			 * default is ON COMMIT PRESERVE ROWS (do nothing)
			 */
			if (stmt->oncommit == ONCOMMIT_DELETE_ROWS)
				preserved = false;

			/* 
			 * Case of ON COMMIT DROP and GLOBAL TEMPORARY might not be
			 * allowed, this is the same as using a normal temporary table
			 * inside a transaction. Here the table should be dropped after
			 * commit so it will not survive a transaction.
			 * Throw an error in this case.
			 */
			if (stmt->oncommit == ONCOMMIT_DROP)
				ereport(ERROR,
						(errmsg("use of ON COMMIT DROP with GLOBAL TEMPORARY is not allowed"),
						 errhint("Create a local temporary table inside a transaction instead, this is the default behavior.")));

	elog(DEBUG1, "GTT DEBUG: Create table %s, has rows persistance: %d, global at position: %d", name, preserved, strpos(asc_toupper(queryString, strlen(queryString)), "GLOBAL", 0));

			/* Create the Global Temporary Table with all associated object */
			gtt_override_create_table(GTT_PROCESSUTILITY_ARGS);

			work_done = true;
			break;
		}

		case T_DropStmt:
		{
			if (((DropStmt *)parsetree)->removeType == OBJECT_TABLE ||
				((DropStmt *)parsetree)->removeType == OBJECT_VIEW)
			{
				List *relationNameList = NULL;
				int relationNameListLength = 0;
#if PG_VERSION_NUM < 150000
				Value *relationSchemaNameValue = NULL;
				Value *relationNameValue = NULL;
#else
				String *relationSchemaNameValue = NULL;
				String *relationNameValue = NULL;
#endif
				int  mainTableOid = 0;
				char tbname[NAMEDATALEN];

				relationNameList = (List *) linitial(((DropStmt *)parsetree)->objects);
				relationNameListLength = list_length(relationNameList);

				switch (relationNameListLength)
				{
					case 1:
					{
						relationNameValue = linitial(relationNameList);
						break;
					}

					case 2:
					{
						relationSchemaNameValue = linitial(relationNameList);
						relationNameValue = lsecond(relationNameList);
						break;
					}

					case 3:
					{
						relationSchemaNameValue = lsecond(relationNameList);
						relationNameValue = lthird(relationNameList);
						break;
					}

					default:
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("improper relation name: \"%s\"",
											   NameListToString(relationNameList))));
						break;
					}
				}

				/* prefix with schema name if it is not added already */
				if (relationSchemaNameValue == NULL)
				{
#if PG_VERSION_NUM < 150000
					Value *schemaNameValue = makeString(pstrdup(PGTT_NSPNAME));
#else
					String *schemaNameValue = makeString(pstrdup(PGTT_NSPNAME));
#endif
					relationNameList = lcons(schemaNameValue, relationNameList);
				}

				if (strpos(
#if PG_VERSION_NUM < 150000
				asc_toupper(relationNameValue->val.str,  strlen(relationNameValue->val.str)),
#else
				asc_toupper(relationNameValue->sval,  strlen(relationNameValue->sval)),
#endif
						       	asc_toupper("pgtt_", 5), 0) == 0)
					break;

	elog(DEBUG1, "GTT DEBUG: looking for drop of tablename: %s",
#if PG_VERSION_NUM < 150000
			relationNameValue->val.str
#else
			relationNameValue->sval
#endif
	    );

				/* Truncate relname to appropriate length */
				strncpy(tbname, psprintf("pgtt_%s",
#if PG_VERSION_NUM < 150000
							relationNameValue->val.str
#else
							relationNameValue->sval
#endif
							), NAMEDATALEN);

				/*
				 * Look if we have a GTT table called pgtt_||name
				 * otherwise we have nothing more to do here.
				 */
				mainTableOid = search_relation(tbname);
elog(DEBUG1, "GTT DEBUG: mainTableOid = %d - tablename: %s", mainTableOid, tbname);

				if (mainTableOid > 0)
				{

					/* 
					 * FIXME: we do not allow multiple target in drop statement
					 * for GTT. See gtt_drop_table_statement() on how to build
					 * the new list of object.
					 */
					/*
					if (list_length(((DropStmt *)parsetree)->objects) != 1)
						ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
									errmsg("drop of Global Temporary Table do not support multiple target"),
									errhint("use individual drop statements.")));
					*/

					/*
					 * Looking if there is a corresponding global temporary table
					 * and in this case perform a "DROP TABLE pgtt_... CASCADE" instead.
					 */
					gtt_drop_table_statement(mainTableOid, psprintf("%s.%s", PGTT_NSPNAME, tbname));

					/*
					 * Unregister the Global Temporary Table and its link to the
					 * view stored in pgtt_global_temp table
					 */
					gtt_unregister_global_temporary_table(mainTableOid, tbname);

					/* reports that the work is already done */
					work_done = true;
					break;
				}
				else
				{
					/* 
					 * FIXME: See RemoveRelations(DropStmt *drop);
					 * src/backend/commands/tablecmds.c line 1070
					 */
				}
			}
			break;
		}
		default:
			break;
	}

	return work_done;
}

int
strpos(char *hay, char *needle, int offset)
{
	char *haystack;
	char *p;

	haystack = (char *) malloc(strlen(hay));
	if (haystack == NULL)
	{
		fprintf(stderr, _("out of memory\n"));
		exit(EXIT_FAILURE);
		return -1;
	}
	memset(haystack, 0, strlen(hay));

	strncpy(haystack, hay+offset, strlen(hay)-offset);
	p = strstr(haystack, needle);
	if (p)
		return p - haystack+offset;

	return -1;
}

/*
 * Create the Global Temporary Table with all associated objects just
 * like PLPGSQL function pgtt_create_table().
 *
 * The processus followed by the extension to emulate the Oracle-style
 * Global Temporary Table is the following:
 *
 * 1) create an unlogged table of the same name but prefixed with 'pgtt_'
 *    with the "hidden" column for a GTT (pgtt_sessid).
 *    The table is stored in extension schema pgtt_schema and the users
 *    must not access to this table directly. They have to use the view
 *    instead. The pgtt_sessid column has default to pg_backend_pid().
 * 2) grant SELECT,INSERT,UPDATE,DELETE on the table to PUBLIC.
 * 3) activate RLS on the table and create two RLS policies to hide rows
 *    to other sessions or transactions when required.
 * 4) force RLS to be active for the owner of the table.
 * 5) create an updatable view using the original table name with a
 *    a WHERE clause to hide the "hidden" column of the table.
 * 6) set owner of the view to current_user which might be a superuser,
 *    grants to other users are the responsability of the administrator.
 * 7) insert the relation between the gtt and the view in the catalog
 *    table pgtt_global_temp.
 *
 * The gtt_bgworker is responsible to remove all rows that are no more
 * visible to any session or transaction.
 */
static void
gtt_override_create_table(GTT_PROCESSUTILITY_PROTO)
{
	bool    need_priv_escalation = !superuser(); /* we might be a SU */
	Oid     save_userid;
	int     save_sec_context;
	char    tbname[NAMEDATALEN];
	int     pos;
	char    *newQueryString = NULL;
	bool	preserved = true;
	ListCell   *elements;
	char       *colnames = NULL;
	int connected = 0;
	int finished = 0;
	int result = 0;
	Oid     oidRel;
	Oid     oidView;
        bool            isnull;
#if PG_VERSION_NUM >= 100000
	Node       *parsetree = pstmt->utilityStmt;
#endif
	/*The CREATE TABLE statement */
	CreateStmt *stmt = (CreateStmt *)parsetree;

        /* Compute a string with the list of column names */
        foreach(elements, stmt->tableElts)
        {
                Node       *element = lfirst(elements);

                switch (nodeTag(element))
                {
                        case T_ColumnDef:
				if (colnames == NULL)
					colnames = ((ColumnDef *) element)->colname;
				else
					colnames = psprintf("%s,%s", colnames, ((ColumnDef *) element)->colname);
                                break;
			default:
                                break;
		}
	}

	elog(DEBUG1, "GTT DEBUG: Execute CREATE TABLE + RLS + VIEW grant");

	/* The Global Temporary Table objects must be created as SU */
	if (need_priv_escalation)
	{
		/* Get current user's Oid and security context */
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		/* Become superuser */
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, save_sec_context
							| SECURITY_LOCAL_USERID_CHANGE
							| SECURITY_RESTRICTED_OPERATION);
	}

	/*
	 * What to do at commit time for global temporary relations
	 * default is ON COMMIT PRESERVE ROWS (do nothing)
	 * FIXME: at this time it is not possible to used ON COMMIT
	 * syntax on non temporary table, it throw an error.
	 */
	if (stmt->oncommit == ONCOMMIT_DELETE_ROWS) 
		preserved = false;

	/* stmt->oncommit = ONCOMMIT_NOOP */

	/* Connect to the current database */
	connected = SPI_connect();
	if (connected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	/* Change relation name and truncate relname to appropriate length */
	strncpy(tbname, psprintf("pgtt_%s", stmt->relation->relname), NAMEDATALEN);

	/* Set DDL to create the unlogged table */
	pos = strpos(asc_toupper(queryString, strlen(queryString)), asc_toupper(stmt->relation->relname, strlen(stmt->relation->relname)), 0) + strlen(stmt->relation->relname);
	newQueryString = psprintf("CREATE UNLOGGED TABLE pgtt_schema.%s %s", tbname, queryString+pos);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Add pgtt_sessid column */
	newQueryString = psprintf("ALTER TABLE pgtt_schema.%s ADD COLUMN pgtt_sessid lsid DEFAULT get_session_id()", tbname);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Create an index on pgtt_sessid column */
	newQueryString = psprintf("CREATE INDEX ON pgtt_schema.%s (pgtt_sessid)", tbname);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Allow all on the global temporary table except truncate and drop to everyone */
	newQueryString = psprintf("GRANT SELECT,INSERT,UPDATE,DELETE ON pgtt_schema.%s TO PUBLIC", tbname);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Activate RLS to set policy based on session */
	newQueryString = psprintf("ALTER TABLE pgtt_schema.%s ENABLE ROW LEVEL SECURITY", tbname);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	if (preserved)
		/*
		 * Create the policy that must be applied on the table
		 * to show only rows where pgtt_sessid is the same as
		 * current pid.
		 */
		newQueryString = psprintf("CREATE POLICY pgtt_rls_session ON pgtt_schema.%s USING (pgtt_sessid = get_session_id()) WITH CHECK (true)", tbname);
	else
		/*
		 * Create the policy that must be applied on the table
		 * to show only rows where pgtt_sessid is the same as
		 * current pid and rows that have been created in the
		 * current transaction.
		 */
		newQueryString = psprintf("CREATE POLICY pgtt_rls_transaction ON pgtt_schema.%s USING (pgtt_sessid = get_session_id() AND xmin::text = txid_current()::text) WITH CHECK (true)", tbname);

        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Force policy to be active for the owner of the table */
	newQueryString = psprintf("ALTER TABLE pgtt_schema.%s FORCE ROW LEVEL SECURITY", tbname);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Create the view */
	if (preserved)
		newQueryString = psprintf("CREATE VIEW %s WITH (security_barrier) AS SELECT %s from pgtt_schema.%s WHERE pgtt_sessid=get_session_id()", stmt->relation->relname, colnames, tbname);
	else
		newQueryString = psprintf("CREATE VIEW %s WITH (security_barrier) AS SELECT %s from pgtt_schema.%s WHERE pgtt_sessid=get_session_id() AND xmin::text = txid_current()::text", stmt->relation->relname, colnames, tbname);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));
	
	/* Set owner of the view to current user, not the function definer (superuser)*/
	newQueryString = psprintf("ALTER VIEW %s OWNER TO %s", stmt->relation->relname, GetUserNameFromId(GetSessionUserId(), false));
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Get OID of the GTT table */
	newQueryString = psprintf("SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = '%s' AND n.nspname = 'pgtt_schema'", tbname);
        result = SPI_exec(newQueryString, 0);
        if (result != SPI_OK_SELECT)

                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));
        if (SPI_processed != 1)
                ereport(ERROR, (errmsg("query must return a single Oid: \"%s\"", newQueryString)));

        oidRel = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
								   SPI_tuptable->tupdesc,
								   1, &isnull));
	if (isnull)
                ereport(ERROR, (errmsg("query must not return NULL: \"%s\"", newQueryString)));

	/* Get OID of the corresponding view */
	newQueryString = psprintf("SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE c.relname = '%s' AND n.nspname = current_schema", stmt->relation->relname);
        result = SPI_exec(newQueryString, 0);
        if (result != SPI_OK_SELECT)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));
        if (SPI_processed != 1)
                ereport(ERROR, (errmsg("query must return a single Oid: \"%s\"", newQueryString)));

        oidView = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
								   SPI_tuptable->tupdesc,
								   1, &isnull));
	if (isnull)
                ereport(ERROR, (errmsg("query must not return NULL: \"%s\"", newQueryString)));

	/* Register the link between the view and the unlogged table */
	newQueryString = psprintf("INSERT INTO pgtt_schema.pgtt_global_temp (relid, viewid, datcrea, preserved) VALUES (%d, %d, now(), '%d')", oidRel, oidView, preserved);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	finished = SPI_finish();
	if (finished != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not disconnect from SPI manager")));
	}

	/* Restore user's privileges */
	if (need_priv_escalation)
		SetUserIdAndSecContext(save_userid, save_sec_context);

}

/*
 * Drop the Global Temporary Table with all associated objects
 * by replacing the DROP TABLE into a DROP TABLE statement with
 * CASCADE of the table prefixed with pgtt_.
 *
 * src/include/nodes/parsenodes.h : 2579
 */
static void
gtt_drop_table_statement(Oid relid, const char *relname)
{
	bool    need_priv_escalation = !superuser(); /* we might be a SU */
	Oid     save_userid;
	int     save_sec_context;
	char    *newQueryString = NULL;
	int     connected = 0;
	int     finished = 0;
	int     result = 0;

	/* The Global Temporary Table objects must be created as SU */
	if (need_priv_escalation)
	{
		/* Get current user's Oid and security context */
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		/* Become superuser */
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, save_sec_context
							| SECURITY_LOCAL_USERID_CHANGE
							| SECURITY_RESTRICTED_OPERATION);
	}

	connected = SPI_connect();
	if (connected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	/* Perform a drop cascade of the GTT table this will remove the associated view */
	newQueryString = psprintf("DROP TABLE %s CASCADE", relname);
	elog(DEBUG1, "GTT DEBUG: Executing DROP TABLE %s CASCADE; (Oid %d)", relname, relid);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	/* Now unregister the GTT table */
	newQueryString = psprintf("DELETE FROM pgtt_schema.pgtt_global_temp WHERE relid = %d", relid);
        result = SPI_exec(newQueryString, 0);
        if (result < 0)
                ereport(ERROR, (errmsg("execution failure on query: \"%s\"", newQueryString)));

	finished = SPI_finish();
	if (finished != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not disconnect from SPI manager")));
	}

	/* Restore user's privileges */
	if (need_priv_escalation)
		SetUserIdAndSecContext(save_userid, save_sec_context);

}

static int
search_relation(char *relname)
{
	ScanKeyData   key[2];
	SysScanDesc   scan;
	Relation      pg_class_rel;
	HeapTuple     tuple;
	int           foundOid = 0;
	Oid           schemaOid;

	/* Get Oid of the Global Temporary Tables */
	schemaOid = get_namespace_oid(PGTT_NSPNAME, false);

	elog(DEBUG1, "GTT DEBUG: Found oid for schema %s, oid %d", PGTT_NSPNAME, schemaOid);

	/* Define scanning */
	ScanKeyInit(&key[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(relname));
	ScanKeyInit(&key[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(schemaOid));
	/* Open catalog's relations */
#if (PG_VERSION_NUM >= 120000)
	pg_class_rel = table_open(RelationRelationId, RowExclusiveLock);
#else
	pg_class_rel = heap_open(RelationRelationId, RowExclusiveLock);
#endif
	/* Start search of relation */
	scan = systable_beginscan(pg_class_rel, ClassNameNspIndexId, true, NULL, lengthof(key), key);
	if (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
#if (PG_VERSION_NUM >= 120000)
		foundOid = ((Form_pg_class) GETSTRUCT(tuple))->oid;
#else
		foundOid = HeapTupleGetOid(tuple);
#endif
	}
	/* Cleanup. */
	systable_endscan(scan);
#if (PG_VERSION_NUM >= 120000)
	table_close(pg_class_rel, RowExclusiveLock);
#else
	heap_close(pg_class_rel, RowExclusiveLock);
#endif

	if (foundOid > 0)
		elog(DEBUG1, "GTT DEBUG: Found oid for relation %s from pg_class, oid %d", relname, foundOid);

	return foundOid;
}

/*
 * Unregister a Global Temporary Table and its link to the
 * view stored in pgtt_global_temp table.
 */
static void
gtt_unregister_global_temporary_table(Oid relid, const char *relname)
{
	RangeVar     *rv;
	Relation      rel;
	ScanKeyData   key[1];
	SysScanDesc   scan;
	HeapTuple     tuple;

	elog(DEBUG1, "GTT DEBUG: removing tuple with relid = %d (relname: %s)", relid, relname);

	/* Set and open the GTT relation */
	rv = makeRangeVar(PGTT_NSPNAME, CATALOG_GLOBAL_TEMP_REL, -1);
#if (PG_VERSION_NUM >= 120000)
	rel = table_openrv(rv, RowExclusiveLock);
#else
	rel = heap_openrv(rv, RowExclusiveLock);
#endif
	/* Define scanning */
	ScanKeyInit(&key[0], Anum_pgtt_relid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

	/* Start search of relation */
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	/* Remove the tuples. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
				simple_heap_delete(rel, &tuple->t_self);
	/* Cleanup. */
	systable_endscan(scan);
#if (PG_VERSION_NUM >= 120000)
	table_close(rel, RowExclusiveLock);
#else
	heap_close(rel, RowExclusiveLock);
#endif

}

/*
 * Function used to generate a local session id composed
 * with the timestamp (epoch) and the pid of the current
 * backend.
 */ 
Datum
get_session_id(PG_FUNCTION_ARGS)
{
	Lsid       *res;

	res = (Lsid *) palloc(sizeof(Lsid));
	res->backend_start_time = (int) MyStartTime;
	res->backend_pid = MyProcPid;

	PG_RETURN_POINTER(res);
}

Datum
lsid_in(PG_FUNCTION_ARGS)
{
	char       *str = PG_GETARG_CSTRING(0);
	int        backend_start_time,
		   backend_pid;
	Lsid       *res;

	if (sscanf(str, "{ %d, %d }", &backend_start_time, &backend_pid) != 2)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for local session id: \"%s\"",
				str)));

	res = (Lsid *) palloc(sizeof(Lsid));
	res->backend_start_time = backend_start_time;
	res->backend_pid = backend_pid;

	PG_RETURN_POINTER(res);
}

Datum
lsid_out(PG_FUNCTION_ARGS)
{
	Lsid    *lsid = (Lsid *) PG_GETARG_POINTER(0);
	char       *res;

	res = psprintf("{%d,%d}", lsid->backend_start_time, lsid->backend_pid);

	PG_RETURN_CSTRING(res);
}

Datum
lsid_recv(PG_FUNCTION_ARGS)
{
    StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
    Lsid    *res;

    res = (Lsid *) palloc(sizeof(Lsid));
    res->backend_start_time = pq_getmsgint(buf, sizeof(int32));
    res->backend_pid = pq_getmsgint(buf, sizeof(int32));

    PG_RETURN_POINTER(res);
}

Datum
lsid_send(PG_FUNCTION_ARGS)
{
    Lsid    *lsid = (Lsid *) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint(&buf, lsid->backend_start_time, 4);
    pq_sendint(&buf, lsid->backend_pid, 4);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Functions used to extract the backend start time
 * and backend pid from lsid type.
 */ 
Datum
get_session_start_time(PG_FUNCTION_ARGS)
{
    Lsid    *lsid = (Lsid *) PG_GETARG_POINTER(0);

    PG_RETURN_INT32(lsid->backend_start_time);
}

Datum
get_session_pid(PG_FUNCTION_ARGS)
{
    Lsid    *lsid = (Lsid *) PG_GETARG_POINTER(0);

    PG_RETURN_INT32(lsid->backend_pid);
}

/*
 * Given two integers representing the number of seconds since epoch of
 * the backend start time and the pid number of the backenreturns a lsid
 */
Datum
generate_lsid(PG_FUNCTION_ARGS)
{
	Lsid       *res;
	int        st  = Int32GetDatum((int32) PG_GETARG_INT32(0));
	int        pid = Int32GetDatum((int32) PG_GETARG_INT32(1));

	Assert(st);
	Assert(pid);

	if (st <= 0 || pid <= 0)
		ereport(ERROR,
			( errmsg("one of the argument is null, this is not supported")));

	res = (Lsid *) palloc(sizeof(Lsid));
	res->backend_start_time = st;
	res->backend_pid = pid;

	PG_RETURN_POINTER(res);
}


/*
 * Operator class for defining B-tree index
 */
static int
lsid_cmp_internal(Lsid * a, Lsid * b)
{
        if (a->backend_start_time < b->backend_start_time)
                return -1;
        if (a->backend_start_time > b->backend_start_time)
                return 1;
	/*
	 * a->backend_start_time = b->backend_start_time
	 * so continue the comparison on pid number
	 */
	if (a->backend_pid < b->backend_pid)
                return -1;
	if (a->backend_pid > b->backend_pid)
                return 1;

        return 0;
}

PG_FUNCTION_INFO_V1(lsid_lt);

Datum
lsid_lt(PG_FUNCTION_ARGS)
{
        Lsid    *a = (Lsid *) PG_GETARG_POINTER(0);
        Lsid    *b = (Lsid *) PG_GETARG_POINTER(1);

        PG_RETURN_BOOL(lsid_cmp_internal(a, b) < 0);
}

PG_FUNCTION_INFO_V1(lsid_le);

Datum
lsid_le(PG_FUNCTION_ARGS)
{
        Lsid    *a = (Lsid *) PG_GETARG_POINTER(0);
        Lsid    *b = (Lsid *) PG_GETARG_POINTER(1);

        PG_RETURN_BOOL(lsid_cmp_internal(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(lsid_eq);

Datum
lsid_eq(PG_FUNCTION_ARGS)
{
        Lsid    *a = (Lsid *) PG_GETARG_POINTER(0);
        Lsid    *b = (Lsid *) PG_GETARG_POINTER(1);

        PG_RETURN_BOOL(lsid_cmp_internal(a, b) == 0);
}

PG_FUNCTION_INFO_V1(lsid_ge);

Datum
lsid_ge(PG_FUNCTION_ARGS)
{
        Lsid    *a = (Lsid *) PG_GETARG_POINTER(0);
        Lsid    *b = (Lsid *) PG_GETARG_POINTER(1);

        PG_RETURN_BOOL(lsid_cmp_internal(a, b) >= 0);
}

PG_FUNCTION_INFO_V1(lsid_gt);

Datum
lsid_gt(PG_FUNCTION_ARGS)
{
        Lsid    *a = (Lsid *) PG_GETARG_POINTER(0);
        Lsid    *b = (Lsid *) PG_GETARG_POINTER(1);

        PG_RETURN_BOOL(lsid_cmp_internal(a, b) > 0);
}

PG_FUNCTION_INFO_V1(lsid_cmp);

Datum
lsid_cmp(PG_FUNCTION_ARGS)
{
        Lsid    *a = (Lsid *) PG_GETARG_POINTER(0);
        Lsid    *b = (Lsid *) PG_GETARG_POINTER(1);

        PG_RETURN_INT32(lsid_cmp_internal(a, b));
}

