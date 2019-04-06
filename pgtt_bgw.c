/*
 * pgtt_bgw.c
 *
 * A background worker process for the global_temporary_table extension to
 * allow maintenance of obsolete rows in pgtt_global_temp_register tables.
 * Run in all database with the extension to remove rows in the register
 * table that are no more visible by any session or any session+transaction.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the COPYING file.
 *
 * Copyright (c) 2018-2019 Gilles Darold
 */

#include "postgres.h"
#include "libpq-fe.h"

/* Necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* Used by this code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include "pgstat.h"
#include "utils/ps_status.h"
#include "catalog/pg_database.h"
#include "access/htup_details.h"
#include "utils/memutils.h"

#if (PG_VERSION_NUM >= 120000)
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/table.h"
#endif

#if (PG_VERSION_NUM >= 100000)
#include "utils/varlena.h"
#endif

/* Default database/user connection */
#define PGTT_START_DBNAME "postgres"
#define PGTT_BGW_USER "postgres"
/* PGTT extension schema */
#define PGTT_SCHEMA "pgtt_schema"

/* the minimum allowed time between two awakenings of the worker */
#define MIN_PGTT_SLEEPTIME 1    /* second */
#define MAX_PGTT_SLEEPTIME 60  /* seconds */

PG_MODULE_MAGIC;

void _PG_init(void);
void pgtt_bgw_main(Datum main_arg) ;
void pgtt_bgw_maintenance(Datum main_arg);

static List *get_database_list(void);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int pgtt_naptime = 5; // Default 5 seconds
static char *pgtt_analyze = "off";
static int pgtt_chunk_size = 250000; // Default delete by 250000 rows at a timeœ

/* Counter of iteration */
static int iteration = 0;
/*
 * Signal handler for SIGTERM
 *      Set a flag to let the main loop to terminate, and set our latch to wake
 *      it up.
 */
static void
pgtt_bgw_sigterm(SIGNAL_ARGS)
{
	int         save_errno = errno;

	got_sigterm = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *      Set a flag to tell the main loop to reread the config file, and set
 *      our latch to wake it up.
 */
static void
pgtt_bgw_sighup(SIGNAL_ARGS)
{
	int         save_errno = errno;

	got_sighup = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Entrypoint of this module.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	DefineCustomIntVariable("pgtt.naptime",
				"How often maintenance of Global Temporary Tables is called (in seconds).",
				NULL,
				&pgtt_naptime,
				5,
				MIN_PGTT_SLEEPTIME,
				MAX_PGTT_SLEEPTIME,
				PGC_SIGHUP,
				0,
				NULL,
				NULL,
				NULL);

	DefineCustomStringVariable("pgtt.analyze",
				 "Whether to force an analyze after a maintenance on a GTT. Possible values: 'off' (default) or 'on'. Default is to let autoanalyze do the job.",
				 NULL,
				 &pgtt_analyze,
				 "off",
				 PGC_SIGHUP,
				 0,
				 NULL,
				 NULL,
				 NULL);

	DefineCustomIntVariable("pgtt.chunk_size",
				"Maximum number of rows to delete in Global Temporary Tables.",
				NULL,
				&pgtt_chunk_size,
				250000,
				1000,
				INT_MAX,
				PGC_SIGHUP,
				0,
				NULL,
				NULL,
				NULL);


	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Start when database starts */
	sprintf(worker.bgw_name, "pgtt master background worker");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 600; /* Restart after 10min in case of crash */
#if (PG_VERSION_NUM < 100000)
	worker.bgw_main = pgtt_bgw_main;
#else
	sprintf(worker.bgw_library_name, "pgtt_bgw");
	sprintf(worker.bgw_function_name, "pgtt_bgw_main");
#endif
	worker.bgw_main_arg = (Datum) 0;
#if (PG_VERSION_NUM >= 90400)
	worker.bgw_notify_pid = 0;
#endif
	RegisterBackgroundWorker(&worker);

}

void
pgtt_bgw_main(Datum main_arg)
{
	int            worker_id = DatumGetInt32(main_arg); /* in case we start mulitple worker at startup */
	StringInfoData buf;

	ereport(DEBUG1,
			(errmsg("PGTT background worker started (#%d)", worker_id)));
	
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pgtt_bgw_sighup);
	pqsignal(SIGTERM, pgtt_bgw_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/*
	 * Connect to the postgres database, will be used to get
	 * a list of all database at each main loop iteration
	 */
	ereport(DEBUG1,
			(errmsg("Initialize connection to database %s", PGTT_START_DBNAME)));
#if (PG_VERSION_NUM >= 110000)
	BackgroundWorkerInitializeConnection(PGTT_START_DBNAME, NULL, 0);
#else
	BackgroundWorkerInitializeConnection(PGTT_START_DBNAME, NULL);
#endif

	initStringInfo(&buf);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		List           *dblist = NIL;
		int             rc;

		/* Using Latch loop method suggested in latch.h
		 * Uses timeout flag in WaitLatch() further below instead of sleep to allow clean shutdown */
		ResetLatch(&MyProc->procLatch);

		CHECK_FOR_INTERRUPTS();

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Get the list of database to scan */
		dblist = get_database_list();

		/* Now for each database run GTT maintenance in a dedicated dynamic worker */
		if (dblist != NIL)
		{
			ListCell                *item;
			BackgroundWorker        worker;
			BackgroundWorkerHandle  *handle;
			BgwHandleStatus         status;
			int                     full_string_length;
			pid_t                   pid;
			int                     i = 0;

			/* set up common data for all our workers */
			worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
			worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
			worker.bgw_restart_time = BGW_NEVER_RESTART;
#if (PG_VERSION_NUM < 100000)
			worker.bgw_main = NULL;
#endif
			sprintf(worker.bgw_library_name, "pgtt_bgw");
			sprintf(worker.bgw_function_name, "pgtt_bgw_maintenance");

			foreach(item, dblist)
			{
				char *dbname = (char *) lfirst(item);

				i++;

				ereport(DEBUG1,
						(errmsg("Dynamic launch of a background worker to clean GTT in database %s", dbname)));
				full_string_length = snprintf(worker.bgw_name, sizeof(worker.bgw_name),
						"pgtt_bgw dynamic bgworker [%s]", dbname);
				if (full_string_length >= sizeof(worker.bgw_name)) {
					memcpy(worker.bgw_name + sizeof(worker.bgw_name) - 4, "...)", 4);
				}
				worker.bgw_main_arg = (Datum) iteration;
				worker.bgw_notify_pid = MyProcPid;
				/* set the database to scan by the dynamic bgworker */
				memset(worker.bgw_extra, 0, BGW_EXTRALEN);
				memcpy(worker.bgw_extra, dbname, strlen(dbname));

				ereport(DEBUG1,
						(errmsg("Registering dynamic background worker...")));
				if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
					ereport(ERROR,
							(errmsg("Unable to register dynamic background worker for pgtt. Consider increasing max_worker_processes if you see this frequently. Main background worker process will try restarting in 10 minutes.")));
				}
				ereport(DEBUG1,
						(errmsg("Waiting for dynamic background worker startup...")));
				status = WaitForBackgroundWorkerStartup(handle, &pid);
				ereport(DEBUG1,
						(errmsg("Dynamic background worker startup status: %d", status)));
				if (status == BGWH_STOPPED) {
				    ereport(ERROR,
					    (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					     errmsg("Could not start dynamic pgtt background process"),
					   errhint("More details may be available in the server log.")));
				}

				if (status == BGWH_POSTMASTER_DIED) {
				    ereport(ERROR,
					    (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					  errmsg("Cannot start dynamic pgtt background processes without postmaster"),
					     errhint("Kill all remaining database processes and restart the database.")));
				}
				Assert(status == BGWH_STARTED);
#if (PG_VERSION_NUM >= 90500)
				/*
				 * Shutdown wait function introduced in 9.5. The latch problems this wait
                                 * fixes are only encountered in 9.6 and later. So this shouldn't be a
				 * problem for 9.4.
				 */
				ereport(DEBUG1,
						(errmsg("Waiting for dynamic pgtt background shutdown...")));
				status = WaitForBackgroundWorkerShutdown(handle);
				ereport(DEBUG1,
						(errmsg("Dynamic pgtt background shutdown status: %d", status)));
				Assert(status == BGWH_STOPPED);
#endif
			}
			list_free(dblist);
			iteration++;
		}
		ereport(DEBUG1,
				(errmsg("Latch status before waitlatch call: %d", MyProc->procLatch.is_set)));

#if (PG_VERSION_NUM >= 100000)
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   pgtt_naptime * 1000L,
					   PG_WAIT_EXTENSION);
#else
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   pgtt_naptime * 1000L);
#endif

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ereport(DEBUG1,
				(errmsg("Latch status after waitlatch call: %d", MyProc->procLatch.is_set)));
	} /* End of main loop */
}

/*
 * Function executed at each loop of the main process.
 * Remove obsolete rows from all Global Temporary Tables.
 */
void
pgtt_bgw_maintenance(Datum main_arg)
{

	char              *dbname;
	char              *analyze;
	StringInfoData	  buf;
	int               worker_iter;
	PGconn            *dbh;
	const char        *conninfo;
	PGresult          *result;

	/* Get the current iteration */
	worker_iter = DatumGetInt32(main_arg);

	/* Get database to scan for this worker_iter */
	dbname = MyBgworkerEntry->bgw_extra;
	Assert(dbname != NULL);

	ereport(DEBUG1,
			(errmsg("Entering pgtt dynamic bgworker to scan database %s", dbname)));

	pgstat_report_appname(psprintf("pgtt_bgw %s", dbname));

	/*
	 * We can not change connection database using SPI
	 * so we use libpq to connect to each database to
	 * proceed to obsolete rows cleaning.
	 */
	conninfo = psprintf("dbname=%s user=%s", dbname, PGTT_BGW_USER);

	ereport(DEBUG1,
			(errmsg("Establishing connection to '%s' using libpd", conninfo)));

	if (( dbh = PQconnectdb( conninfo )) == NULL )
		ereport(ERROR,
				 ( errmsg("could not connect allocate a connection to database '%s'", dbname)));
	if ( PQstatus(dbh) == CONNECTION_BAD )
		ereport(ERROR,
				 ( errmsg("could not connect to database %s using conninfo '%s', error: %s", dbname, conninfo, PQerrorMessage(dbh))));

	ereport(DEBUG1,
			(errmsg("Connected to database %s", dbname)));

	initStringInfo(&buf);

	/* First determine if pgtt is even installed in this database */
	appendStringInfo(&buf, "SELECT extname FROM pg_catalog.pg_extension WHERE extname = 'pgtt'");

	ereport(DEBUG1,
			(errmsg("Checking if pgtt extension is installed in database: %s", dbname)));

	pgstat_report_activity(STATE_RUNNING, buf.data);
	if (( result = PQexec(dbh, buf.data)) == NULL )
		ereport(ERROR,
				 ( errmsg("Cannot determine if pgtt is installed in database %s: error: %s",
					  dbname, PQerrorMessage( dbh ))));

	resetStringInfo(&buf);

	if (PQntuples( result ) != 1)
	{
		ereport(DEBUG1,
				(errmsg("pgtt not installed in database %s. Nothing to do exiting gracefully.", dbname)));
		/* Nothing left to do, go to end of function. */
	}
	else
	{
		PQclear( result );
		/* Look if we need to force an ANALYSE after the maintenance */
		if (strcmp(pgtt_analyze, "on") == 0) {
			analyze = "true";
		} else {
			analyze = "false";
		}

		if (worker_iter)
			ereport(DEBUG1,
					(errmsg("%s dynamic bgworker running maintenance task on database %s",
									MyBgworkerEntry->bgw_name, dbname)));
		else
			ereport(DEBUG1,
					(errmsg("%s dynamic bgworker truncating GTT tables on database %s",
									MyBgworkerEntry->bgw_name, dbname)));

		appendStringInfo(&buf, "SELECT \"%s\".pgtt_maintenance(%d, %d, %s) AS nrows", PGTT_SCHEMA, worker_iter, pgtt_chunk_size,  analyze);

		pgstat_report_activity(STATE_RUNNING, buf.data);

		if (( result = PQexec(dbh, buf.data)) == NULL )
		{
			PQclear( result );
			PQfinish(dbh);
			ereport(ERROR,
					 ( errmsg("Cannot call pgtt_maintenance() function in database %s: error: %s",
						  dbname, PQerrorMessage( dbh ))));
		}
		if (!PQgetisnull(result, 0, 0))
		{
			int           nrows;

			nrows = atoi(PQgetvalue(result, 0, 0));
			if (nrows > 0)
				ereport(LOG,
					(errmsg("pgtt dynamic background worker removed %d obsolete rows from database %s",
						nrows, dbname)));
		}
	}
	PQclear( result );


	PQfinish(dbh);
	pgstat_report_activity(STATE_IDLE, NULL);
	ereport(DEBUG1,
			(errmsg("pgtt dynamic bgworker shutting down gracefully for database %s.", dbname)));

}

/*
 * get_database_list
 *		Return a list of all databases found in pg_database.
 *
 * Note: this is the only function in which the GTT launcher uses a
 * transaction.  Although we aren't attached to any particular database and
 * therefore can't access most catalogs, we do have enough infrastructure
 * to do a seqscan on pg_database.
 */
static List *
get_database_list(void)
{
	List	   *dblist = NIL;
	Relation	rel;
#if (PG_VERSION_NUM >= 120000)
	TableScanDesc scan;
#else
	HeapScanDesc scan;
#endif
	HeapTuple	tup;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	/*
	 * Start a transaction so we can access pg_database, and get a snapshot.
	 * We don't have a use for the snapshot itself, but we're interested in
	 * the secondary effect that it sets RecentGlobalXmin.  (This is critical
	 * for anything that reads heap pages, because HOT may decide to prune
	 * them even if the process doesn't attempt to modify any tuples.)
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
#if (PG_VERSION_NUM >= 120000)
	scan = table_beginscan_catalog(rel, 0, NULL);
#else
	scan = heap_beginscan_catalog(rel, 0, NULL);
#endif

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		MemoryContext oldcxt;

		/* do not consider template or database that do not allow connection */
		if (pgdatabase->datistemplate || !pgdatabase->datallowconn)
			continue;

		/*
		 * Allocate our results in the caller's context, not the
		 * transaction's. We do this inside the loop, and restore the original
		 * context at the end, so that leaky things like heap_getnext() are
		 * not called in a potentially long-lived context.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);

		dblist = lappend(dblist, pstrdup(NameStr(pgdatabase->datname)));

		MemoryContextSwitchTo(oldcxt);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return dblist;
}

