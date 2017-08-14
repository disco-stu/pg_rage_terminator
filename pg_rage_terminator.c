/*-------------------------------------------------------------------------
 *
 * pg_rage_terminator.c
 *		Kills random connections of a Postgres server.
 *
 * Copyright (c) 2015, Adrian Vondendriesch
 *
 * IDENTIFICATION
 *		pg_rage_terminator/pg_rage_terminator.c
 *
 *-------------------------------------------------------------------------
 */

/* Some general headers for custom bgworker facility */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

/* Entry point of library loading */
void _PG_init(void);

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* GUC variables */
static int chance = 10;
static int interval = 5;

/* Worker name */
static char *worker_name = "pg_rage_terminator";

/*
 * Forward declaration for main routine. Makes compiler
 * happy (-Wunused-function, __attribute__((noreturn)))
 */
void pg_rage_terminator_main(Datum main_arg) pg_attribute_noreturn();

static void
pg_rage_terminator_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

static void
pg_rage_terminator_sighup(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

static void
pg_rage_terminator_build_query(StringInfoData *buf)
{
    appendStringInfo(buf, "SELECT "
               "pid, pg_terminate_backend(pid) as status, "
               "usename, datname, client_addr::text "
               "FROM pg_stat_activity "
               "WHERE client_port IS NOT NULL "
               "AND ((random() * 100)::int < %d) ",
                     chance);
    elog(DEBUG1, "Kill query is: %s", buf->data);
}

void
pg_rage_terminator_main(Datum main_arg)
{
	StringInfoData buf;

	/* Register functions for SIGTERM/SIGHUP management */
	pqsignal(SIGHUP, pg_rage_terminator_sighup);
	pqsignal(SIGTERM, pg_rage_terminator_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to a database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	/* Build query for process */
	initStringInfo(&buf);
	pg_rage_terminator_build_query(&buf);

	while (!got_sigterm)
	{
		int rc = 0;
        int ret, i;
        int sleep_interval;

        if (0 == interval)
            sleep_interval = 10;
        else
            sleep_interval = interval;

        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       sleep_interval * 1000L
#if PG_VERSION_NUM >= 100000
					   , PG_WAIT_EXTENSION
#endif
			);
        ResetLatch(&MyProc->procLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* Process signals */
		if (got_sighup)
		{
			int old_chance;
			/* Save old value of kill chance */
			old_chance = chance;

			/* Process config file */
			ProcessConfigFile(PGC_SIGHUP);
			got_sighup = false;
			ereport(LOG, (errmsg("bgworker pg_rage_terminator signal: processed SIGHUP")));

			/* Rebuild query if necessary */
			if (old_chance != chance)
			{
				resetStringInfo(&buf);
				initStringInfo(&buf);
				pg_rage_terminator_build_query(&buf);
			}
		}

		if (got_sigterm)
		{
			/* Simply exit */
			ereport(LOG, (errmsg("bgworker pg_rage_terminator signal: processed SIGTERM")));
			proc_exit(0);
		}

        /*
         * If interval is 0 we should not do anything.
         * This has to be done after sighup and sigterm handling.
         */
        if (0 == interval)
        {
            elog(LOG, "Nothing to do, sleep zzzzZZZZ");
            continue;
        }


		/* Process idle connection kill */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING, buf.data);

		/* Statement start time */
		SetCurrentStatementStartTimestamp();

		/* Execute query */
		ret = SPI_execute(buf.data, false, 0);

		/* Some error handling */
		if (ret != SPI_OK_SELECT)
			elog(FATAL, "Error when trying to rage");

		/* Do some processing and log stuff disconnected */
		for (i = 0; i < SPI_processed; i++)
		{
			int32 pidValue;
			bool isnull;
			char *datname = NULL;
			char *usename = NULL;
			char *client_addr = NULL;

			/* Fetch values */
			pidValue = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
												   SPI_tuptable->tupdesc,
												   1, &isnull));
			usename = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													3, &isnull));
			datname = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													4, &isnull));
            client_addr = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
                                                        SPI_tuptable->tupdesc,
                                                        5, &isnull));

			/* Log what has been disconnected */
			elog(LOG, "Rage terminated connection with PID %d %s/%s/%s",
				 pidValue, datname ? datname : "none",
				 usename ? usename : "none",
				 client_addr ? client_addr : "none");
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	/* No problems, so clean exit */
	proc_exit(0);
}

static void
pg_rage_terminator_load_params(void)
{
	/*
	 * Kill backends with a chance of <chance>.
     * Look every <interval> seconds for new targets.
	 */
	DefineCustomIntVariable("pg_rage_terminator.chance",
                            "Chance to terminate a backend in Percent (aboslue).",
                            "Default of 10",
                            &chance,
                            10,
                            0,
                            100,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

	DefineCustomIntVariable("pg_rage_terminator.interval",
                            "Inteval in which pg_rager_terminator looks for new targets (s).",
                            "Default of 5",
                            &interval,
                            5,
                            -1,
                            3600,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);
}

/*
 * Entry point for worker loading
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* Add parameters */
	pg_rage_terminator_load_params();

	/* Worker parameter and registration */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/*
	 * bgw_main is considered a footgun, per commit
	 * 2113ac4cbb12 in postgresql.git. Deprecate its
	 * usage here and use the safer method by setting
	 * bgw_function_name and bgw_library_name. PG10 gets
	 * rid of bgw_main completly, but we need to retain
	 * it here to get the initialization correct.
	 */
#if PG_VERSION_NUM < 100000
	worker.bgw_main = NULL;
#endif

	snprintf(worker.bgw_library_name, BGW_MAXLEN - 1, "pg_rage_terminator");
	snprintf(worker.bgw_function_name, BGW_MAXLEN - 1, "pg_rage_terminator_main");

	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
	/* Wait 10 seconds for restart before crash */
	worker.bgw_restart_time = 10;
	worker.bgw_main_arg = (Datum) 0;
#if PG_VERSION_NUM >= 90400
	/*
	 * Notify PID is present since 9.4. If this is not initialized
	 * a static background worker cannot start properly.
	 */
	worker.bgw_notify_pid = 0;
#endif
	RegisterBackgroundWorker(&worker);
}
