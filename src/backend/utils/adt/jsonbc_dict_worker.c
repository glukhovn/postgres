/*
 * src/backend/utils/adt/jsonbc_dict_worker.c
 *
 *  Created on: 15.11.2016
 *      Author: Nikita Glukhov
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "lib/ilist.h"
#include "mb/pg_wchar.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/jsonbc_dict.h"
#include "utils/memutils.h"

#define JSONBC_MQ_BUF_SIZE 1024

typedef struct JsonbcDictWorker
{
	Oid					dbid;
	BackgroundWorkerHandle	*handle;
	bool				started;

	Latch		   		workerLatch;
	Latch			   	backendLatch;

	dlist_node			lruNode;

	struct
	{
		JsonbcDictId	dict;
		JsonbcKeyId		nextKeyId;
		shm_mq		   *keymq;
	} request;

	struct
	{
		JsonbcKeyId		id;
		shm_mq		   *errmq;
	} response;

	char				mqbuf[JSONBC_MQ_BUF_SIZE];
} JsonbcDictWorker;

typedef struct JsonbcDictWorkers
{
	slock_t				mutex;
	dlist_head			lruList;
	int					nworkers;
	JsonbcDictWorker	workers[FLEXIBLE_ARRAY_MEMBER];
} JsonbcDictWorkers;

int jsonbc_max_workers; /* GUC parameter */

static JsonbcDictWorker	   *jsonbcDictWorker;
static JsonbcDictWorkers   *jsonbcDictWorkers;
static bool 				jsonbcDictWorkerShutdownRequested = false;

#define PG_JSONBC_DICT_SHM_MAGIC 0x8a02fb3d

static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	jsonbcDictWorkerShutdownRequested = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	if (jsonbcDictWorker)
		SetLatch(&jsonbcDictWorker->workerLatch);

	errno = save_errno;
}

void
JsonbcDictWorkerShmemInit()
{
	bool		found;
	int			i;

	if (jsonbc_max_workers <= 0)
		return;

	jsonbcDictWorkers = (JsonbcDictWorkers *)
			ShmemInitStruct("jsonbc dictionary workers",
							offsetof(JsonbcDictWorkers, workers) +
								sizeof(JsonbcDictWorker) * jsonbc_max_workers,
							&found);

	if (found)
		return;

	dlist_init(&jsonbcDictWorkers->lruList);
	SpinLockInit(&jsonbcDictWorkers->mutex);
	jsonbcDictWorkers->nworkers = jsonbc_max_workers;

	memset(jsonbcDictWorkers->workers, 0,
		   sizeof(JsonbcDictWorker) * jsonbc_max_workers);

	for (i = 0; i < jsonbc_max_workers; i++)
	{
		JsonbcDictWorker	   *wrk = &jsonbcDictWorkers->workers[i];
		InitSharedLatch(&wrk->workerLatch);
		InitSharedLatch(&wrk->backendLatch);
	}
}

static char *
jsonbcDictWorkerReceiveString(shm_mq *mq, int *len)
{
	shm_mq_handle	   *mqh;
	char			   *str;
	void			   *data;
	Size				size;

	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, NULL, NULL);

	if (shm_mq_receive(mqh, &size, &data, false) != SHM_MQ_SUCCESS ||
		size != sizeof(*len))
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error reading key len from mq");
	}

	memcpy(len, data, size);

	if (shm_mq_receive(mqh, &size, &data, false) != SHM_MQ_SUCCESS ||
		size != *len)
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error reading key from mq");
	}

	str = memcpy(palloc(size), data, size);

	shm_mq_detach(mq);

	return str;
}

static void
jsonbcDictWorkerSendString(shm_mq *mq, const char *str, int len)
{
	shm_mq_handle	   *mqh;

	shm_mq_set_sender(mq, MyProc);
	mqh = shm_mq_attach(mq, NULL, NULL);

	if (shm_mq_wait_for_attach(mqh) != SHM_MQ_SUCCESS)
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error attaching to key mq");
	}

	if (shm_mq_send(mqh, sizeof(len), &len, false) != SHM_MQ_SUCCESS ||
		shm_mq_send(mqh, len, str, false) != SHM_MQ_SUCCESS)
	{
		shm_mq_detach(mq);
		elog(ERROR, "jsonbc: error writing to key mq");
	}

	shm_mq_detach(mq);
}

static void
jsonbcDictWorkerMain(Datum main_arg)
{
	JsonbcDictWorker	   *wrk = (JsonbcDictWorker	*) DatumGetPointer(main_arg);
	MemoryContext			mcxt;

	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "jsonbc dict wrk toplevel");
	mcxt = AllocSetContextCreate(TopMemoryContext,
								 "jsonbc dict wrk",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(mcxt);

	JsonbcDictWorkerShmemInit();

	BackgroundWorkerInitializeConnectionByOid(wrk->dbid, InvalidOid);

	SetClientEncoding(GetDatabaseEncoding());

	OwnLatch(&wrk->workerLatch);
	ResetLatch(&wrk->workerLatch);
	SetLatch(&wrk->backendLatch);

	jsonbcDictWorker = wrk;

	for (;;)
	{
		int		rc = WaitLatch(&wrk->workerLatch,
							   WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

		if (rc & WL_POSTMASTER_DEATH)
			exit(1);

		if (jsonbcDictWorkerShutdownRequested)
			break;

		ResetLatch(&wrk->workerLatch);

		PG_TRY();
		{
			JsonbcKeyName	key;

			key.s = jsonbcDictWorkerReceiveString(wrk->request.keymq, &key.len);


			StartTransactionCommand();

			wrk->response.id = jsonbcDictGetIdByNameSeqCached(wrk->request.dict,
															  key);

			if (wrk->response.id == JsonbcInvalidKeyId)
				wrk->response.id =
						jsonbcDictGetIdByNameSlow(wrk->request.dict,
												  key,
												  wrk->request.nextKeyId);

			CommitTransactionCommand();

			SetLatch(&wrk->backendLatch);
		}
		PG_CATCH();
		{
			ErrorData	   *edata;

			MemoryContextSwitchTo(mcxt);
			edata = CopyErrorData();
			FlushErrorState();
			wrk->response.id = JsonbcInvalidKeyId;
			wrk->response.errmq = shm_mq_create(wrk->mqbuf, sizeof(wrk->mqbuf));

			SetLatch(&wrk->backendLatch);

			jsonbcDictWorkerSendString(wrk->response.errmq,
									   edata->message, strlen(edata->message));

			FreeErrorData(edata);
		}
		PG_END_TRY();
	}

	DisownLatch(&wrk->workerLatch);

	proc_exit(0);
}

static void
jsonbcDictWorkerStart(JsonbcDictWorker *wrk)
{
	BackgroundWorker		bgworker;
	BackgroundWorkerHandle *bgwhandle;
	MemoryContext			oldcontext;
	BgwHandleStatus			status;
	pid_t					pid;
	int						rc;

	elog(DEBUG1, "starting jsonbc dictionary background worker for DB %d", wrk->dbid);

	/* We might be running in a short-lived memory context. */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	/* Configure a worker. */
	snprintf(bgworker.bgw_name, BGW_MAXLEN, "jsonbc dictionary worker");
	bgworker.bgw_flags = BGWORKER_SHMEM_ACCESS |
						 BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgworker.bgw_start_time = BgWorkerStart_ConsistentState;
	bgworker.bgw_restart_time = BGW_NEVER_RESTART;
	bgworker.bgw_main = jsonbcDictWorkerMain;
	bgworker.bgw_main_arg = PointerGetDatum(wrk);
	bgworker.bgw_notify_pid = MyProcPid;

	OwnLatch(&wrk->backendLatch);
	ResetLatch(&wrk->backendLatch);

	PG_TRY();
	{
		if (!RegisterDynamicBackgroundWorker(&bgworker, &bgwhandle))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not register jsonbc dictionary worker"),
					 errhint("Consider increasing the configuration parameter "
							 "\"max_worker_processes\"")));

		status = WaitForBackgroundWorkerStartup(bgwhandle, &pid);

		if (status == BGWH_STOPPED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not start background process"),
				   errhint("More details may be available in the server log.")));
		if (status == BGWH_POSTMASTER_DIED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				  errmsg("cannot start background processes without postmaster"),
					 errhint("Kill all remaining database processes and restart the database.")));
		Assert(status == BGWH_STARTED);

		GetSharedBackgroundWorkerHandle(bgwhandle, &wrk->handle);
		wrk->started = true;

		pfree(bgwhandle);

		rc = WaitLatch(&wrk->backendLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
	}
	PG_CATCH();
	{
		DisownLatch(&wrk->backendLatch);
		PG_RE_THROW();
	}
	PG_END_TRY();

	DisownLatch(&wrk->backendLatch);

	if (rc & WL_POSTMASTER_DEATH)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			  errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));

	MemoryContextSwitchTo(oldcontext);
}

static void
jsonbcDictWorkerStop(JsonbcDictWorker *wrk)
{
	BgwHandleStatus		bgwstatus;

	TerminateBackgroundWorker(wrk->handle);

#if 0 /* FIXME */
	SetBackgroundWorkerNotifyPid(wrk->handle, MyProcPid);

	bgwstatus = WaitForBackgroundWorkerShutdown(wrk->handle);

	if (bgwstatus == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));

	Assert(bgwstatus == BGWH_STOPPED);
#endif

	wrk->started = false;
}

typedef struct JsonbcDictWorkerHandle
{
	JsonbcDictWorker	   *worker;
	LOCKTAG					locktag;
} JsonbcDictWorkerHandle;

#define PG_JSONBC_DICT_LOCK_MAGIC 0x3C59A016

static void
jsonbcDictWorkerInitLockTag(LOCKTAG *tag, Oid dbid)
{
	tag->locktag_field1 = PG_JSONBC_DICT_LOCK_MAGIC;
	tag->locktag_field2 = (uint32) dbid;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
}

static JsonbcDictWorker *
jsonbcDictWorkerFind(Oid dbid)
{
	JsonbcDictWorker	   *result = NULL;
	JsonbcDictWorker	   *free = NULL;
	JsonbcDictWorker	   *worker;
	int						i;

	SpinLockAcquire(&jsonbcDictWorkers->mutex);

	for (i = 0, worker = jsonbcDictWorkers->workers;
		 i < jsonbcDictWorkers->nworkers;
		 i++, worker++)
	{
		if (worker->dbid == dbid)
		{
			result = worker;
			dlist_delete(&result->lruNode);
			dlist_push_tail(&jsonbcDictWorkers->lruList, &result->lruNode);
			break;
		}

		if (!worker->dbid && !free)
			free = worker;
	}

	if (!result && free)
	{
		result = free;
		result->dbid = dbid;
		dlist_push_tail(&jsonbcDictWorkers->lruList, &result->lruNode);
	}

	SpinLockRelease(&jsonbcDictWorkers->mutex);

	return result;
}

static JsonbcDictWorker *
jsonbcDictWorkerPeekVictim(Oid *dbid)
{
	JsonbcDictWorker   *victim = NULL;

	SpinLockAcquire(&jsonbcDictWorkers->mutex);
	if (!dlist_is_empty(&jsonbcDictWorkers->lruList))
	{
		victim = dlist_container(JsonbcDictWorker, lruNode,
								dlist_head_node(&jsonbcDictWorkers->lruList));
		*dbid = victim->dbid;
	}
	SpinLockRelease(&jsonbcDictWorkers->mutex);

	return victim;
}

static bool
jsonbcDictWorkerCheckVictim(JsonbcDictWorker *victim, Oid dbid)
{
	bool	result;

	SpinLockAcquire(&jsonbcDictWorkers->mutex);

	if (dlist_is_empty(&jsonbcDictWorkers->lruList))
		result = false;
	else
	{
		JsonbcDictWorker *lru = dlist_container(JsonbcDictWorker, lruNode,
						dlist_head_node(&jsonbcDictWorkers->lruList));
		result = victim == lru && lru->dbid == dbid;
	}

	SpinLockRelease(&jsonbcDictWorkers->mutex);

	return result;
}

static Oid
jsonbcDictWorkerGetDbId(JsonbcDictWorker *worker)
{
	Oid		dbid;

	SpinLockAcquire(&jsonbcDictWorkers->mutex);
	dbid = worker->dbid;
	SpinLockRelease(&jsonbcDictWorkers->mutex);

	return dbid;
}

static void
jsonbcDictWorkerReassignDbId(JsonbcDictWorker *worker, Oid dbid)
{
	SpinLockAcquire(&jsonbcDictWorkers->mutex);

	Assert(!worker->started);

	dlist_delete(&worker->lruNode);
	dlist_push_tail(&jsonbcDictWorkers->lruList, &worker->lruNode);
	worker->started = false;
	worker->dbid = dbid;

	SpinLockRelease(&jsonbcDictWorkers->mutex);
}

static JsonbcDictWorker *
jsonbcDictWorkerTryReuseOne(dbid)
{
	LOCKTAG				locktag;
	JsonbcDictWorker   *victim;
	Oid					victimdbid;

	if (!(victim = jsonbcDictWorkerPeekVictim(&victimdbid)))
		return NULL;

	jsonbcDictWorkerInitLockTag(&locktag, victimdbid);
	LockAcquire(&locktag, ExclusiveLock, false, false);

	if (jsonbcDictWorkerCheckVictim(victim, victimdbid))
	{
		jsonbcDictWorkerStop(victim);
		jsonbcDictWorkerReassignDbId(victim, dbid);
	}
	else
		victim = NULL;

	LockRelease(&locktag, ExclusiveLock, false);

	return victim;
}

static JsonbcDictWorkerHandle
jsonbcDictWorkerAcquire(Oid dbid)
{
	JsonbcDictWorkerHandle		handle;
	JsonbcDictWorker		   *worker;

	jsonbcDictWorkerInitLockTag(&handle.locktag, dbid);
	LockAcquire(&handle.locktag, ExclusiveLock, false, false);

	while (!(worker = jsonbcDictWorkerFind(dbid)))
	{
		LockRelease(&handle.locktag, ExclusiveLock, false);
		worker = jsonbcDictWorkerTryReuseOne(dbid);
		LockAcquire(&handle.locktag, ExclusiveLock, false, false);

		if (worker && jsonbcDictWorkerGetDbId(worker) == dbid)
			break;
	}

	handle.worker = worker;

	if (!worker->started)
		jsonbcDictWorkerStart(worker);

	return handle;
}

static void
jsonbcDictWorkerRelease(JsonbcDictWorkerHandle *handle)
{
	LockRelease(&handle->locktag, ExclusiveLock, false);
	handle->worker = NULL;
}

static JsonbcKeyId
jsonbcDictWorkerExecRequest(JsonbcDictWorker *wrk,
							JsonbcDictId dict,
							JsonbcKeyName key,
							JsonbcKeyId nextKeyId,
							char **errmsg,
							int *errmsglen)
{
	JsonbcKeyId		result;

	OwnLatch(&wrk->backendLatch);

	PG_TRY();
	{
		wrk->request.dict = dict;
		wrk->request.nextKeyId = nextKeyId;
		wrk->request.keymq = shm_mq_create(wrk->mqbuf, sizeof(wrk->mqbuf));

		ResetLatch(&wrk->backendLatch);
		SetLatch(&wrk->workerLatch);

		jsonbcDictWorkerSendString(wrk->request.keymq, key.s, key.len);

		(void) WaitLatch(&wrk->backendLatch, WL_LATCH_SET, 0);

		result = wrk->response.id;

		elog(DEBUG1, "received response from jsonbc worker: %d", result);

		if (result == JsonbcInvalidKeyId)
			*errmsg = jsonbcDictWorkerReceiveString(wrk->response.errmq,
													errmsglen);
	}
	PG_CATCH();
	{
		DisownLatch(&wrk->backendLatch);
		PG_RE_THROW();
	}
	PG_END_TRY();

	DisownLatch(&wrk->backendLatch);

	return result;
}

JsonbcKeyId
jsonbcDictWorkerGetIdByName(JsonbcDictId dict, JsonbcKeyName key,
							JsonbcKeyId nextKeyId)
{
	JsonbcDictWorkerHandle	worker;
	JsonbcKeyId				result;
	char				   *errmessage;
	int					    errmessagelen;

	if (jsonbc_max_workers <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("cannot insert new keys into pg_jsonbc_dict: "
						"no jsonbc dictionary workers were configured"),
				 errhint("Consider increasing the configuration parameter "
						 "\"jsonbc_max_workers\".")));

	worker = jsonbcDictWorkerAcquire(MyDatabaseId);

	result = jsonbcDictWorkerExecRequest(worker.worker, dict, key, nextKeyId,
										 &errmessage, &errmessagelen);

	jsonbcDictWorkerRelease(&worker);

	if (result == JsonbcInvalidKeyId)
		elog(ERROR, "failed to insert key into jsonbc dictionary (%.*s)",
			 errmessagelen, errmessage);

	return result;
}
