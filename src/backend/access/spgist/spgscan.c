/*-------------------------------------------------------------------------
 *
 * spgscan.c
 *	  routines for scanning SP-GiST indexes
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/spgist/spgscan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "access/spgist_private.h"
#include "access/spgist_proc.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"


typedef void (*storeRes_func) (SpGistScanOpaque so, ItemPointer heapPtr,
							   Datum leafValue, bool isnull, bool recheck);

static void
spgAddStartItem(IndexScanDesc scan, bool isnull)
{
	SpGistScanOpaque	 so = (SpGistScanOpaque) scan->opaque;
	SpGistSearchItem	*startEntry =
			(SpGistSearchItem *) palloc0(sizeof(SpGistSearchItem));

	ItemPointerSet(&startEntry->heap,
				   isnull ? SPGIST_NULL_BLKNO : SPGIST_ROOT_BLKNO,
				   FirstOffsetNumber);
	startEntry->itemState = INNER;
	startEntry->level = 0;
	startEntry->isnull = isnull;

	spgAddSearchItemToQueue(scan, startEntry, so->distances);
}

/*
 * Initialize queue to search the root page, resetting
 * any previously active scan
 */
static void
resetSpGistScanOpaque(IndexScanDesc scan)
{
	SpGistScanOpaque	so = (SpGistScanOpaque) scan->opaque;
	MemoryContext	 	oldCtx = MemoryContextSwitchTo(so->queueCxt);

	memset(so->distances, 0, sizeof(double) * scan->numberOfOrderBys);

	if (so->searchNulls)
		/* Add a work item to scan the null index entries */
		spgAddStartItem(scan, true);

	if (so->searchNonNulls)
		/* Add a work item to scan the non-null index entries */
		spgAddStartItem(scan, false);

	MemoryContextSwitchTo(oldCtx);

	if (so->want_itup)
	{
		/* Must pfree reconstructed tuples to avoid memory leak */
		int			i;

		for (i = 0; i < so->nPtrs; i++)
			pfree(so->reconTups[i]);
	}
	so->iPtr = so->nPtrs = 0;
}

/*
 * Prepare scan keys in SpGistScanOpaque from caller-given scan keys
 *
 * Sets searchNulls, searchNonNulls, numberOfKeys, keyData fields of *so.
 *
 * The point here is to eliminate null-related considerations from what the
 * opclass consistent functions need to deal with.  We assume all SPGiST-
 * indexable operators are strict, so any null RHS value makes the scan
 * condition unsatisfiable.  We also pull out any IS NULL/IS NOT NULL
 * conditions; their effect is reflected into searchNulls/searchNonNulls.
 */
static void
spgPrepareScanKeys(IndexScanDesc scan)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;
	bool		qual_ok;
	bool		haveIsNull;
	bool		haveNotNull;
	int			nkeys;
	int			i;

	if (scan->numberOfKeys <= 0)
	{
		/* If no quals, whole-index scan is required */
		so->searchNulls = true;
		so->searchNonNulls = true;
		so->numberOfKeys = 0;
		return;
	}

	/* Examine the given quals */
	qual_ok = true;
	haveIsNull = haveNotNull = false;
	nkeys = 0;
	for (i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		skey = &scan->keyData[i];

		if (skey->sk_flags & SK_SEARCHNULL)
			haveIsNull = true;
		else if (skey->sk_flags & SK_SEARCHNOTNULL)
			haveNotNull = true;
		else if (skey->sk_flags & SK_ISNULL)
		{
			/* ordinary qual with null argument - unsatisfiable */
			qual_ok = false;
			break;
		}
		else
		{
			/* ordinary qual, propagate into so->keyData */
			so->keyData[nkeys++] = *skey;
			/* this effectively creates a not-null requirement */
			haveNotNull = true;
		}
	}

	/* IS NULL in combination with something else is unsatisfiable */
	if (haveIsNull && haveNotNull)
		qual_ok = false;

	/* Emit results */
	if (qual_ok)
	{
		so->searchNulls = haveIsNull;
		so->searchNonNulls = haveNotNull;
		so->numberOfKeys = nkeys;
	}
	else
	{
		so->searchNulls = false;
		so->searchNonNulls = false;
		so->numberOfKeys = 0;
	}
}

IndexScanDesc
spgbeginscan(Relation rel, int keysz, int orderbysz)
{
	IndexScanDesc scan;
	SpGistScanOpaque so;

	scan = RelationGetIndexScan(rel, keysz, orderbysz);

	so = (SpGistScanOpaque) palloc0(sizeof(SpGistScanOpaqueData));
	if (keysz > 0)
		so->keyData = (ScanKey) palloc(sizeof(ScanKeyData) * keysz);
	else
		so->keyData = NULL;
	initSpGistState(&so->state, scan->indexRelation);

	so->tempCxt = AllocSetContextCreate(CurrentMemoryContext,
										"SP-GiST search temporary context",
										ALLOCSET_DEFAULT_SIZES);

	/* Set up indexTupDesc and xs_hitupdesc in case it's an index-only scan */
	so->indexTupDesc = scan->xs_hitupdesc = RelationGetDescr(rel);
	so->tmpTreeItem = palloc(SPGISTHDRSZ + sizeof(double) * scan->numberOfOrderBys);
	so->distances = palloc(sizeof(double) * scan->numberOfOrderBys);

	so->queueCxt = AllocSetContextCreate(CurrentMemoryContext,
										 "SP-GiST queue context",
										 ALLOCSET_DEFAULT_SIZES);

	scan->opaque = so;

	return scan;
}

void
spgrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;
	MemoryContext oldCxt;

	/* copy scankeys into local storage */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));

	/* preprocess scankeys, set up the representation in *so */
	spgPrepareScanKeys(scan);

	MemoryContextReset(so->queueCxt);
	oldCxt = MemoryContextSwitchTo(so->queueCxt);
	so->queue = rb_create(
			SPGISTHDRSZ + sizeof(double) * scan->numberOfOrderBys,
			SpGistSearchTreeItemComparator,
			SpGistSearchTreeItemCombiner,
			SpGistSearchTreeItemAllocator,
			SpGistSearchTreeItemDeleter,
			scan);
	MemoryContextSwitchTo(oldCxt);

	/* set up starting queue entries */
	resetSpGistScanOpaque(scan);
	so->curTreeItem = NULL;

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys,
				scan->numberOfOrderBys * sizeof(ScanKeyData));
}

void
spgendscan(IndexScanDesc scan)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	MemoryContextDelete(so->tempCxt);
	MemoryContextDelete(so->queueCxt);
	pfree(so->tmpTreeItem);
	if (scan->numberOfOrderBys > 0)
		pfree(so->distances);
}

/*
 * Test whether a leaf tuple satisfies all the scan keys
 *
 * *reportedSome is set to true if:
 *		the scan is not ordered AND the item satisfies the scankeys
 */
static bool
spgLeafTest(Relation index, IndexScanDesc scan,
			SpGistLeafTuple leafTuple, bool isnull,
			int level, Datum reconstructedValue,
			void *traversalValue,
			bool *reportedSome, storeRes_func storeRes)
{
	SpGistScanOpaque		so = scan->opaque;
	spgLeafConsistentIn		in;
	spgLeafConsistentOut	out;
	FmgrInfo			   *procinfo;
	MemoryContext			oldCtx;
	Datum					leafDatum;
	Datum					leafValue;
	bool					result;
	bool					recheck;

	/* use temp context for calling leaf_consistent */
	oldCtx = MemoryContextSwitchTo(so->tempCxt);

	if (isnull)
	{
		/* Should not have arrived on a nulls page unless nulls are wanted */
		Assert(so->searchNulls);
		leafValue = (Datum) 0;
		recheck = false;
		result = true;
		goto report;
	}

	leafDatum = SGLTDATUM(leafTuple, &so->state);

	in.scankeys = so->keyData;
	in.nkeys = so->numberOfKeys;
	in.reconstructedValue = reconstructedValue;
	in.traversalValue = traversalValue;
	in.level = level;
	in.returnData = so->want_itup;
	in.leafDatum = leafDatum;
	in.orderbykeys = scan->orderByData;
	in.norderbys = scan->numberOfOrderBys;

	out.leafValue = (Datum) 0;
	out.recheck = false;
	out.distances = NULL;

	procinfo = index_getprocinfo(index, 1, SPGIST_LEAF_CONSISTENT_PROC);
	result = DatumGetBool(FunctionCall2Coll(procinfo,
											index->rd_indcollation[0],
											PointerGetDatum(&in),
											PointerGetDatum(&out)));
	recheck = out.recheck;
	leafValue = out.leafValue;

report:
	if (result)
	{
		/* item passes the scankeys */
		if (scan->numberOfOrderBys > 0)
		{
			/* the scan is ordered -> add the item to the queue */
			if (isnull)
			{
				/* Assume that all distances for null entries are infinities */
				int i;
				out.distances = palloc(scan->numberOfOrderBys * sizeof(double));
				for (i = 0; i < scan->numberOfOrderBys; ++i)
					out.distances[i] = get_float8_infinity();
			}
			MemoryContextSwitchTo(so->queueCxt);
			spgAddSearchItemToQueue(scan,
				spgNewHeapItem(so, level, leafTuple->heapPtr, leafValue, recheck, isnull), 
				out.distances);
		}
		else
		{
			/* non-ordered scan, so report the item right away */
			MemoryContextSwitchTo(oldCtx);
			storeRes(so, &leafTuple->heapPtr, leafValue, isnull, recheck);
			*reportedSome = true;
		}
	}
	MemoryContextSwitchTo(oldCtx);

	return result;
}

/* A bundle initializer for inner_consistent methods */
static void
spgInitInnerConsistentIn(spgInnerConsistentIn *in, IndexScanDesc scan,
						 SpGistSearchItem *item,
						 SpGistInnerTuple innerTuple,
						 MemoryContext traversalMemoryContext)
{
	SpGistScanOpaque so = scan->opaque;
	in->scankeys = so->keyData;
	in->nkeys = so->numberOfKeys;
	in->reconstructedValue = item->value;
	in->traversalMemoryContext = traversalMemoryContext;
	in->traversalValue = item->traversalValue;
	in->level = item->level;
	in->returnData = so->want_itup;
	in->allTheSame = innerTuple->allTheSame;
	in->hasPrefix = (innerTuple->prefixSize > 0);
	in->prefixDatum = SGITDATUM(innerTuple, &so->state);
	in->nNodes = innerTuple->nNodes;
	in->nodeLabels = spgExtractNodeLabels(&so->state, innerTuple);
	in->norderbys = scan->numberOfOrderBys;
	in->orderbyKeys = scan->orderByData;
}

static void
spgInnerTest(Relation index, IndexScanDesc scan, SpGistSearchItem *item,
			 SpGistInnerTuple innerTuple, bool isnull)
{
	SpGistScanOpaque		so = scan->opaque;
	MemoryContext			oldCxt = MemoryContextSwitchTo(so->tempCxt);
	spgInnerConsistentIn	in;
	spgInnerConsistentOut	out;
	SpGistNodeTuple			*nodes;
	SpGistNodeTuple			node;
	int						i;
	double					*inf_distances = palloc(scan->numberOfOrderBys * sizeof (double));

	for (i = 0; i < scan->numberOfOrderBys; ++i)
		inf_distances[i] = get_float8_infinity();

	spgInitInnerConsistentIn(&in, scan, item, innerTuple, oldCxt);

	/* collect node pointers */
	nodes = (SpGistNodeTuple *) palloc(sizeof(SpGistNodeTuple) * in.nNodes);
	SGITITERATE(innerTuple, i, node)
	{
		nodes[i] = node;
	}

	memset(&out, 0, sizeof(out));

	if (!isnull)
	{
		/* use user-defined inner consistent method */
		FmgrInfo *consistent_procinfo =
				index_getprocinfo(index, 1, SPGIST_INNER_CONSISTENT_PROC);
		FunctionCall2Coll(consistent_procinfo,
						  index->rd_indcollation[0],
						  PointerGetDatum(&in),
						  PointerGetDatum(&out));
	}
	else
	{
		/* force all children to be visited */
		out.nNodes = in.nNodes;
		out.nodeNumbers = (int *) palloc(sizeof(int) * in.nNodes);
		for (i = 0; i < in.nNodes; i++)
			out.nodeNumbers[i] = i;
	}

	MemoryContextSwitchTo(so->queueCxt);

	/* If allTheSame, they should all or none of 'em match */
	if (innerTuple->allTheSame)
		if (out.nNodes != 0 && out.nNodes != in.nNodes)
			elog(ERROR, "inconsistent inner_consistent results for allTheSame inner tuple");

	for (i = 0; i < out.nNodes; i++)
	{
		int			nodeN = out.nodeNumbers[i];

		Assert(nodeN >= 0 && nodeN < in.nNodes);
		if (ItemPointerIsValid(&nodes[nodeN]->t_tid))
		{
			double *distances;
			SpGistSearchItem *newItem;

			/* Create new work item for this node */
			newItem = palloc(sizeof(SpGistSearchItem));
			newItem->heap = nodes[nodeN]->t_tid;
			newItem->level = out.levelAdds ? item->level + out.levelAdds[i]
										   : item->level;

			/* Must copy value out of temp context */
			newItem->value = out.reconstructedValues ?
					datumCopy(out.reconstructedValues[i],
							  so->state.attType.attbyval,
							  so->state.attType.attlen) : (Datum) 0;

			/*
			 * Elements of out.traversalValues should be allocated in
			 * in.traversalMemoryContext, which is actually a long
			 * lived context of index scan.
			 */
			newItem->traversalValue = (out.traversalValues) ?
					out.traversalValues[i] : NULL;

			/* Will copy out the distances in spgAddSearchItemToQueue anyway */
			distances = out.distances ? out.distances[i] : inf_distances;

			newItem->itemState = INNER;
			newItem->isnull = isnull;
			spgAddSearchItemToQueue(scan, newItem, distances);
		}
	}

	MemoryContextSwitchTo(oldCxt);
}

/* Returns a next item in an (ordered) scan or null if the index is exhausted */
static SpGistSearchItem *
spgGetNextQueueItem(SpGistScanOpaque so)
{
	MemoryContext oldCxt = MemoryContextSwitchTo(so->queueCxt);
	SpGistSearchItem *item = NULL;

	while (item == NULL)
	{
		/* Update curTreeItem if we don't have one */
		if (so->curTreeItem == NULL)
		{
			so->curTreeItem = (SpGistSearchTreeItem *) rb_leftmost(so->queue);
			/* Done when tree is empty */
			if (so->curTreeItem == NULL)
				break;
		}

		item = so->curTreeItem->head;
		if (item != NULL)
		{
			/* Delink item from chain */
			so->curTreeItem->head = item->next;
			if (item == so->curTreeItem->lastHeap)
				so->curTreeItem->lastHeap = NULL;
		}
		else
		{
			/* curTreeItem is exhausted, so remove it from rbtree */
			rb_delete(so->queue, (RBNode *) so->curTreeItem);
			so->curTreeItem = NULL;
		}
	}

	MemoryContextSwitchTo(oldCxt);
	return item;
}

/*
 * Walk the tree and report all tuples passing the scan quals to the storeRes
 * subroutine.
 *
 * If scanWholeIndex is true, we'll do just that.  If not, we'll stop at the
 * next page boundary once we have reported at least one tuple.
 */
static void
spgWalk(Relation index, IndexScanDesc scan, bool scanWholeIndex,
		storeRes_func storeRes, Snapshot snapshot)
{
	Buffer		buffer = InvalidBuffer;
	bool		reportedSome = false;
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	while (scanWholeIndex || !reportedSome)
	{
		SpGistSearchItem *item = spgGetNextQueueItem(so);

		if (item == NULL)
			break; /* No more items in queue -> done */

redirect:
		/* Check for interrupts, just in case of infinite loop */
		CHECK_FOR_INTERRUPTS();

		if (SPGISTSearchItemIsHeap(*item))
		{
			/* We store heap items in the queue only in case of ordered search */
			Assert(scan->numberOfOrderBys > 0);
			storeRes(so, &item->heap, item->value, item->isnull,
					 item->itemState == HEAP_RECHECK);
			reportedSome = true;
		}
		else
		{
			BlockNumber		blkno  = ItemPointerGetBlockNumber(&item->heap);
			OffsetNumber	offset = ItemPointerGetOffsetNumber(&item->heap);
			Page			page;
			bool			isnull;

			if (buffer == InvalidBuffer)
			{
				buffer = ReadBuffer(index, blkno);
				LockBuffer(buffer, BUFFER_LOCK_SHARE);
			}
			else if (blkno != BufferGetBlockNumber(buffer))
			{
				UnlockReleaseBuffer(buffer);
				buffer = ReadBuffer(index, blkno);
				LockBuffer(buffer, BUFFER_LOCK_SHARE);
			}

			/* else new pointer points to the same page, no work needed */

			page = BufferGetPage(buffer);
			TestForOldSnapshot(snapshot, index, page);

			isnull = SpGistPageStoresNulls(page) ? true : false;

			if (SpGistPageIsLeaf(page))
			{
				/* Page is a leaf - that is, all it's tuples are heap items */
				SpGistLeafTuple leafTuple;
				OffsetNumber max = PageGetMaxOffsetNumber(page);

				if (SpGistBlockIsRoot(blkno))
				{
					/* When root is a leaf, examine all its tuples */
					for (offset = FirstOffsetNumber; offset <= max; offset++)
					{
						leafTuple = (SpGistLeafTuple)
							PageGetItem(page, PageGetItemId(page, offset));
						if (leafTuple->tupstate != SPGIST_LIVE)
						{
							/* all tuples on root should be live */
							elog(ERROR, "unexpected SPGiST tuple state: %d",
									leafTuple->tupstate);
						}

						Assert(ItemPointerIsValid(&leafTuple->heapPtr));
						spgLeafTest(index, scan, leafTuple, isnull, item->level,
								item->value, item->traversalValue,
								&reportedSome, storeRes);
					}
				}
				else
				{
					/* Normal case: just examine the chain we arrived at */
					while (offset != InvalidOffsetNumber)
					{
						Assert(offset >= FirstOffsetNumber && offset <= max);
						leafTuple = (SpGistLeafTuple)
							PageGetItem(page, PageGetItemId(page, offset));
						if (leafTuple->tupstate != SPGIST_LIVE)
						{
							if (leafTuple->tupstate == SPGIST_REDIRECT)
							{
								/* redirection tuple should be first in chain */
								Assert(offset == ItemPointerGetOffsetNumber(&item->heap));
								/* transfer attention to redirect point */
								item->heap = ((SpGistDeadTuple) leafTuple)->pointer;
								Assert(ItemPointerGetBlockNumber(&item->heap) != SPGIST_METAPAGE_BLKNO);
								goto redirect;
							}
							if (leafTuple->tupstate == SPGIST_DEAD)
							{
								/* dead tuple should be first in chain */
								Assert(offset == ItemPointerGetOffsetNumber(&item->heap));
								/* No live entries on this page */
								Assert(leafTuple->nextOffset == InvalidOffsetNumber);
								break;
							}
							/* We should not arrive at a placeholder */
							elog(ERROR, "unexpected SPGiST tuple state: %d",
									leafTuple->tupstate);
						}

						Assert(ItemPointerIsValid(&leafTuple->heapPtr));
						spgLeafTest(index, scan, leafTuple, isnull, item->level,
								item->value, item->traversalValue,
								&reportedSome, storeRes);
						offset = leafTuple->nextOffset;
					}
				}
			}
			else	/* page is inner */
			{
				SpGistInnerTuple innerTuple = (SpGistInnerTuple)
						PageGetItem(page, PageGetItemId(page, offset));

				if (innerTuple->tupstate != SPGIST_LIVE)
				{
					if (innerTuple->tupstate == SPGIST_REDIRECT)
					{
						/* transfer attention to redirect point */
						item->heap = ((SpGistDeadTuple) innerTuple)->pointer;
						Assert(ItemPointerGetBlockNumber(&item->heap) !=
							   SPGIST_METAPAGE_BLKNO);
						goto redirect;
					}
					elog(ERROR, "unexpected SPGiST tuple state: %d",
						 innerTuple->tupstate);
				}

				spgInnerTest(index, scan, item, innerTuple, isnull);
			}
		}

		/* done with this scan item */
		spgFreeSearchItem(so, item);
		/* clear temp context before proceeding to the next one */
		MemoryContextReset(so->tempCxt);
	}

	if (buffer != InvalidBuffer)
		UnlockReleaseBuffer(buffer);
}


/* storeRes subroutine for getbitmap case */
static void
storeBitmap(SpGistScanOpaque so, ItemPointer heapPtr,
			Datum leafValue, bool isnull, bool recheck)
{
	tbm_add_tuples(so->tbm, heapPtr, 1, recheck);
	so->ntids++;
}

int64
spggetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	/* Copy want_itup to *so so we don't need to pass it around separately */
	so->want_itup = false;

	so->tbm = tbm;
	so->ntids = 0;

	spgWalk(scan->indexRelation, scan, true, storeBitmap, scan->xs_snapshot);

	return so->ntids;
}

/* storeRes subroutine for gettuple case */
static void
storeGettuple(SpGistScanOpaque so, ItemPointer heapPtr,
			  Datum leafValue, bool isnull, bool recheck)
{
	Assert(so->nPtrs < MaxIndexTuplesPerPage);
	so->heapPtrs[so->nPtrs] = *heapPtr;
	so->recheck[so->nPtrs] = recheck;
	if (so->want_itup)
	{
		/*
		 * Reconstruct index data.  We have to copy the datum out of the temp
		 * context anyway, so we may as well create the tuple here.
		 */
		so->reconTups[so->nPtrs] = heap_form_tuple(so->indexTupDesc,
												   &leafValue,
												   &isnull);
	}
	so->nPtrs++;
}

bool
spggettuple(IndexScanDesc scan, ScanDirection dir)
{
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	if (dir != ForwardScanDirection)
		elog(ERROR, "SP-GiST only supports forward scan direction");

	/* Copy want_itup to *so so we don't need to pass it around separately */
	so->want_itup = scan->xs_want_itup;

	for (;;)
	{
		if (so->iPtr < so->nPtrs)
		{
			/* continuing to return reported tuples */
			scan->xs_ctup.t_self = so->heapPtrs[so->iPtr];
			scan->xs_recheck = so->recheck[so->iPtr];
			scan->xs_recheckorderby = false;
			scan->xs_hitup = so->reconTups[so->iPtr];
			so->iPtr++;
			return true;
		}

		if (so->want_itup)
		{
			/* Must pfree reconstructed tuples to avoid memory leak */
			int			i;

			for (i = 0; i < so->nPtrs; i++)
				pfree(so->reconTups[i]);
		}
		so->iPtr = so->nPtrs = 0;

		spgWalk(scan->indexRelation, scan, false, storeGettuple,
				scan->xs_snapshot);

		if (so->nPtrs == 0)
			break;				/* must have completed scan */
	}

	return false;
}

bool
spgcanreturn(Relation index, int attno)
{
	SpGistCache *cache;

	/* We can do it if the opclass config function says so */
	cache = spgGetCache(index);

	return cache->config.canReturnData;
}
