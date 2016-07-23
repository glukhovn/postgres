/*-------------------------------------------------------------------------
 *
 * spgproc.c
 *	  Common procedures for SP-GiST.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/spgist/spgproc.c
 *
 *-------------------------------------------------------------------------
 */

#include "access/spgist_proc.h"

/*
 * Add SpGistSearchItem to queue
 *
 * Called in queue context
 */
void
spgAddSearchItemToQueue(SpGistScanOpaque so, SpGistSearchItem *item,
						double *distances)
{
	memcpy(item->distances, distances, so->numberOfOrderBys * sizeof(double));
	pairingheap_add(so->queue, &item->phNode);
}

/*
 * Leaf SpGistSearchItem constructor, called in queue context
 */
SpGistSearchItem *
spgNewHeapItem(SpGistScanOpaque so, int level,
		ItemPointerData heapPtr, Datum leafValue, bool recheck, bool isnull)
{
	SpGistSearchItem *newItem = (SpGistSearchItem *)
			palloc(SizeOfSpGistSearchItem(so->numberOfOrderBys));
	newItem->level = level;
	newItem->heap = heapPtr;
	/* copy value to queue cxt out of tmp cxt */
	newItem->value = isnull ? (Datum) 0 :
			datumCopy(leafValue, so->state.attType.attbyval,
				so->state.attType.attlen);
	newItem->traversalValue = NULL;
	newItem->itemState = recheck ? HEAP_RECHECK : HEAP_NORECHECK;
	newItem->isnull = isnull;
	return newItem;
}

void
spgFreeSearchItem(SpGistScanOpaque so, SpGistSearchItem *item)
{
	if (!so->state.attType.attbyval &&
			DatumGetPointer(item->value) != NULL)
		pfree(DatumGetPointer(item->value));

	if (item->traversalValue)
		pfree(item->traversalValue);

	pfree(item);
}
