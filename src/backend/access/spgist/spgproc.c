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

int
SpGistSearchTreeItemComparator(const RBNode *a, const RBNode *b, void *arg)
{
	const SpGistSearchTreeItem *sa = (const SpGistSearchTreeItem *) a;
	const SpGistSearchTreeItem *sb = (const SpGistSearchTreeItem *) b;
	IndexScanDesc scan = (IndexScanDesc) arg;
	int i;

	/* Order according to distance comparison */
	for (i = 0; i < scan->numberOfOrderBys; i++)
	{
		if (sa->distances[i] != sb->distances[i])
			return (sa->distances[i] > sb->distances[i]) ? 1 : -1;
	}

	return 0;
}

void
SpGistSearchTreeItemCombiner(RBNode *existing, const RBNode *newrb, void *arg)
{
	SpGistSearchTreeItem *scurrent = (SpGistSearchTreeItem *) existing;
	const SpGistSearchTreeItem *snew = (const SpGistSearchTreeItem *) newrb;
	SpGistSearchItem *newitem = snew->head;

	/* snew should have just one item in its chain */
	Assert(newitem && newitem->next == NULL);

	/*
	 * If new item is heap tuple, it goes to front of chain; otherwise insert
	 * it before the first index-page item, so that index pages are visited in
	 * LIFO order, ensuring depth-first search of index pages.  See comments
	 * in gist_private.h.
	 */
	if (SPGISTSearchItemIsHeap(*newitem))
	{
		newitem->next = scurrent->head;
		scurrent->head = newitem;
		if (scurrent->lastHeap == NULL)
			scurrent->lastHeap = newitem;
	}
	else if (scurrent->lastHeap == NULL)
	{
		newitem->next = scurrent->head;
		scurrent->head = newitem;
	}
	else
	{
		newitem->next = scurrent->lastHeap->next;
		scurrent->lastHeap->next = newitem;
	}
}

RBNode *
SpGistSearchTreeItemAllocator(void *arg)
{
	IndexScanDesc scan = (IndexScanDesc) arg;

	return (RBNode *) palloc(SPGISTHDRSZ + sizeof(double) * scan->numberOfOrderBys);
}

void
SpGistSearchTreeItemDeleter(RBNode *rb, void *arg)
{
	pfree(rb);
}

/*
 * Construct SpGistSearchTreeItem storing item and add it to queue
 *
 * Called in queue context
 */
void
spgAddSearchItemToQueue(SpGistScanOpaque so, SpGistSearchItem *item,
						double *distances)
{
	bool isNew;
	SpGistSearchTreeItem *newItem = so->tmpTreeItem;
	memcpy(newItem->distances, distances, so->numberOfOrderBys * sizeof(double));
	newItem->head = item;
	item->next = NULL;
	newItem->lastHeap = SPGISTSearchItemIsHeap(*item) ? item : NULL;
	rb_insert(so->queue, (RBNode *) newItem, &isNew);
}

/*
 * Leaf SpGistSearchItem constructor, called in queue context
 */
SpGistSearchItem *
spgNewHeapItem(SpGistScanOpaque so, int level,
		ItemPointerData heapPtr, Datum leafValue, bool recheck, bool isnull)
{
	SpGistSearchItem *newItem = (SpGistSearchItem *) palloc(sizeof(SpGistSearchItem));
	newItem->next = NULL;
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
