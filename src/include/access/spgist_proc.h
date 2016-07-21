#ifndef SPGIST_SEARCH_H
#define SPGIST_SEARCH_H
#include "postgres.h"

#include "access/relscan.h"
#include "access/spgist_private.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/geo_decls.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#define SPGISTHDRSZ offsetof(SpGistSearchTreeItem, distances)
#define SPGISTSearchItemIsHeap(item)	((item).itemState == HEAP_RECHECK \
									  || (item).itemState == HEAP_NORECHECK)

extern int SpGistSearchTreeItemComparator(const RBNode *a, const RBNode *b, void *arg);

extern void SpGistSearchTreeItemCombiner(RBNode *existing, const RBNode *newrb, void *arg);

#define GSTIHDRSZ offsetof(SpGistSearchTreeItem, distances)

extern RBNode * SpGistSearchTreeItemAllocator(void *arg);

extern void SpGistSearchTreeItemDeleter(RBNode *rb, void *arg);

extern void spgAddSearchItemToQueue(SpGistScanOpaque so, SpGistSearchItem *item, double *distances);

extern SpGistSearchItem *spgNewHeapItem(SpGistScanOpaque so, int level,
		ItemPointerData heapPtr, Datum leafValue, bool recheck, bool isnull);

extern void spgFreeSearchItem(SpGistScanOpaque so, SpGistSearchItem *item);

#endif   /* SPGIST_SEARCH_H */
