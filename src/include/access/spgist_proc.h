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

#define SPGISTSearchItemIsHeap(item)	((item).itemState == HEAP_RECHECK \
									  || (item).itemState == HEAP_NORECHECK)

extern void spgAddSearchItemToQueue(SpGistScanOpaque so, SpGistSearchItem *item, double *distances);

extern SpGistSearchItem *spgNewHeapItem(SpGistScanOpaque so, int level,
		ItemPointerData heapPtr, Datum leafValue, bool recheck, bool isnull);

extern void spgFreeSearchItem(SpGistScanOpaque so, SpGistSearchItem *item);

#endif   /* SPGIST_SEARCH_H */
