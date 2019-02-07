/*-------------------------------------------------------------------------
 *
 * gistbuild.c
 *	  build algorithm for GiST indexes implementation.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistbuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/gist_private.h"
#include "access/gistxlog.h"
#include "access/nbtree.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* Step of index tuples for check whether to switch to buffering build mode */
#define BUFFERING_MODE_SWITCH_CHECK_STEP 256

/*
 * Number of tuples to process in the slow way before switching to buffering
 * mode, when buffering is explicitly turned on. Also, the number of tuples
 * to process between readjusting the buffer size parameter, while in
 * buffering mode.
 */
#define BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET 4096

typedef enum
{
	GIST_BUFFERING_DISABLED,	/* in regular build mode and aren't going to
								 * switch */
	GIST_BUFFERING_AUTO,		/* in regular build mode, but will switch to
								 * buffering build mode if the index grows too
								 * big */
	GIST_BUFFERING_STATS,		/* gathering statistics of index tuple size
								 * before switching to the buffering build
								 * mode */
	GIST_BUFFERING_ACTIVE		/* in buffering build mode */
} GistBufferingMode;

/* Working state for gistbuild and its callback */
typedef struct
{
	Relation	indexrel;
	Relation	heaprel;
	GISTSTATE  *giststate;

	int64		indtuples;		/* number of tuples indexed */
	int64		indtuplesSize;	/* total size of all indexed tuples */

	Size		freespace;		/* amount of free space to leave on pages */

	/*
	 * Extra data structures used during a buffering build. 'gfbb' contains
	 * information related to managing the build buffers. 'parentMap' is a
	 * lookup table of the parent of each internal page.
	 */
	GISTBuildBuffers *gfbb;
	HTAB	   *parentMap;

	GistBufferingMode bufferingMode;
} GISTBuildState;

/* prototypes for private functions */
static void gistInitBuffering(GISTBuildState *buildstate);
static int	calculatePagesPerBuffer(GISTBuildState *buildstate, int levelStep);
static void gistBuildCallback(Relation index,
				  HeapTuple htup,
				  Datum *values,
				  bool *isnull,
				  bool tupleIsAlive,
				  void *state);
static void gistBufferingBuildInsert(GISTBuildState *buildstate,
						 IndexTuple itup);
static bool gistProcessItup(GISTBuildState *buildstate, IndexTuple itup,
				BlockNumber startblkno, int startlevel);
static BlockNumber gistbufferinginserttuples(GISTBuildState *buildstate,
						  Buffer buffer, int level,
						  IndexTuple *itup, int ntup, OffsetNumber oldoffnum,
						  BlockNumber parentblk, OffsetNumber downlinkoffnum);
static Buffer gistBufferingFindCorrectParent(GISTBuildState *buildstate,
							   BlockNumber childblkno, int level,
							   BlockNumber *parentblk,
							   OffsetNumber *downlinkoffnum);
static void gistProcessEmptyingQueue(GISTBuildState *buildstate);
static void gistEmptyAllBuffers(GISTBuildState *buildstate);
static int	gistGetMaxLevel(Relation index);

static void gistInitParentMap(GISTBuildState *buildstate);
static void gistMemorizeParent(GISTBuildState *buildstate, BlockNumber child,
				   BlockNumber parent);
static void gistMemorizeAllDownlinks(GISTBuildState *buildstate, Buffer parent);
static BlockNumber gistGetParent(GISTBuildState *buildstate, BlockNumber child);

typedef struct GistWriteState
{
	BTWriteState btws;
	GISTSTATE  *giststate;
	Size		freespace;
} GistWriteState;

static Page
gistNewPage(int level)
{
	Page		page = (Page) palloc(BLCKSZ);

	/* Zero the page and set up standard page header info */
	GISTInitPage(page, BLCKSZ, level ? 0 : F_LEAF);

	return page;
}

static void
gistSortAddTup(Page page, Size itemsize, IndexTuple itup,
			   OffsetNumber itup_off)
{
	if (PageAddItem(page, (Item) itup, itemsize, itup_off,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to the index page");
}

static IndexTuple
gistMakeUnionOfTuples(Relation index, GISTSTATE *giststate,
					  IndexTuple *itvec, int len)
{
	Datum		attr[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	gistMakeUnionItVec(giststate, itvec, len, attr, isnull);

	return gistFormTuple(giststate, index, attr, isnull, false);
}

typedef struct GistPageState
{
	Page		btps_page;		/* workspace for page building */
	BlockNumber btps_blkno;		/* block # to write this page at */
	OffsetNumber btps_lastoff;	/* last item offset loaded */
	uint32		btps_level;		/* tree level (0 = leaf) */
	Size		btps_full;		/* "full" if less than this much free space */
	struct GistPageState *btps_next;	/* link to parent level, if any */
	IndexTuple	tuples[BLCKSZ / sizeof(IndexTupleData)];
} GistPageState;

static GistPageState *
gistPageState(GistWriteState *wstate, uint32 level)
{
	GistPageState *state = (GistPageState *) palloc0(sizeof(*state));

	/* create initial page for level */
	state->btps_page = gistNewPage(level);

	/* and assign it a page position */
	state->btps_blkno = wstate->btws.btws_pages_alloced++;

	/* initialize lastoff so first item goes into P_FIRSTKEY */
	state->btps_lastoff = FirstOffsetNumber;
	state->btps_level = level;

	/* set "full" threshold based on level.  See notes at head of file. */
	state->btps_full = wstate->freespace; /* FIXME */

	/* no parent level, yet */
	state->btps_next = NULL;

	return state;
}

/*
 * Maximum size of a GiST index entry, including its tuple header.
 *
 * We actually need to be able to fit one items on every page,
 * so restrict any one item to 1/1 the per-page available space.
 */
#define GistMaxItemSize(page) \
	MAXALIGN_DOWN((PageGetPageSize(page) - \
				   MAXALIGN(SizeOfPageHeaderData + 1*sizeof(ItemIdData)) - \
				   MAXALIGN(sizeof(GISTPageOpaqueData))) / 1)

static void
gistBuildAdd(GistWriteState *wstate, GistPageState *state, IndexTuple itup);

static void
gistFinishPage(GistWriteState *wstate, GistPageState *state,
			   BlockNumber blkno)
{
	int			i;
	IndexTuple	unionTuple =
		gistMakeUnionOfTuples(wstate->btws.index, wstate->giststate,
							  state->tuples, state->btps_lastoff - 1);

	ItemPointerSetBlockNumber(&unionTuple->t_tid, blkno);

	gistBuildAdd(wstate, state->btps_next, unionTuple);

	for (i = 0; i < state->btps_lastoff - 1; i++)
		pfree(state->tuples[i]);
}

static void
gistBuildAdd(GistWriteState *wstate, GistPageState *state, IndexTuple itup)
{
	Page		npage;
	BlockNumber nblkno;
	OffsetNumber last_off;
	Size		pgspc;
	Size		itupsz;

	/*
	 * This is a handy place to check for cancel interrupts during the
	 * gist load phase of index creation.
	 */
	CHECK_FOR_INTERRUPTS();

	npage = state->btps_page;
	nblkno = state->btps_blkno;
	last_off = state->btps_lastoff;

	pgspc = PageGetFreeSpace(npage);
	itupsz = IndexTupleSize(itup);
	itupsz = MAXALIGN(itupsz);

	/*
	 * Check whether the item can fit on a btree page at all. (Eventually, we
	 * ought to try to apply TOAST methods if not.) We actually need to be
	 * able to fit three items on every page, so restrict any one item to 1/3
	 * the per-page available space. Note that at this point, itupsz doesn't
	 * include the ItemId.
	 *
	 * NOTE: similar code appears in _bt_insertonpg() to defend against
	 * oversize items being inserted into an already-existing index. But
	 * during creation of an index, we don't go through there.
	 */
	if (itupsz > GistMaxItemSize(npage))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
						itupsz, GistMaxItemSize(npage),
						RelationGetRelationName(wstate->btws.index)),
				 errhint("Values larger than 1/1 of a buffer page cannot be indexed.\n"
						 "Consider a function index of an MD5 hash of the value, "
						 "or use full text indexing."),
				 errtableconstraint(wstate->btws.heap,
									RelationGetRelationName(wstate->btws.index))));

	/*
	 * Check to see if page is "full".  It's definitely full if the item won't
	 * fit.  Otherwise, compare to the target freespace derived from the
	 * fillfactor.  However, we must put at least two items on each page, so
	 * disregard fillfactor if we don't have that many.
	 */
	if (pgspc < itupsz ||
		(pgspc < state->btps_full && last_off > FirstOffsetNumber))
	{
		/*
		 * Finish off the page and write it out.
		 */
		Page		opage = npage;
		BlockNumber oblkno = nblkno;

		/* Create new page of same level */
		npage = gistNewPage(state->btps_level);

		/* and assign it a page position */
		nblkno = wstate->btws.btws_pages_alloced++;

		/*
		 * Set the sibling links for both pages.
		 */
		GistPageGetOpaque(opage)->rightlink = nblkno;
		GistPageGetOpaque(npage)->rightlink = InvalidBlockNumber;	/* redundant */

		/*
		 * Link the old page into its parent, using its minimum key. If we
		 * don't have a parent, we have to create one; this adds a new btree
		 * level.
		 */
		if (state->btps_next == NULL)
			state->btps_next = gistPageState(wstate, state->btps_level + 1);

		gistFinishPage(wstate, state, oblkno);

		/*
		 * Write out the old page.  We never need to touch it again, so we can
		 * free the opage workspace too.
		 */
		bt_blwritepage(&wstate->btws, opage, oblkno);

		/*
		 * Reset last_off to point to new page
		 */
		last_off = FirstOffsetNumber;
	}

	state->tuples[last_off - 1] = CopyIndexTuple(itup);

	/*
	 * Add the new item into the current page.
	 */
	gistSortAddTup(npage, itupsz, itup, last_off);
	last_off = OffsetNumberNext(last_off);

	state->btps_page = npage;
	state->btps_blkno = nblkno;
	state->btps_lastoff = last_off;
}

static void
gistUpperShutdown(GistWriteState *wstate, GistPageState *state)
{
	GistPageState *s;

	/*
	 * Each iteration of this loop completes one more level of the tree.
	 */
	for (s = state; s != NULL; s = s->btps_next)
	{
		BlockNumber blkno = s->btps_blkno;

		/*
		 * We have to link the last page on this level to somewhere.
		 *
		 * If we're at the top, it's the root, so assign root page number.
		 * Otherwise, add an entry for it to its parent using its minimum key.
		 * This may cause the last page of the parent level to split, but
		 * that's not a problem -- we haven't gotten to it yet.
		 */
		if (s->btps_next == NULL)
			s->btps_blkno = GIST_ROOT_BLKNO;
		else
			gistFinishPage(wstate, s, blkno);

		bt_blwritepage(&wstate->btws, s->btps_page, s->btps_blkno);
		s->btps_page = NULL;	/* writepage freed the workspace */
	}
}

static void
gistLoad(GistWriteState *wstate, BTSpool *btspool)
{
	GistPageState *state = gistPageState(wstate, 0);
	IndexTuple	itup;

	while ((itup = tuplesort_getindextuple(btspool->sortstate, true)) != NULL)
	{
#if 1
		IndexTuple	itup2 = itup;
#else
		IndexTuple	itup2;
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];

		index_deform_tuple(itup, wstate->index->rd_att, values, isnull);
		itup2 = gistFormTuple(wstate->giststate, wstate->index,
							  values, isnull,
							  true /* size is currently bogus */ );

		ItemPointerCopy(&itup->t_tid, &itup2->t_tid);
#endif
		gistBuildAdd(wstate, state, itup2);
	}

	/* Close down final pages */
	gistUpperShutdown(wstate, state);

	/*
	 * If the index is WAL-logged, we must fsync it down to disk before it's
	 * safe to commit the transaction.  (For a non-WAL-logged index we don't
	 * care since the index will be uninteresting after a crash anyway.)
	 *
	 * It's obvious that we must do this when not WAL-logging the build. It's
	 * less obvious that we have to do it even if we did WAL-log the index
	 * pages.  The reason is that since we're building outside shared buffers,
	 * a CHECKPOINT occurring during the build has no way to flush the
	 * previously written data to disk (indeed it won't know the index even
	 * exists).  A crash later on would replay WAL from the checkpoint,
	 * therefore it wouldn't replay our earlier WAL entries. If we do not
	 * fsync those pages here, they might still not be on disk when the crash
	 * occurs.
	 */
	if (RelationNeedsWAL(wstate->btws.index))
	{
		RelationOpenSmgr(wstate->btws.index);
		smgrimmedsync(wstate->btws.index->rd_smgr, MAIN_FORKNUM);
	}
}

static Datum
gistCompressAttr(Relation indexRel, Datum val, int attno, void **cxt)
{
	GISTSTATE  *giststate = *cxt;
	GISTENTRY	centry;
	GISTENTRY  *cep;

	if (!giststate)
		*cxt = giststate = initGISTstate(indexRel);

	gistentryinit(centry, val, indexRel, NULL, (OffsetNumber) 0, true);

	/* there may not be a compress function in opclass */
	if (OidIsValid(giststate->compressFn[attno].fn_oid))
		cep = (GISTENTRY *)
			DatumGetPointer(FunctionCall1Coll(&giststate->compressFn[attno],
											  giststate->supportCollation[attno],
											  PointerGetDatum(&centry)));
	else
		cep = &centry;

	return cep->key;
}

void
gist_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
	index_parallel_build_main(seg, toc, gistCompressAttr);
}

static void
gistLeafBuild(void *cxt, BTSpool *btspool, BTSpool *btspool2)
{
	GistWriteState wstate;
	GISTBuildState *buildstate = cxt;

	Assert(!btspool2);

	tuplesort_performsort(btspool->sortstate);

	wstate.btws.heap = btspool->heap;
	wstate.btws.index = btspool->index;

	/*
	 * We need to log index creation in WAL iff WAL archiving/streaming
	 * is enabled UNLESS the index isn't WAL-logged anyway.
	 */
	wstate.btws.btws_use_wal =
		 /* XLogIsNeeded() && FIXME */ RelationNeedsWAL(wstate.btws.index);

	/* reserve the metapage */
	wstate.btws.btws_pages_alloced = GIST_ROOT_BLKNO + 1;
	wstate.btws.btws_pages_written = 0;
	wstate.btws.btws_zeropage = NULL;	/* until needed */

	wstate.giststate = buildstate->giststate;
	wstate.freespace = buildstate->freespace;

	gistLoad(&wstate, btspool);
}

static IndexBuildResult *
gistTrySortedBuild(GISTBuildState *buildstate, IndexInfo *indexInfo)
{
	Relation	index = buildstate->indexrel;
	AttrNumber	indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	AttrNumber	attno;

	for (attno = 1; attno <= indnkeyatts; attno++)
	{
		Oid			opfamily = index->rd_opfamily[attno - 1];
		//Oid			opcintype = indexRel->rd_opcintype[attno - 1];
		Oid			opckeytype = index->rd_att->attrs[attno - 1].atttypid;
		Oid			procid = get_opfamily_proc(opfamily,
											   opckeytype, opckeytype,
											   GIST_ORDER_PROC);

		if (!OidIsValid(procid))
			return NULL;
	}

	return btbuild_internal(buildstate->heaprel, index, indexInfo,
							GIST_ORDER_PROC, GIST_SORTSUPP_PROC,
							gistCompressAttr, buildstate->giststate,
							gistLeafBuild, buildstate,
							"postgres", "gist_parallel_build_main");
}

/*
 * Main entry point to GiST index build. Initially calls insert over and over,
 * but switches to more efficient buffering build algorithm after a certain
 * number of tuples (unless buffering mode is disabled).
 */
IndexBuildResult *
gistbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	double		reltuples;
	GISTBuildState buildstate;
	Buffer		buffer;
	Page		page;
	MemoryContext oldcxt = CurrentMemoryContext;
	int			fillfactor;

	buildstate.indexrel = index;
	buildstate.heaprel = heap;
	if (index->rd_options)
	{
		/* Get buffering mode from the options string */
		GiSTOptions *options = (GiSTOptions *) index->rd_options;
		char	   *bufferingMode = (char *) options + options->bufferingModeOffset;

		if (strcmp(bufferingMode, "on") == 0)
			buildstate.bufferingMode = GIST_BUFFERING_STATS;
		else if (strcmp(bufferingMode, "off") == 0)
			buildstate.bufferingMode = GIST_BUFFERING_DISABLED;
		else
			buildstate.bufferingMode = GIST_BUFFERING_AUTO;

		fillfactor = options->fillfactor;
	}
	else
	{
		/*
		 * By default, switch to buffering mode when the index grows too large
		 * to fit in cache.
		 */
		buildstate.bufferingMode = GIST_BUFFERING_AUTO;
		fillfactor = GIST_DEFAULT_FILLFACTOR;
	}
	/* Calculate target amount of free space to leave on pages */
	buildstate.freespace = BLCKSZ * (100 - fillfactor) / 100;

	/*
	 * We expect to be called exactly once for any index relation. If that's
	 * not the case, big trouble's what we have.
	 */
	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* no locking is needed */
	buildstate.giststate = initGISTstate(index);

	/*
	 * Create a temporary memory context that is reset once for each tuple
	 * processed.  (Note: we don't bother to make this a child of the
	 * giststate's scanCxt, so we have to delete it separately at the end.)
	 */
	buildstate.giststate->tempCxt = createTempGistContext();


	if ((result = gistTrySortedBuild(&buildstate, indexInfo)))
	{
		MemoryContextDelete(buildstate.giststate->tempCxt);
		freeGISTstate(buildstate.giststate);

		return result;
	}

	/* initialize the root page */
	buffer = gistNewBuffer(index);
	Assert(BufferGetBlockNumber(buffer) == GIST_ROOT_BLKNO);
	page = BufferGetPage(buffer);

	START_CRIT_SECTION();

	GISTInitBuffer(buffer, F_LEAF);

	MarkBufferDirty(buffer);

	if (RelationNeedsWAL(index))
	{
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buffer, REGBUF_WILL_INIT);

		recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_CREATE_INDEX);
		PageSetLSN(page, recptr);
	}
	else
		PageSetLSN(page, gistGetFakeLSN(heap));

	UnlockReleaseBuffer(buffer);

	END_CRIT_SECTION();

	/* build the index */
	buildstate.indtuples = 0;
	buildstate.indtuplesSize = 0;

	/*
	 * Do the heap scan.
	 */
	reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
								   gistBuildCallback, (void *) &buildstate, NULL);

	/*
	 * If buffering was used, flush out all the tuples that are still in the
	 * buffers.
	 */
	if (buildstate.bufferingMode == GIST_BUFFERING_ACTIVE)
	{
		elog(DEBUG1, "all tuples processed, emptying buffers");
		gistEmptyAllBuffers(&buildstate);
		gistFreeBuildBuffers(buildstate.gfbb);
	}

	/* okay, all heap tuples are indexed */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(buildstate.giststate->tempCxt);

	freeGISTstate(buildstate.giststate);

	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = (double) buildstate.indtuples;

	return result;
}

/*
 * Validator for "buffering" reloption on GiST indexes. Allows "on", "off"
 * and "auto" values.
 */
void
gistValidateBufferingOption(const char *value)
{
	if (value == NULL ||
		(strcmp(value, "on") != 0 &&
		 strcmp(value, "off") != 0 &&
		 strcmp(value, "auto") != 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for \"buffering\" option"),
				 errdetail("Valid values are \"on\", \"off\", and \"auto\".")));
	}
}

/*
 * Attempt to switch to buffering mode.
 *
 * If there is not enough memory for buffering build, sets bufferingMode
 * to GIST_BUFFERING_DISABLED, so that we don't bother to try the switch
 * anymore. Otherwise initializes the build buffers, and sets bufferingMode to
 * GIST_BUFFERING_ACTIVE.
 */
static void
gistInitBuffering(GISTBuildState *buildstate)
{
	Relation	index = buildstate->indexrel;
	int			pagesPerBuffer;
	Size		pageFreeSpace;
	Size		itupAvgSize,
				itupMinSize;
	double		avgIndexTuplesPerPage,
				maxIndexTuplesPerPage;
	int			i;
	int			levelStep;

	/* Calc space of index page which is available for index tuples */
	pageFreeSpace = BLCKSZ - SizeOfPageHeaderData - sizeof(GISTPageOpaqueData)
		- sizeof(ItemIdData)
		- buildstate->freespace;

	/*
	 * Calculate average size of already inserted index tuples using gathered
	 * statistics.
	 */
	itupAvgSize = (double) buildstate->indtuplesSize /
		(double) buildstate->indtuples;

	/*
	 * Calculate minimal possible size of index tuple by index metadata.
	 * Minimal possible size of varlena is VARHDRSZ.
	 *
	 * XXX: that's not actually true, as a short varlen can be just 2 bytes.
	 * And we should take padding into account here.
	 */
	itupMinSize = (Size) MAXALIGN(sizeof(IndexTupleData));
	for (i = 0; i < index->rd_att->natts; i++)
	{
		if (TupleDescAttr(index->rd_att, i)->attlen < 0)
			itupMinSize += VARHDRSZ;
		else
			itupMinSize += TupleDescAttr(index->rd_att, i)->attlen;
	}

	/* Calculate average and maximal number of index tuples which fit to page */
	avgIndexTuplesPerPage = pageFreeSpace / itupAvgSize;
	maxIndexTuplesPerPage = pageFreeSpace / itupMinSize;

	/*
	 * We need to calculate two parameters for the buffering algorithm:
	 * levelStep and pagesPerBuffer.
	 *
	 * levelStep determines the size of subtree that we operate on, while
	 * emptying a buffer. A higher value is better, as you need fewer buffer
	 * emptying steps to build the index. However, if you set it too high, the
	 * subtree doesn't fit in cache anymore, and you quickly lose the benefit
	 * of the buffers.
	 *
	 * In Arge et al's paper, levelStep is chosen as logB(M/4B), where B is
	 * the number of tuples on page (ie. fanout), and M is the amount of
	 * internal memory available. Curiously, they doesn't explain *why* that
	 * setting is optimal. We calculate it by taking the highest levelStep so
	 * that a subtree still fits in cache. For a small B, our way of
	 * calculating levelStep is very close to Arge et al's formula. For a
	 * large B, our formula gives a value that is 2x higher.
	 *
	 * The average size (in pages) of a subtree of depth n can be calculated
	 * as a geometric series:
	 *
	 * B^0 + B^1 + B^2 + ... + B^n = (1 - B^(n + 1)) / (1 - B)
	 *
	 * where B is the average number of index tuples on page. The subtree is
	 * cached in the shared buffer cache and the OS cache, so we choose
	 * levelStep so that the subtree size is comfortably smaller than
	 * effective_cache_size, with a safety factor of 4.
	 *
	 * The estimate on the average number of index tuples on page is based on
	 * average tuple sizes observed before switching to buffered build, so the
	 * real subtree size can be somewhat larger. Also, it would selfish to
	 * gobble the whole cache for our index build. The safety factor of 4
	 * should account for those effects.
	 *
	 * The other limiting factor for setting levelStep is that while
	 * processing a subtree, we need to hold one page for each buffer at the
	 * next lower buffered level. The max. number of buffers needed for that
	 * is maxIndexTuplesPerPage^levelStep. This is very conservative, but
	 * hopefully maintenance_work_mem is set high enough that you're
	 * constrained by effective_cache_size rather than maintenance_work_mem.
	 *
	 * XXX: the buffer hash table consumes a fair amount of memory too per
	 * buffer, but that is not currently taken into account. That scales on
	 * the total number of buffers used, ie. the index size and on levelStep.
	 * Note that a higher levelStep *reduces* the amount of memory needed for
	 * the hash table.
	 */
	levelStep = 1;
	for (;;)
	{
		double		subtreesize;
		double		maxlowestlevelpages;

		/* size of an average subtree at this levelStep (in pages). */
		subtreesize =
			(1 - pow(avgIndexTuplesPerPage, (double) (levelStep + 1))) /
			(1 - avgIndexTuplesPerPage);

		/* max number of pages at the lowest level of a subtree */
		maxlowestlevelpages = pow(maxIndexTuplesPerPage, (double) levelStep);

		/* subtree must fit in cache (with safety factor of 4) */
		if (subtreesize > effective_cache_size / 4)
			break;

		/* each node in the lowest level of a subtree has one page in memory */
		if (maxlowestlevelpages > ((double) maintenance_work_mem * 1024) / BLCKSZ)
			break;

		/* Good, we can handle this levelStep. See if we can go one higher. */
		levelStep++;
	}

	/*
	 * We just reached an unacceptable value of levelStep in previous loop.
	 * So, decrease levelStep to get last acceptable value.
	 */
	levelStep--;

	/*
	 * If there's not enough cache or maintenance_work_mem, fall back to plain
	 * inserts.
	 */
	if (levelStep <= 0)
	{
		elog(DEBUG1, "failed to switch to buffered GiST build");
		buildstate->bufferingMode = GIST_BUFFERING_DISABLED;
		return;
	}

	/*
	 * The second parameter to set is pagesPerBuffer, which determines the
	 * size of each buffer. We adjust pagesPerBuffer also during the build,
	 * which is why this calculation is in a separate function.
	 */
	pagesPerBuffer = calculatePagesPerBuffer(buildstate, levelStep);

	/* Initialize GISTBuildBuffers with these parameters */
	buildstate->gfbb = gistInitBuildBuffers(pagesPerBuffer, levelStep,
											gistGetMaxLevel(index));

	gistInitParentMap(buildstate);

	buildstate->bufferingMode = GIST_BUFFERING_ACTIVE;

	elog(DEBUG1, "switched to buffered GiST build; level step = %d, pagesPerBuffer = %d",
		 levelStep, pagesPerBuffer);
}

/*
 * Calculate pagesPerBuffer parameter for the buffering algorithm.
 *
 * Buffer size is chosen so that assuming that tuples are distributed
 * randomly, emptying half a buffer fills on average one page in every buffer
 * at the next lower level.
 */
static int
calculatePagesPerBuffer(GISTBuildState *buildstate, int levelStep)
{
	double		pagesPerBuffer;
	double		avgIndexTuplesPerPage;
	double		itupAvgSize;
	Size		pageFreeSpace;

	/* Calc space of index page which is available for index tuples */
	pageFreeSpace = BLCKSZ - SizeOfPageHeaderData - sizeof(GISTPageOpaqueData)
		- sizeof(ItemIdData)
		- buildstate->freespace;

	/*
	 * Calculate average size of already inserted index tuples using gathered
	 * statistics.
	 */
	itupAvgSize = (double) buildstate->indtuplesSize /
		(double) buildstate->indtuples;

	avgIndexTuplesPerPage = pageFreeSpace / itupAvgSize;

	/*
	 * Recalculate required size of buffers.
	 */
	pagesPerBuffer = 2 * pow(avgIndexTuplesPerPage, levelStep);

	return (int) rint(pagesPerBuffer);
}

/*
 * Per-tuple callback from IndexBuildHeapScan.
 */
static void
gistBuildCallback(Relation index,
				  HeapTuple htup,
				  Datum *values,
				  bool *isnull,
				  bool tupleIsAlive,
				  void *state)
{
	GISTBuildState *buildstate = (GISTBuildState *) state;
	IndexTuple	itup;
	MemoryContext oldCtx;

	oldCtx = MemoryContextSwitchTo(buildstate->giststate->tempCxt);

	/* form an index tuple and point it at the heap tuple */
	itup = gistFormTuple(buildstate->giststate, index, values, isnull, true);
	itup->t_tid = htup->t_self;

	if (buildstate->bufferingMode == GIST_BUFFERING_ACTIVE)
	{
		/* We have buffers, so use them. */
		gistBufferingBuildInsert(buildstate, itup);
	}
	else
	{
		/*
		 * There's no buffers (yet). Since we already have the index relation
		 * locked, we call gistdoinsert directly.
		 */
		gistdoinsert(index, itup, buildstate->freespace,
					 buildstate->giststate, buildstate->heaprel);
	}

	/* Update tuple count and total size. */
	buildstate->indtuples += 1;
	buildstate->indtuplesSize += IndexTupleSize(itup);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->giststate->tempCxt);

	if (buildstate->bufferingMode == GIST_BUFFERING_ACTIVE &&
		buildstate->indtuples % BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0)
	{
		/* Adjust the target buffer size now */
		buildstate->gfbb->pagesPerBuffer =
			calculatePagesPerBuffer(buildstate, buildstate->gfbb->levelStep);
	}

	/*
	 * In 'auto' mode, check if the index has grown too large to fit in cache,
	 * and switch to buffering mode if it has.
	 *
	 * To avoid excessive calls to smgrnblocks(), only check this every
	 * BUFFERING_MODE_SWITCH_CHECK_STEP index tuples
	 */
	if ((buildstate->bufferingMode == GIST_BUFFERING_AUTO &&
		 buildstate->indtuples % BUFFERING_MODE_SWITCH_CHECK_STEP == 0 &&
		 effective_cache_size < smgrnblocks(index->rd_smgr, MAIN_FORKNUM)) ||
		(buildstate->bufferingMode == GIST_BUFFERING_STATS &&
		 buildstate->indtuples >= BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET))
	{
		/*
		 * Index doesn't fit in effective cache anymore. Try to switch to
		 * buffering build mode.
		 */
		gistInitBuffering(buildstate);
	}
}

/*
 * Insert function for buffering index build.
 */
static void
gistBufferingBuildInsert(GISTBuildState *buildstate, IndexTuple itup)
{
	/* Insert the tuple to buffers. */
	gistProcessItup(buildstate, itup, 0, buildstate->gfbb->rootlevel);

	/* If we filled up (half of a) buffer, process buffer emptying. */
	gistProcessEmptyingQueue(buildstate);
}

/*
 * Process an index tuple. Runs the tuple down the tree until we reach a leaf
 * page or node buffer, and inserts the tuple there. Returns true if we have
 * to stop buffer emptying process (because one of child buffers can't take
 * index tuples anymore).
 */
static bool
gistProcessItup(GISTBuildState *buildstate, IndexTuple itup,
				BlockNumber startblkno, int startlevel)
{
	GISTSTATE  *giststate = buildstate->giststate;
	GISTBuildBuffers *gfbb = buildstate->gfbb;
	Relation	indexrel = buildstate->indexrel;
	BlockNumber childblkno;
	Buffer		buffer;
	bool		result = false;
	BlockNumber blkno;
	int			level;
	OffsetNumber downlinkoffnum = InvalidOffsetNumber;
	BlockNumber parentblkno = InvalidBlockNumber;

	CHECK_FOR_INTERRUPTS();

	/*
	 * Loop until we reach a leaf page (level == 0) or a level with buffers
	 * (not including the level we start at, because we would otherwise make
	 * no progress).
	 */
	blkno = startblkno;
	level = startlevel;
	for (;;)
	{
		ItemId		iid;
		IndexTuple	idxtuple,
					newtup;
		Page		page;
		OffsetNumber childoffnum;

		/* Have we reached a level with buffers? */
		if (LEVEL_HAS_BUFFERS(level, gfbb) && level != startlevel)
			break;

		/* Have we reached a leaf page? */
		if (level == 0)
			break;

		/*
		 * Nope. Descend down to the next level then. Choose a child to
		 * descend down to.
		 */

		buffer = ReadBuffer(indexrel, blkno);
		LockBuffer(buffer, GIST_EXCLUSIVE);

		page = (Page) BufferGetPage(buffer);
		childoffnum = gistchoose(indexrel, page, itup, giststate);
		iid = PageGetItemId(page, childoffnum);
		idxtuple = (IndexTuple) PageGetItem(page, iid);
		childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

		if (level > 1)
			gistMemorizeParent(buildstate, childblkno, blkno);

		/*
		 * Check that the key representing the target child node is consistent
		 * with the key we're inserting. Update it if it's not.
		 */
		newtup = gistgetadjusted(indexrel, idxtuple, itup, giststate);
		if (newtup)
		{
			blkno = gistbufferinginserttuples(buildstate,
											  buffer,
											  level,
											  &newtup,
											  1,
											  childoffnum,
											  InvalidBlockNumber,
											  InvalidOffsetNumber);
			/* gistbufferinginserttuples() released the buffer */
		}
		else
			UnlockReleaseBuffer(buffer);

		/* Descend to the child */
		parentblkno = blkno;
		blkno = childblkno;
		downlinkoffnum = childoffnum;
		Assert(level > 0);
		level--;
	}

	if (LEVEL_HAS_BUFFERS(level, gfbb))
	{
		/*
		 * We've reached level with buffers. Place the index tuple to the
		 * buffer, and add the buffer to the emptying queue if it overflows.
		 */
		GISTNodeBuffer *childNodeBuffer;

		/* Find the buffer or create a new one */
		childNodeBuffer = gistGetNodeBuffer(gfbb, giststate, blkno, level);

		/* Add index tuple to it */
		gistPushItupToNodeBuffer(gfbb, childNodeBuffer, itup);

		if (BUFFER_OVERFLOWED(childNodeBuffer, gfbb))
			result = true;
	}
	else
	{
		/*
		 * We've reached a leaf page. Place the tuple here.
		 */
		Assert(level == 0);
		buffer = ReadBuffer(indexrel, blkno);
		LockBuffer(buffer, GIST_EXCLUSIVE);
		gistbufferinginserttuples(buildstate, buffer, level,
								  &itup, 1, InvalidOffsetNumber,
								  parentblkno, downlinkoffnum);
		/* gistbufferinginserttuples() released the buffer */
	}

	return result;
}

/*
 * Insert tuples to a given page.
 *
 * This is analogous with gistinserttuples() in the regular insertion code.
 *
 * Returns the block number of the page where the (first) new or updated tuple
 * was inserted. Usually that's the original page, but might be a sibling page
 * if the original page was split.
 *
 * Caller should hold a lock on 'buffer' on entry. This function will unlock
 * and unpin it.
 */
static BlockNumber
gistbufferinginserttuples(GISTBuildState *buildstate, Buffer buffer, int level,
						  IndexTuple *itup, int ntup, OffsetNumber oldoffnum,
						  BlockNumber parentblk, OffsetNumber downlinkoffnum)
{
	GISTBuildBuffers *gfbb = buildstate->gfbb;
	List	   *splitinfo;
	bool		is_split;
	BlockNumber placed_to_blk = InvalidBlockNumber;

	is_split = gistplacetopage(buildstate->indexrel,
							   buildstate->freespace,
							   buildstate->giststate,
							   buffer,
							   itup, ntup, oldoffnum, &placed_to_blk,
							   InvalidBuffer,
							   &splitinfo,
							   false,
							   buildstate->heaprel);

	/*
	 * If this is a root split, update the root path item kept in memory. This
	 * ensures that all path stacks are always complete, including all parent
	 * nodes up to the root. That simplifies the algorithm to re-find correct
	 * parent.
	 */
	if (is_split && BufferGetBlockNumber(buffer) == GIST_ROOT_BLKNO)
	{
		Page		page = BufferGetPage(buffer);
		OffsetNumber off;
		OffsetNumber maxoff;

		Assert(level == gfbb->rootlevel);
		gfbb->rootlevel++;

		elog(DEBUG2, "splitting GiST root page, now %d levels deep", gfbb->rootlevel);

		/*
		 * All the downlinks on the old root page are now on one of the child
		 * pages. Visit all the new child pages to memorize the parents of the
		 * grandchildren.
		 */
		if (gfbb->rootlevel > 1)
		{
			maxoff = PageGetMaxOffsetNumber(page);
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		iid = PageGetItemId(page, off);
				IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
				BlockNumber childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				Buffer		childbuf = ReadBuffer(buildstate->indexrel, childblkno);

				LockBuffer(childbuf, GIST_SHARE);
				gistMemorizeAllDownlinks(buildstate, childbuf);
				UnlockReleaseBuffer(childbuf);

				/*
				 * Also remember that the parent of the new child page is the
				 * root block.
				 */
				gistMemorizeParent(buildstate, childblkno, GIST_ROOT_BLKNO);
			}
		}
	}

	if (splitinfo)
	{
		/*
		 * Insert the downlinks to the parent. This is analogous with
		 * gistfinishsplit() in the regular insertion code, but the locking is
		 * simpler, and we have to maintain the buffers on internal nodes and
		 * the parent map.
		 */
		IndexTuple *downlinks;
		int			ndownlinks,
					i;
		Buffer		parentBuffer;
		ListCell   *lc;

		/* Parent may have changed since we memorized this path. */
		parentBuffer =
			gistBufferingFindCorrectParent(buildstate,
										   BufferGetBlockNumber(buffer),
										   level,
										   &parentblk,
										   &downlinkoffnum);

		/*
		 * If there's a buffer associated with this page, that needs to be
		 * split too. gistRelocateBuildBuffersOnSplit() will also adjust the
		 * downlinks in 'splitinfo', to make sure they're consistent not only
		 * with the tuples already on the pages, but also the tuples in the
		 * buffers that will eventually be inserted to them.
		 */
		gistRelocateBuildBuffersOnSplit(gfbb,
										buildstate->giststate,
										buildstate->indexrel,
										level,
										buffer, splitinfo);

		/* Create an array of all the downlink tuples */
		ndownlinks = list_length(splitinfo);
		downlinks = (IndexTuple *) palloc(sizeof(IndexTuple) * ndownlinks);
		i = 0;
		foreach(lc, splitinfo)
		{
			GISTPageSplitInfo *splitinfo = lfirst(lc);

			/*
			 * Remember the parent of each new child page in our parent map.
			 * This assumes that the downlinks fit on the parent page. If the
			 * parent page is split, too, when we recurse up to insert the
			 * downlinks, the recursive gistbufferinginserttuples() call will
			 * update the map again.
			 */
			if (level > 0)
				gistMemorizeParent(buildstate,
								   BufferGetBlockNumber(splitinfo->buf),
								   BufferGetBlockNumber(parentBuffer));

			/*
			 * Also update the parent map for all the downlinks that got moved
			 * to a different page. (actually this also loops through the
			 * downlinks that stayed on the original page, but it does no
			 * harm).
			 */
			if (level > 1)
				gistMemorizeAllDownlinks(buildstate, splitinfo->buf);

			/*
			 * Since there's no concurrent access, we can release the lower
			 * level buffers immediately. This includes the original page.
			 */
			UnlockReleaseBuffer(splitinfo->buf);
			downlinks[i++] = splitinfo->downlink;
		}

		/* Insert them into parent. */
		gistbufferinginserttuples(buildstate, parentBuffer, level + 1,
								  downlinks, ndownlinks, downlinkoffnum,
								  InvalidBlockNumber, InvalidOffsetNumber);

		list_free_deep(splitinfo);	/* we don't need this anymore */
	}
	else
		UnlockReleaseBuffer(buffer);

	return placed_to_blk;
}

/*
 * Find the downlink pointing to a child page.
 *
 * 'childblkno' indicates the child page to find the parent for. 'level' is
 * the level of the child. On entry, *parentblkno and *downlinkoffnum can
 * point to a location where the downlink used to be - we will check that
 * location first, and save some cycles if it hasn't moved. The function
 * returns a buffer containing the downlink, exclusively-locked, and
 * *parentblkno and *downlinkoffnum are set to the real location of the
 * downlink.
 *
 * If the child page is a leaf (level == 0), the caller must supply a correct
 * parentblkno. Otherwise we use the parent map hash table to find the parent
 * block.
 *
 * This function serves the same purpose as gistFindCorrectParent() during
 * normal index inserts, but this is simpler because we don't need to deal
 * with concurrent inserts.
 */
static Buffer
gistBufferingFindCorrectParent(GISTBuildState *buildstate,
							   BlockNumber childblkno, int level,
							   BlockNumber *parentblkno,
							   OffsetNumber *downlinkoffnum)
{
	BlockNumber parent;
	Buffer		buffer;
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber off;

	if (level > 0)
		parent = gistGetParent(buildstate, childblkno);
	else
	{
		/*
		 * For a leaf page, the caller must supply a correct parent block
		 * number.
		 */
		if (*parentblkno == InvalidBlockNumber)
			elog(ERROR, "no parent buffer provided of child %d", childblkno);
		parent = *parentblkno;
	}

	buffer = ReadBuffer(buildstate->indexrel, parent);
	page = BufferGetPage(buffer);
	LockBuffer(buffer, GIST_EXCLUSIVE);
	gistcheckpage(buildstate->indexrel, buffer);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Check if it was not moved */
	if (parent == *parentblkno && *parentblkno != InvalidBlockNumber &&
		*downlinkoffnum != InvalidOffsetNumber && *downlinkoffnum <= maxoff)
	{
		ItemId		iid = PageGetItemId(page, *downlinkoffnum);
		IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

		if (ItemPointerGetBlockNumber(&(idxtuple->t_tid)) == childblkno)
		{
			/* Still there */
			return buffer;
		}
	}

	/*
	 * Downlink was not at the offset where it used to be. Scan the page to
	 * find it. During normal gist insertions, it might've moved to another
	 * page, to the right, but during a buffering build, we keep track of the
	 * parent of each page in the lookup table so we should always know what
	 * page it's on.
	 */
	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
	{
		ItemId		iid = PageGetItemId(page, off);
		IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

		if (ItemPointerGetBlockNumber(&(idxtuple->t_tid)) == childblkno)
		{
			/* yes!!, found it */
			*downlinkoffnum = off;
			return buffer;
		}
	}

	elog(ERROR, "failed to re-find parent for block %u", childblkno);
	return InvalidBuffer;		/* keep compiler quiet */
}

/*
 * Process buffers emptying stack. Emptying of one buffer can cause emptying
 * of other buffers. This function iterates until this cascading emptying
 * process finished, e.g. until buffers emptying stack is empty.
 */
static void
gistProcessEmptyingQueue(GISTBuildState *buildstate)
{
	GISTBuildBuffers *gfbb = buildstate->gfbb;

	/* Iterate while we have elements in buffers emptying stack. */
	while (gfbb->bufferEmptyingQueue != NIL)
	{
		GISTNodeBuffer *emptyingNodeBuffer;

		/* Get node buffer from emptying stack. */
		emptyingNodeBuffer = (GISTNodeBuffer *) linitial(gfbb->bufferEmptyingQueue);
		gfbb->bufferEmptyingQueue = list_delete_first(gfbb->bufferEmptyingQueue);
		emptyingNodeBuffer->queuedForEmptying = false;

		/*
		 * We are going to load last pages of buffers where emptying will be
		 * to. So let's unload any previously loaded buffers.
		 */
		gistUnloadNodeBuffers(gfbb);

		/*
		 * Pop tuples from the buffer and run them down to the buffers at
		 * lower level, or leaf pages. We continue until one of the lower
		 * level buffers fills up, or this buffer runs empty.
		 *
		 * In Arge et al's paper, the buffer emptying is stopped after
		 * processing 1/2 node buffer worth of tuples, to avoid overfilling
		 * any of the lower level buffers. However, it's more efficient to
		 * keep going until one of the lower level buffers actually fills up,
		 * so that's what we do. This doesn't need to be exact, if a buffer
		 * overfills by a few tuples, there's no harm done.
		 */
		while (true)
		{
			IndexTuple	itup;

			/* Get next index tuple from the buffer */
			if (!gistPopItupFromNodeBuffer(gfbb, emptyingNodeBuffer, &itup))
				break;

			/*
			 * Run it down to the underlying node buffer or leaf page.
			 *
			 * Note: it's possible that the buffer we're emptying splits as a
			 * result of this call. If that happens, our emptyingNodeBuffer
			 * points to the left half of the split. After split, it's very
			 * likely that the new left buffer is no longer over the half-full
			 * threshold, but we might as well keep flushing tuples from it
			 * until we fill a lower-level buffer.
			 */
			if (gistProcessItup(buildstate, itup, emptyingNodeBuffer->nodeBlocknum, emptyingNodeBuffer->level))
			{
				/*
				 * A lower level buffer filled up. Stop emptying this buffer,
				 * to avoid overflowing the lower level buffer.
				 */
				break;
			}

			/* Free all the memory allocated during index tuple processing */
			MemoryContextReset(buildstate->giststate->tempCxt);
		}
	}
}

/*
 * Empty all node buffers, from top to bottom. This is done at the end of
 * index build to flush all remaining tuples to the index.
 *
 * Note: This destroys the buffersOnLevels lists, so the buffers should not
 * be inserted to after this call.
 */
static void
gistEmptyAllBuffers(GISTBuildState *buildstate)
{
	GISTBuildBuffers *gfbb = buildstate->gfbb;
	MemoryContext oldCtx;
	int			i;

	oldCtx = MemoryContextSwitchTo(buildstate->giststate->tempCxt);

	/*
	 * Iterate through the levels from top to bottom.
	 */
	for (i = gfbb->buffersOnLevelsLen - 1; i >= 0; i--)
	{
		/*
		 * Empty all buffers on this level. Note that new buffers can pop up
		 * in the list during the processing, as a result of page splits, so a
		 * simple walk through the list won't work. We remove buffers from the
		 * list when we see them empty; a buffer can't become non-empty once
		 * it's been fully emptied.
		 */
		while (gfbb->buffersOnLevels[i] != NIL)
		{
			GISTNodeBuffer *nodeBuffer;

			nodeBuffer = (GISTNodeBuffer *) linitial(gfbb->buffersOnLevels[i]);

			if (nodeBuffer->blocksCount != 0)
			{
				/*
				 * Add this buffer to the emptying queue, and proceed to empty
				 * the queue.
				 */
				if (!nodeBuffer->queuedForEmptying)
				{
					MemoryContextSwitchTo(gfbb->context);
					nodeBuffer->queuedForEmptying = true;
					gfbb->bufferEmptyingQueue =
						lcons(nodeBuffer, gfbb->bufferEmptyingQueue);
					MemoryContextSwitchTo(buildstate->giststate->tempCxt);
				}
				gistProcessEmptyingQueue(buildstate);
			}
			else
				gfbb->buffersOnLevels[i] =
					list_delete_first(gfbb->buffersOnLevels[i]);
		}
		elog(DEBUG2, "emptied all buffers at level %d", i);
	}
	MemoryContextSwitchTo(oldCtx);
}

/*
 * Get the depth of the GiST index.
 */
static int
gistGetMaxLevel(Relation index)
{
	int			maxLevel;
	BlockNumber blkno;

	/*
	 * Traverse down the tree, starting from the root, until we hit the leaf
	 * level.
	 */
	maxLevel = 0;
	blkno = GIST_ROOT_BLKNO;
	while (true)
	{
		Buffer		buffer;
		Page		page;
		IndexTuple	itup;

		buffer = ReadBuffer(index, blkno);

		/*
		 * There's no concurrent access during index build, so locking is just
		 * pro forma.
		 */
		LockBuffer(buffer, GIST_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (GistPageIsLeaf(page))
		{
			/* We hit the bottom, so we're done. */
			UnlockReleaseBuffer(buffer);
			break;
		}

		/*
		 * Pick the first downlink on the page, and follow it. It doesn't
		 * matter which downlink we choose, the tree has the same depth
		 * everywhere, so we just pick the first one.
		 */
		itup = (IndexTuple) PageGetItem(page,
										PageGetItemId(page, FirstOffsetNumber));
		blkno = ItemPointerGetBlockNumber(&(itup->t_tid));
		UnlockReleaseBuffer(buffer);

		/*
		 * We're going down on the tree. It means that there is yet one more
		 * level in the tree.
		 */
		maxLevel++;
	}
	return maxLevel;
}


/*
 * Routines for managing the parent map.
 *
 * Whenever a page is split, we need to insert the downlinks into the parent.
 * We need to somehow find the parent page to do that. In normal insertions,
 * we keep a stack of nodes visited when we descend the tree. However, in
 * buffering build, we can start descending the tree from any internal node,
 * when we empty a buffer by cascading tuples to its children. So we don't
 * have a full stack up to the root available at that time.
 *
 * So instead, we maintain a hash table to track the parent of every internal
 * page. We don't need to track the parents of leaf nodes, however. Whenever
 * we insert to a leaf, we've just descended down from its parent, so we know
 * its immediate parent already. This helps a lot to limit the memory used
 * by this hash table.
 *
 * Whenever an internal node is split, the parent map needs to be updated.
 * the parent of the new child page needs to be recorded, and also the
 * entries for all page whose downlinks are moved to a new page at the split
 * needs to be updated.
 *
 * We also update the parent map whenever we descend the tree. That might seem
 * unnecessary, because we maintain the map whenever a downlink is moved or
 * created, but it is needed because we switch to buffering mode after
 * creating a tree with regular index inserts. Any pages created before
 * switching to buffering mode will not be present in the parent map initially,
 * but will be added there the first time we visit them.
 */

typedef struct
{
	BlockNumber childblkno;		/* hash key */
	BlockNumber parentblkno;
} ParentMapEntry;

static void
gistInitParentMap(GISTBuildState *buildstate)
{
	HASHCTL		hashCtl;

	hashCtl.keysize = sizeof(BlockNumber);
	hashCtl.entrysize = sizeof(ParentMapEntry);
	hashCtl.hcxt = CurrentMemoryContext;
	buildstate->parentMap = hash_create("gistbuild parent map",
										1024,
										&hashCtl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
gistMemorizeParent(GISTBuildState *buildstate, BlockNumber child, BlockNumber parent)
{
	ParentMapEntry *entry;
	bool		found;

	entry = (ParentMapEntry *) hash_search(buildstate->parentMap,
										   (const void *) &child,
										   HASH_ENTER,
										   &found);
	entry->parentblkno = parent;
}

/*
 * Scan all downlinks on a page, and memorize their parent.
 */
static void
gistMemorizeAllDownlinks(GISTBuildState *buildstate, Buffer parentbuf)
{
	OffsetNumber maxoff;
	OffsetNumber off;
	BlockNumber parentblkno = BufferGetBlockNumber(parentbuf);
	Page		page = BufferGetPage(parentbuf);

	Assert(!GistPageIsLeaf(page));

	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(page, off);
		IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
		BlockNumber childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

		gistMemorizeParent(buildstate, childblkno, parentblkno);
	}
}

static BlockNumber
gistGetParent(GISTBuildState *buildstate, BlockNumber child)
{
	ParentMapEntry *entry;
	bool		found;

	/* Find node buffer in hash table */
	entry = (ParentMapEntry *) hash_search(buildstate->parentMap,
										   (const void *) &child,
										   HASH_FIND,
										   &found);
	if (!found)
		elog(ERROR, "could not find parent of block %d in lookup table", child);

	return entry->parentblkno;
}
