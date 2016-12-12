/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "memtx_space.h"
#include "space.h"
#include "iproto_constants.h"
#include "txn.h"
#include "tuple.h"
#include "xrow.h"
#include "memtx_hash.h"
#include "memtx_tree.h"
#include "memtx_rtree.h"
#include "memtx_bitset.h"
#include "port.h"

enum {
	/**
	 * This number is calculated based on the
	 * max (realistic) number of insertions
	 * a deletion from a B-tree or an R-tree
	 * can lead to, and, as a result, the max
	 * number of new block allocations.
	 */
	RESERVE_EXTENTS_BEFORE_DELETE = 8,
	RESERVE_EXTENTS_BEFORE_REPLACE = 16
};

typedef struct tuple *
(*memtx_replace_f)(struct space *, struct tuple *, struct tuple *,
		    enum dup_replace_mode);

static inline enum dup_replace_mode
dup_replace_mode(uint32_t op)
{
	return op == IPROTO_INSERT ? DUP_INSERT : DUP_REPLACE_OR_INSERT;
}

/**
 * Do the plumbing necessary for correct statement-level
 * and transaction rollback.
 */
static inline void
memtx_txn_add_undo(struct txn *txn, struct tuple *old_tuple,
		   struct tuple *new_tuple)
{
	/*
	 * Remember the old tuple only if we replaced it
	 * successfully, to not remove a tuple inserted by
	 * another transaction in rollback().
	 */
	struct txn_stmt *stmt = txn_current_stmt(txn);
	assert(stmt->space);
	stmt->old_tuple = old_tuple;
	stmt->new_tuple = new_tuple;
}

/**
 * A short-cut version of replace() used during bulk load
 * from snapshot.
 */
static struct tuple *
memtx_replace_build_next(struct space *space,
			 struct tuple *old_tuple, struct tuple *new_tuple,
			 enum dup_replace_mode mode)
{
	assert(old_tuple == NULL && mode == DUP_INSERT);
	(void) mode;
	if (old_tuple) {
		/*
		 * Called from txn_rollback() In practice
		 * is impossible: all possible checks for tuple
		 * validity are done before the space is changed,
		 * and WAL is off, so this part can't fail.
		 */
		panic("Failed to commit transaction when loading "
		      "from snapshot");
	}
	((MemtxIndex *) space->index[0])->buildNext(new_tuple);
	tuple_ref(new_tuple);
	return NULL;
}

/**
 * A short-cut version of replace() used when loading
 * data from XLOG files.
 */
static struct tuple *
memtx_replace_primary_key(struct space *space, struct tuple *old_tuple,
			  struct tuple *new_tuple, enum dup_replace_mode mode)
{
	old_tuple = space->index[0]->replace(old_tuple, new_tuple, mode);
	if (new_tuple)
		tuple_ref(new_tuple);
	return old_tuple;
}

/**
 * @brief A single method to handle REPLACE, DELETE and UPDATE.
 *
 * @param sp space
 * @param old_tuple the tuple that should be removed (can be NULL)
 * @param new_tuple the tuple that should be inserted (can be NULL)
 * @param mode      dup_replace_mode, used only if new_tuple is not
 *                  NULL and old_tuple is NULL, and only for the
 *                  primary key.
 *
 * For DELETE, new_tuple must be NULL. old_tuple must be
 * previously found in the primary key.
 *
 * For REPLACE, old_tuple must be NULL. The additional
 * argument dup_replace_mode further defines how REPLACE
 * should proceed.
 *
 * For UPDATE, both old_tuple and new_tuple must be given,
 * where old_tuple must be previously found in the primary key.
 *
 * Let's consider these three cases in detail:
 *
 * 1. DELETE, old_tuple is not NULL, new_tuple is NULL
 *    The effect is that old_tuple is removed from all
 *    indexes. dup_replace_mode is ignored.
 *
 * 2. REPLACE, old_tuple is NULL, new_tuple is not NULL,
 *    has one simple sub-case and two with further
 *    ramifications:
 *
 *	A. dup_replace_mode is DUP_INSERT. Attempts to insert the
 *	new tuple into all indexes. If *any* of the unique indexes
 *	has a duplicate key, deletion is aborted, all of its
 *	effects are removed, and an error is thrown.
 *
 *	B. dup_replace_mode is DUP_REPLACE. It means an existing
 *	tuple has to be replaced with the new one. To do it, tries
 *	to find a tuple with a duplicate key in the primary index.
 *	If the tuple is not found, throws an error. Otherwise,
 *	replaces the old tuple with a new one in the primary key.
 *	Continues on to secondary keys, but if there is any
 *	secondary key, which has a duplicate tuple, but one which
 *	is different from the duplicate found in the primary key,
 *	aborts, puts everything back, throws an exception.
 *
 *	For example, if there is a space with 3 unique keys and
 *	two tuples { 1, 2, 3 } and { 3, 1, 2 }:
 *
 *	This REPLACE/DUP_REPLACE is OK: { 1, 5, 5 }
 *	This REPLACE/DUP_REPLACE is not OK: { 2, 2, 2 } (there
 *	is no tuple with key '2' in the primary key)
 *	This REPLACE/DUP_REPLACE is not OK: { 1, 1, 1 } (there
 *	is a conflicting tuple in the secondary unique key).
 *
 *	C. dup_replace_mode is DUP_REPLACE_OR_INSERT. If
 *	there is a duplicate tuple in the primary key, behaves the
 *	same way as DUP_REPLACE, otherwise behaves the same way as
 *	DUP_INSERT.
 *
 * 3. UPDATE has to delete the old tuple and insert a new one.
 *    dup_replace_mode is ignored.
 *    Note that old_tuple primary key doesn't have to match
 *    new_tuple primary key, thus a duplicate can be found.
 *    For this reason, and since there can be duplicates in
 *    other indexes, UPDATE is the same as DELETE +
 *    REPLACE/DUP_INSERT.
 *
 * @return old_tuple. DELETE, UPDATE and REPLACE/DUP_REPLACE
 * always produce an old tuple. REPLACE/DUP_INSERT always returns
 * NULL. REPLACE/DUP_REPLACE_OR_INSERT may or may not find
 * a duplicate.
 *
 * The method is all-or-nothing in all cases. Changes are either
 * applied to all indexes, or nothing applied at all.
 *
 * Note, that even in case of REPLACE, dup_replace_mode only
 * affects the primary key, for secondary keys it's always
 * DUP_INSERT.
 *
 * The call never removes more than one tuple: if
 * old_tuple is given, dup_replace_mode is ignored.
 * Otherwise, it's taken into account only for the
 * primary key.
 */
static struct tuple *
memtx_replace_all_keys(struct space *space, struct tuple *old_tuple,
		       struct tuple *new_tuple, enum dup_replace_mode mode)
{
	/*
	 * Ensure we have enough slack memory to guarantee
	 * successful statement-level rollback.
	 */
	memtx_index_extent_reserve(new_tuple ?
				   RESERVE_EXTENTS_BEFORE_REPLACE :
				   RESERVE_EXTENTS_BEFORE_DELETE);
	uint32_t i = 0;
	try {
		/* Update the primary key */
		Index *pk = index_find(space, 0);
		assert(pk->key_def->opts.is_unique);
		/*
		 * If old_tuple is not NULL, the index
		 * has to find and delete it, or raise an
		 * error.
		 */
		old_tuple = pk->replace(old_tuple, new_tuple, mode);

		assert(old_tuple || new_tuple);
		/* Update secondary keys. */
		for (i++; i < space->index_count; i++) {
			Index *index = space->index[i];
			index->replace(old_tuple, new_tuple, DUP_INSERT);
		}
	} catch (Exception *e) {
		/* Rollback all changes */
		for (; i > 0; i--) {
			Index *index = space->index[i-1];
			index->replace(new_tuple, old_tuple, DUP_INSERT);
		}
		throw;
	}
	if (new_tuple)
		tuple_ref(new_tuple);
	return old_tuple;
}

static memtx_replace_f memtx_replace[] = {
	memtx_replace_build_next,
	memtx_replace_primary_key,
	memtx_replace_all_keys,
};

MemtxSpace::MemtxSpace(MemtxEngine *e)
	: Handler(e)
{
	m_state = e->m_state;
}

void
MemtxSpace::applySnapshotRow(struct space *space, struct request *request)
{
	return applyWalRow(space, request);
}

void
MemtxSpace::applyWalRow(struct space *space, struct request *request)
{
	struct tuple *new_tuple = NULL;
	struct tuple *old_tuple = NULL;
	enum dup_replace_mode mode = dup_replace_mode(request->type);

	assert(request->index_id == 0);
	switch (request->type) {
	case IPROTO_INSERT:
	case IPROTO_REPLACE:
		new_tuple = tuple_new_xc(space->format, request->tuple,
					 request->tuple_end);
		break;
	case IPROTO_UPDATE:
	{
		/* Rebind to pk is already done when recovering from WAL. */
		assert(request->index_id == 0);
		const char *key = request->key;
		uint32_t part_count = mp_decode_array(&key);
		old_tuple = space->index[0]->findByKey(key, part_count);
		if (old_tuple == NULL)
			return;
		new_tuple = tuple_update(space->format,
					 region_aligned_alloc_xc_cb,
					 &fiber()->gc,
					 old_tuple, request->tuple,
					 request->tuple_end,
					 request->index_base, NULL);
		break;
	}
	case IPROTO_DELETE:
	{
		assert(request->index_id == 0);
		const char *key = request->key;
		uint32_t part_count = mp_decode_array(&key);
		old_tuple = space->index[0]->findByKey(key, part_count);
		if (old_tuple == NULL)
			return;
		break;
	}
	case IPROTO_UPSERT:
	{
		assert(request->index_id == 0);
		Index *pk = space->index[0];
		struct key_def *key_def = pk->key_def;
		uint32_t part_count = pk->key_def->part_count;
		const char *key = tuple_extract_key_raw(request->tuple,
							request->tuple_end,
							key_def, NULL);
		/* Cut array header */
		mp_decode_array(&key);
		/* Try to find the tuple by primary key. */
		old_tuple = pk->findByKey(key, part_count);
		if (old_tuple == NULL) {
			new_tuple = tuple_new_xc(space->format, request->tuple,
						 request->tuple_end);
		} else {
			new_tuple = tuple_upsert(space->format,
						 region_aligned_alloc_xc_cb,
						 &fiber()->gc, old_tuple,
						 request->ops, request->ops_end,
						 request->index_base);
		}
		break;
	}
	default:
		tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
			  (uint32_t) request->type);
		break;
	}
	TupleRefNil tuple_ref(new_tuple);
	struct txn *txn = NULL;
	try {
		if (!rlist_empty(&space->on_replace))
			txn = txn_begin_stmt(space);

		old_tuple = memtx_replace[m_state](space, old_tuple, new_tuple,
						   mode);
		if (txn) {
			memtx_txn_add_undo(txn, old_tuple, new_tuple);
			txn_commit_stmt(txn, request);
		} else {
			if (old_tuple)
				tuple_unref(old_tuple);
			fiber_gc();
		}
	} catch (ClientError *e) {
		txn ? txn_rollback_stmt() : fiber_gc();
		say_error("rollback: %s", e->errmsg);
		if (request->type != IPROTO_UPSERT)
			throw;
	} catch (Exception *e) {
		txn ? txn_rollback_stmt() : fiber_gc();
		throw;
	}
	/** The new tuple is referenced by the primary key. */
}

struct tuple *
MemtxSpace::executeReplace(struct txn *txn, struct space *space,
			   struct request *request)
{
	assert(m_state == MEMTX_OK);
	struct tuple *new_tuple = tuple_new_xc(space->format, request->tuple,
					       request->tuple_end);
	/* GC the new tuple if there is an exception below. */
	TupleRef ref(new_tuple);
	enum dup_replace_mode mode = dup_replace_mode(request->type);
	struct tuple *old_tuple = memtx_replace_all_keys(space, NULL, new_tuple, mode);
	memtx_txn_add_undo(txn, old_tuple, new_tuple);
	/** The new tuple is referenced by the primary key. */
	return new_tuple;
}

struct tuple *
MemtxSpace::executeDelete(struct txn *txn, struct space *space,
			  struct request *request)
{
	assert(m_state == MEMTX_OK);
	/* Try to find the tuple by unique key. */
	Index *pk = index_find_unique(space, request->index_id);
	const char *key = request->key;
	uint32_t part_count = mp_decode_array(&key);
	if (primary_key_validate(pk->key_def, key, part_count))
		diag_raise();
	struct tuple *old_tuple = pk->findByKey(key, part_count);
	if (old_tuple == NULL)
		return NULL;

	memtx_replace_all_keys(space, old_tuple, NULL, DUP_REPLACE_OR_INSERT);
	memtx_txn_add_undo(txn, old_tuple, NULL);
	return old_tuple;
}

struct tuple *
MemtxSpace::executeUpdate(struct txn *txn, struct space *space,
			  struct request *request)
{
	/* Try to find the tuple by unique key. */
	Index *pk = index_find_unique(space, request->index_id);
	const char *key = request->key;
	uint32_t part_count = mp_decode_array(&key);
	if (primary_key_validate(pk->key_def, key, part_count))
		diag_raise();
	struct tuple *old_tuple = pk->findByKey(key, part_count);

	if (old_tuple == NULL)
		return NULL;

	/* Update the tuple; legacy, request ops are in request->tuple */
	struct tuple *new_tuple = tuple_update(space->format,
					       region_aligned_alloc_xc_cb,
					       &fiber()->gc,
					       old_tuple, request->tuple,
					       request->tuple_end,
					       request->index_base, NULL);
	TupleRef ref(new_tuple);
	memtx_replace_all_keys(space, old_tuple, new_tuple, DUP_REPLACE);
	memtx_txn_add_undo(txn, old_tuple, new_tuple);
	return new_tuple;
}

void
MemtxSpace::executeUpsert(struct txn *txn, struct space *space,
			  struct request *request)
{
	assert(m_state == MEMTX_OK);
	Index *pk = index_find_unique(space, request->index_id);

	/* Check tuple fields */
	if (tuple_validate_raw(space->format, request->tuple))
		diag_raise();

	struct key_def *key_def = pk->key_def;
	uint32_t part_count = pk->key_def->part_count;
	/*
	 * Extract the primary key from tuple.
	 * Allocate enough memory to store the key.
	 */
	const char *key = tuple_extract_key_raw(request->tuple,
						request->tuple_end,
						key_def, NULL);
	if (key == NULL)
		diag_raise();
	/* Cut array header */
	mp_decode_array(&key);

	/* Try to find the tuple by primary key. */
	struct tuple *old_tuple = pk->findByKey(key, part_count);

	if (old_tuple == NULL) {
		/**
		 * Old tuple was not found. In a "true"
		 * non-reading-write engine, this is known only
		 * after commit. Thus any error that can happen
		 * at this point is ignored. Emulate this by
		 * suppressing the error. It's logged and ignored.
		 *
		 * Taking into account that:
		 * 1) Default tuple fields are already fully checked
		 *    at the beginning of the function
		 * 2) Space with unique secondary indexes does not support
		 *    upsert and we can't get duplicate error
		 *
		 * Thus we could get only OOM error, but according to
		 *   https://github.com/tarantool/tarantool/issues/1156
		 *   we should not suppress it
		 *
		 * So we have nothing to catch and suppress!
		 */
		if (tuple_update_check_ops(region_aligned_alloc_xc_cb, &fiber()->gc,
				       request->ops, request->ops_end,
				       request->index_base)) {
			diag_raise();
		}
		struct tuple *new_tuple = tuple_new_xc(space->format,
						       request->tuple,
						       request->tuple_end);
		TupleRef ref(new_tuple); /* useless, for unified approach */
		old_tuple = memtx_replace_all_keys(space, NULL, new_tuple, DUP_INSERT);
		memtx_txn_add_undo(txn, old_tuple, new_tuple);
	} else {
		/**
		 * Update the tuple.
		 * tuple_upsert throws on totally wrong tuple ops,
		 * but ignores ops that not suitable for the tuple
		 */
		struct tuple *new_tuple;
		new_tuple = tuple_upsert(space->format,
					 region_aligned_alloc_xc_cb,
					 &fiber()->gc, old_tuple,
					 request->ops, request->ops_end,
					 request->index_base);
		TupleRef ref(new_tuple);

		/**
		 * Ignore and log all client exceptions,
		 * note that OutOfMemory is not catched.
		 */
		try {
			memtx_replace_all_keys(space, old_tuple, new_tuple, DUP_REPLACE);
			memtx_txn_add_undo(txn, old_tuple, new_tuple);
		} catch (ClientError *e) {
			say_error("UPSERT failed:");
			e->log();
		}
	}
	/* Return nothing: UPSERT does not return data. */
}

Index *
MemtxSpace::createIndex(struct space *space, struct key_def *key_def_arg)
{
	(void) space;
	switch (key_def_arg->type) {
	case HASH:
		return new MemtxHash(key_def_arg);
	case TREE:
		return new MemtxTree(key_def_arg);
	case RTREE:
		return new MemtxRTree(key_def_arg);
	case BITSET:
		return new MemtxBitset(key_def_arg);
	default:
		unreachable();
		return NULL;
	}
}

void
MemtxSpace::dropIndex(Index *index)
{
	if (index->key_def->iid != 0)
		return; /* nothing to do for secondary keys */
	/*
	 * Delete all tuples in the old space if dropping the
	 * primary key.
	 */
	struct iterator *it = ((MemtxIndex*) index)->position();
	index->initIterator(it, ITER_ALL, NULL, 0);
	struct tuple *tuple;
	while ((tuple = it->next(it)))
		tuple_unref(tuple);
}

void
MemtxSpace::prepareAlterSpace(struct space *old_space, struct space *new_space)
{
	(void)new_space;
	MemtxSpace *handler = (MemtxSpace *) old_space->handler;
	m_state = handler->m_state;
}

void
MemtxSpace::executeSelect(struct txn *, struct space *space,
			  uint32_t index_id, uint32_t iterator,
			  uint32_t offset, uint32_t limit,
			  const char *key, const char * /* key_end */,
			  struct port *port)
{
	assert(m_state == MEMTX_OK);
	MemtxIndex *index = (MemtxIndex *) index_find(space, index_id);

	ERROR_INJECT_EXCEPTION(ERRINJ_TESTING);

	uint32_t found = 0;
	if (iterator >= iterator_type_MAX)
		tnt_raise(IllegalParams, "Invalid iterator type");
	enum iterator_type type = (enum iterator_type) iterator;

	uint32_t part_count = key ? mp_decode_array(&key) : 0;
	if (key_validate(index->key_def, type, key, part_count))
		diag_raise();

	struct iterator *it = index->position();
	index->initIterator(it, type, key, part_count);

	struct tuple *tuple;
	while ((tuple = it->next(it)) != NULL) {
		if (offset > 0) {
			offset--;
			continue;
		}
		if (limit == found++)
			break;
		port_add_tuple(port, tuple);
	}
}
