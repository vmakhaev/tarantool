#ifndef INCLUDES_TARANTOOL_BOX_VINYL_H
#define INCLUDES_TARANTOOL_BOX_VINYL_H
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

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "tt_uuid.h"

#ifdef __cplusplus
extern "C" {
#endif

struct vy_env;
struct vy_tx;
struct vy_cursor;
struct vy_index;
struct key_def;
struct tuple;
struct tuple_format;
struct vclock;
struct request;
struct space;
struct txn_stmt;
enum iterator_type;

/*
 * Environment
 */

struct vy_env *
vy_env_new(void);

void
vy_env_delete(struct vy_env *e);

/*
 * Recovery
 */

void
vy_bootstrap(struct vy_env *e);

void
vy_begin_initial_recovery(struct vy_env *e, struct vclock *vclock);

void
vy_begin_final_recovery(struct vy_env *e);

int
vy_end_recovery(struct vy_env *e);

int
vy_checkpoint(struct vy_env *env);

int
vy_wait_checkpoint(struct vy_env *env, struct vclock *vlock);

/*
 * Introspection
 */

enum vy_info_type {
	VY_INFO_TABLE_BEGIN,
	VY_INFO_TABLE_END,
	VY_INFO_STRING,
	VY_INFO_U32,
	VY_INFO_U64,
};

struct vy_info_node {
	enum vy_info_type type;
	const char *key;
	union {
		const char *str;
		uint32_t u32;
		uint64_t u64;
	} value;
};

struct vy_info_handler {
	void (*fn)(struct vy_info_node *node, void *ctx);
	void *ctx;
};

void
vy_info_gather(struct vy_env *env, struct vy_info_handler *h);

/*
 * Transaction
 */

struct vy_tx *
vy_begin(struct vy_env *e);

/**
 * Get a tuple from the vinyl index.
 * @param tx          Current transaction.
 * @param index       Vinyl index.
 * @param key         MessagePack'ed data, the array without a
 *                    header.
 * @param part_count  Part count of the key
 * @param[out] result Is set to the the found tuple.
 *
 * @retval  0 Success.
 * @retval -1 Memory or read error.
 */
int
vy_get(struct vy_tx *tx, struct vy_index *index,
       const char *key, uint32_t part_count, struct tuple **result);

/**
 * Execute REPLACE in a vinyl space.
 * @param tx      Current transaction.
 * @param stmt    Statement for triggers filled with old
 *                statement.
 * @param space   Vinyl space.
 * @param request Request with the tuple data.
 *
 * @retval  0 Success
 * @retval -1 Memory error OR duplicate key error OR the primary
 *            index is not found OR a tuple reference increment
 *            error.
 */
int
vy_replace_all(struct vy_tx *tx, struct txn_stmt *stmt,
		 struct space *space, struct request *request);

/**
 * Execute DELETE in a vinyl space.
 * @param tx      Current transaction.
 * @param stmt    Statement for triggers filled with deleted
 *                statement.
 * @param space   Vinyl space.
 * @param request Request with the tuple data.
 *
 * @retval  0 Success
 * @retval -1 Memory error OR the index is not found OR a tuple
 *            reference increment error.
 */
int
vy_delete_all(struct vy_tx *tx, struct txn_stmt *stmt, struct space *space,
	      struct request *request);

/**
 * Execute UPDATE in a vinyl space.
 * @param tx      Current transaction.
 * @param stmt    Statement for triggers filled with old and new
 *                statements.
 * @param space   Vinyl space.
 * @param request Request with the tuple data.
 *
 * @retval  0 Success
 * @retval -1 Memory error OR the index is not found OR a tuple
 *            reference increment error.
 */
int
vy_update_all(struct vy_tx *tx, struct txn_stmt *stmt, struct space *space,
	      struct request *request);

/**
 * Execute INSERT in a vinyl space.
 * @param tx      Current transaction.
 * @param space   Vinyl space.
 * @param request Request with the tuple data and update
 *                operations.
 *
 * @retval  0 Success
 * @retval -1 Memory error OR duplicate error OR the primary
 *            index is not found
 */
int
vy_insert_all(struct vy_tx *tx, struct space *space, struct request *request);

/**
 * Execute UPSERT in a vinyl space.
 * @param tx      Current transaction.
 * @param stmt    Statement for triggers filled with old and new
 *                statements.
 * @param space   Vinyl space.
 * @param request Request with the tuple data and update
 *                operations.
 *
 * @retval  0 Success
 * @retval -1 Memory error OR the index is not found OR a tuple
 *            reference increment error.
 */
int
vy_upsert_all(struct vy_tx *tx, struct txn_stmt *stmt, struct space *space,
	      struct request *request);

int
vy_prepare(struct vy_env *e, struct vy_tx *tx);

int
vy_commit(struct vy_env *e, struct vy_tx *tx, int64_t lsn);

void
vy_rollback(struct vy_env *e, struct vy_tx *tx);

void *
vy_savepoint(struct vy_tx *tx);

void
vy_rollback_to_savepoint(struct vy_tx *tx, void *svp);

/*
 * Index
 */

/**
 * Create a new vinyl index object without opening it.
 * @param e                    Vinyl environment.
 * @param user_key_def         Key definition declared by an user
 *                             with space:create_index().
 * @param space                Space for which the new index
 *                             belongs.
 */
struct vy_index *
vy_index_new(struct vy_env *e, struct key_def *user_key_def,
	     struct space *space);

/**
 * Hook on an alter space commit event. It is called on each
 * create_index(), drop_index() and is used for update
 * vy_index.space attribute.
 * @param old_space Old space.
 * @param new_space New space.
 */
void
vy_commit_alter_space(struct space *old_space, struct space *new_space);

int
vy_index_open(struct vy_index *index);

/**
 * Close index and drop all data
 */
int
vy_index_drop(struct vy_index *index);

size_t
vy_index_bsize(struct vy_index *db);

/*
 * Index Cursor
 */

/**
 * Create a cursor. If tx is not NULL, the cursor life time is
 * bound by the transaction life time. Otherwise, the cursor
 * allocates its own transaction.
 */
struct vy_cursor *
vy_cursor_new(struct vy_tx *tx, struct vy_index *index, const char *key,
	      uint32_t part_count, enum iterator_type type);

/**
 * Fetch the transaction used in the cursor.
 */
int
vy_cursor_tx(struct vy_cursor *cursor, struct vy_tx **tx);

void
vy_cursor_delete(struct vy_cursor *cursor);

int
vy_cursor_next(struct vy_cursor *cursor, struct tuple **result);

/*
 * Replication
 */

typedef int
(*vy_send_row_f)(void *, const char *tuple, uint32_t tuple_size, int64_t lsn);
int
vy_index_send(struct vy_index *index, vy_send_row_f sendrow, void *ctx);

/*
 * Metadata
 */

/*
 * State of a run as recorded in the vinyl metadata table.
 */
enum vy_run_state {
	/*
	 * Run is permanent. It was created and it must be recovered.
	 * It may or may not have a file on disk though depending on
	 * whether it was dumped or not.
	 */
	VY_RUN_COMMITTED,
	/*
	 * Run was deleted after compaction. On snapshot its file will
	 * be deleted and the record wiped out. When recovering from
	 * xlog, we must "replay" the delete operation upon running into
	 * such a record.
	 */
	VY_RUN_DELETED,
	/*
	 * Such a record is created for the new run which is going to be
	 * the product of compaction. It serves for reserving run ID.
	 * When compaction completes, it either becomes "committed" or
	 * "failed" depending on whether compaction succeeded or failed.
	 * It is ignored on recovery.
	 */
	VY_RUN_RESERVED,
	/*
	 * Run had been created for compaction which was then aborted.
	 * The special state is needed solely for garbage collection.
	 * It is ignored on recovery.
	 */
	VY_RUN_FAILED,

	vy_run_state_MAX,
};

struct vy_meta {
	/** UUID of the server. */
	struct tt_uuid server_uuid;
	/** Unique ID of the run. */
	uint64_t run_id;
	/** Space ID this run is for. */
	uint32_t space_id;
	/** Index ID this run is for. */
	uint32_t index_id;
	/** LSN at the time of index creation. */
	uint64_t index_lsn;
	/** Run state. */
	enum vy_run_state state;
	/** Start of the range this run belongs to. */
	const char *begin;
	/** End of the range this run belongs to. */
	const char *end;
};

int
vy_meta_create_from_tuple(struct vy_meta *r, struct tuple *tuple);
int
vy_meta_insert_run(const char *begin, const char *end,
		   const struct key_def *key_def,
		   enum vy_run_state state, int64_t *p_run_id);
int
vy_meta_update_run(int64_t run_id, enum vy_run_state state);
int
vy_meta_delete_run(int64_t run_id);

int
vy_recovery_process_meta(struct vy_index *index, const struct vy_meta *def);

void
vy_index_purge_run(struct vy_index *index, int64_t run_id);

#ifdef __cplusplus
}
#endif

#endif /* INCLUDES_TARANTOOL_BOX_VINYL_H */
