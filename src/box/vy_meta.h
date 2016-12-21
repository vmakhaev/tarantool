#ifndef INCLUDES_TARANTOOL_BOX_VY_META_H
#define INCLUDES_TARANTOOL_BOX_VY_META_H

#include <stdint.h>

#include "tt_uuid.h"

#ifdef __cplusplus
extern "C" {
#endif

struct key_def;
struct tuple;

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

#ifdef __cplusplus
}
#endif

#endif /* INCLUDES_TARANTOOL_BOX_VY_META_H */
