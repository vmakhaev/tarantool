#include "vy_meta.h"

#include <msgpuck.h>

#include "box.h" /* boxk() */
#include "cluster.h" /* SERVER_UUID */
#include "diag.h"
#include "index.h" /* box_index_max() */
#include "iproto_constants.h"
#include "key_def.h"
#include "schema.h"
#include "scoped_guard.h"
#include "space.h"
#include "tt_uuid.h"
#include "tuple.h"

/*
 * Fill vy_meta structure from a record in _vinyl
 * system space.
 */
void
vy_meta_create_from_tuple(struct vy_meta *def, struct tuple *tuple)
{
	tuple_field_uuid(tuple, 0, &def->server_uuid);
	def->run_id = tuple_field_uint(tuple, 1);
	def->space_id = tuple_field_u32(tuple, 2);
	def->index_id = tuple_field_u32(tuple, 3);
	def->index_lsn = tuple_field_uint(tuple, 4);
	def->state = (enum vy_run_state)tuple_field_u32(tuple, 5);
	def->begin = tuple_field_check(tuple, 6, MP_ARRAY);
	def->end = tuple_field_check(tuple, 7, MP_ARRAY);
}

/*
 * Return the id that will be used for the next new run,
 * or -1 on failure.
 */
int64_t
vy_meta_next_run_id(void)
{
	char server_uuid_str[UUID_STR_LEN];
	tt_uuid_to_string(&SERVER_UUID, server_uuid_str);

	char key[64];
	char *key_end = key;
	key_end = mp_encode_array(key_end, 1);
	key_end = mp_encode_str(key_end, server_uuid_str, UUID_STR_LEN);
	assert(key_end <= key + sizeof(key));

	box_tuple_t *max;
	if (box_index_max(BOX_VINYL_ID, 0, key, key_end, &max) != 0)
		return -1;
	if (max != NULL) {
		struct vy_meta def;
		try {
			vy_meta_create_from_tuple(&def, max);
		} catch (Exception *e) {
			return -1;
		}
		if (tt_uuid_is_equal(&def.server_uuid, &SERVER_UUID))
			return def.run_id + 1;
	}
	/* First run for this server. */
	return 0;
}

/*
 * Insert a run record into the vinyl metadata table.
 *
 * This function allocates a unique ID for the run on success.
 */
int
vy_meta_insert_run(const char *begin, const char *end,
		   const struct key_def *key_def,
		   enum vy_run_state state, int64_t *p_run_id)
{
	char server_uuid_str[UUID_STR_LEN];
	tt_uuid_to_string(&SERVER_UUID, server_uuid_str);

	int64_t run_id = vy_meta_next_run_id();
	if (run_id < 0)
		return -1;

	char empty_key[16];
	assert(mp_sizeof_array(0) <= sizeof(empty_key));
	mp_encode_array(empty_key, 0);
	if (begin == NULL)
		begin = empty_key;
	if (end == NULL)
		end = empty_key;

	if (boxk(IPROTO_INSERT, BOX_VINYL_ID, "[%s%llu%u%u%llu%u%p%p]",
		 server_uuid_str, (unsigned long long)run_id,
		 (unsigned)key_def->space_id, (unsigned)key_def->iid,
		 (unsigned long long)key_def->opts.lsn, (unsigned)state,
		 begin, end) != 0)
		return -1;

	*p_run_id = run_id;
	return 0;
}

/*
 * Update the state of a run in the vinyl metadata table.
 */
int
vy_meta_update_run(int64_t run_id, enum vy_run_state state)
{
	assert(run_id >= 0);
	char server_uuid_str[UUID_STR_LEN];
	tt_uuid_to_string(&SERVER_UUID, server_uuid_str);
	return boxk(IPROTO_UPDATE, BOX_VINYL_ID, "[%s%llu][[%s%d%u]]",
		    server_uuid_str, (unsigned long long)run_id,
		    "=", 5, (unsigned)state);
}

/*
 * Delete a run record from the vinyl metadata table.
 */
int
vy_meta_delete_run(int64_t run_id)
{
	assert(run_id >= 0);
	char server_uuid_str[UUID_STR_LEN];
	tt_uuid_to_string(&SERVER_UUID, server_uuid_str);
	return boxk(IPROTO_DELETE, BOX_VINYL_ID, "[%s%llu]",
		    server_uuid_str, (unsigned long long)run_id);
}

static int
vy_meta_purge_state(Index *index, enum vy_run_state state,
		    const char *server_uuid_str, vy_meta_purge_cb cb)
{
	char key[64];
	size_t key_len = mp_format(key, sizeof(key), "%u%s",
				   (unsigned)state, server_uuid_str);
	assert(key_len <= sizeof(key));

	struct iterator *it = index->allocIterator();
	auto guard_it_free = make_scoped_guard([=]{ it->free(it); });

	while (true) {
		index->initIterator(it, ITER_EQ, key, 2);
		struct tuple *tuple = it->next(it);
		if (tuple == NULL)
			break;
		struct vy_meta def;
		try {
			vy_meta_create_from_tuple(&def, tuple);
		} catch (Exception *e) {
			return -1;
		}
		/* def points to tuple fields. */
		tuple_ref(tuple);
		int rc = cb(&def);
		tuple_unref(tuple);
		if (rc != 0)
			return -1;
		if (vy_meta_delete_run(def.run_id) != 0)
			return -1;
	}
	return 0;
}

/*
 * Delete all stale run records whose from the vinyl metadata table,
 * calling @cb per each deleted record. If @cb fails, -1 is returned
 * and the procedure is aborted.
 */
int
vy_meta_purge(vy_meta_purge_cb cb)
{
	char server_uuid_str[UUID_STR_LEN];
	tt_uuid_to_string(&SERVER_UUID, server_uuid_str);

	struct space *space = space_by_id(BOX_VINYL_ID);
	Index *index = space_index(space, 1);

	/* TODO: Delete VY_RUN_RESERVED records as well. */
	if (vy_meta_purge_state(index, VY_RUN_DELETED,
				server_uuid_str, cb) != 0 ||
	    vy_meta_purge_state(index, VY_RUN_FAILED,
				server_uuid_str, cb) != 0)
		return -1;

	return 0;
}
