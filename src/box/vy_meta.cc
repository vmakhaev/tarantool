#include "vy_meta.h"

#include <msgpuck.h>

#include "box.h" /* boxk() */
#include "cluster.h" /* SERVER_UUID */
#include "diag.h"
#include "index.h" /* box_index_max() */
#include "iproto_constants.h"
#include "key_def.h"
#include "schema.h" /* BOX_VINYL_ID */
#include "tt_uuid.h"
#include "tuple.h"

/*
 * Try to decode u64 from MsgPack data.
 * Return true on success.
 */
static inline bool
mp_decode_uint_check(const char **data, uint64_t *pval)
{
	if (mp_typeof(**data) != MP_UINT)
		return false;
	*pval = mp_decode_uint(data);
	return true;
}

/*
 * Try to decode u32 from MsgPack data.
 * Return true on success.
 */
static inline bool
mp_decode_u32_check(const char **data, uint32_t *pval)
{
	uint64_t val;
	if (!mp_decode_uint_check(data, &val))
		return false;
	if (val > UINT32_MAX)
		return false;
	*pval = val;
	return true;
}

/*
 * Fill vy_meta structure from a record in _vinyl
 * system space.
 */
int
vy_meta_create_from_tuple(struct vy_meta *def, struct tuple *tuple)
{
	const char *data = tuple_data(tuple);
	uint32_t len;
	const char *str;

	if (mp_decode_array(&data) != 8)
		goto fail;
	if (mp_typeof(*data) != MP_STR)
		goto fail;
	str = mp_decode_str(&data, &len);
	uint32_t state;
	if (tt_uuid_from_strl(str, len, &def->server_uuid) != 0 ||
	    !mp_decode_uint_check(&data, &def->run_id) ||
	    !mp_decode_u32_check(&data, &def->space_id) ||
	    !mp_decode_u32_check(&data, &def->index_id) ||
	    !mp_decode_uint_check(&data, &def->index_lsn) ||
	    !mp_decode_u32_check(&data, &state) ||
	    state >= vy_run_state_MAX)
		goto fail;
	def->state = (enum vy_run_state)state;
	if (mp_typeof(*data) != MP_ARRAY)
		goto fail;
	def->begin = data;
	mp_next(&data);
	if (mp_typeof(*data) != MP_ARRAY)
		goto fail;
	def->end = data;
	return 0;
fail:
	diag_set(ClientError, ER_VINYL, "invalid metadata");
	return -1;
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
	int64_t run_id = 0;
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
		if (vy_meta_create_from_tuple(&def, max) != 0)
			return -1;
		if (tt_uuid_is_equal(&def.server_uuid, &SERVER_UUID))
			run_id = def.run_id + 1;
	}

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
