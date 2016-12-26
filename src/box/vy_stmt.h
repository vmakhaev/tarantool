#ifndef INCLUDES_TARANTOOL_BOX_VY_STMT_H
#define INCLUDES_TARANTOOL_BOX_VY_STMT_H
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
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <trivia/util.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <sys/uio.h> /* struct iovec */
#include <msgpuck.h>

#include "iproto_constants.h"
#include "key_def.h"
#include "tuple_compare.h"
#include "tuple_format.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct key_def;
struct tuple_format;
struct xrow_header;

/**
 * There are two groups of statements:
 *
 *  - SELECT and DELETE are "key" statements.
 *  - DELETE, UPSERT and REPLACE are "tuple" statements.
 *
 * REPLACE/UPSERT statements structure:
 *
 *  4 bytes      4 bytes     MessagePack data.
 * +------+-----+------+-----------------------------------+- - - - - - .
 * | off1 | ... | offN | header ..|key1|..|key2|..|keyN|.. | operations |
 * +--+---+-----+--+---+-----------------------------------+- - - - - - .
 *    |     ...    |              ^               ^
 *    |            +--------------+               |
 *    +-------------------------------------------+
 * Offsets are stored only for indexed fields, though MessagePack'ed tuple data
 * can contain also not indexed fields. For example, if fields 3 and 5 are
 * indexed then before MessagePack data are stored offsets only for field 3 and
 * field 5.
 *
 * SELECT/DELETE statements structure.
 * +--------------+-----------------+
 * | array header | part1 ... partN |  -  MessagePack data
 * +--------------+-----------------+
 *
 * Field 'operations' is used for storing operations of UPSERT statement.
 */
struct vy_stmt {
	int64_t  lsn;
	/**
	 * Size of the MessagePack data in raw part of the
	 * statement. It includes upsert operations, if the
	 * statement is UPSERT.
	 */
	uint32_t data_size;
	uint16_t refs; /* atomic */
	uint8_t  type; /* IPROTO_SELECT/REPLACE/UPSERT/DELETE */
	/**
	 * Number of UPSERT statements for the same key preceding
	 * this statement. Used to trigger upsert squashing in the
	 * background (see vy_range_set_upsert()).
	 */
	uint8_t n_upserts;
	/** Offsets count before MessagePack data. */
	uint8_t data_offset;
	char raw[0];
};

/**
 * Increase the reference counter of statement.
 *
 * @param stmt statement.
 */
void
vy_stmt_ref(struct vy_stmt *stmt);

/**
 * Decrease the reference counter of statement.
 *
 * @param stmt statement.
 */
void
vy_stmt_unref(struct vy_stmt *stmt);

/**
 * Duplicate statememnt
 *
 * @param stmt statement
 * @return new statement of the same type with the same data.
 */
struct vy_stmt *
vy_stmt_dup(const struct vy_stmt *stmt);

/** TODO: internal, move to .cc file */
struct vy_stmt *
vy_stmt_alloc(uint32_t size);

/**
 * Return the total size of statement in bytes
 *
 * @param stmt statement
 * @retval the total size of statement in bytes.
 */
static inline uint32_t
vy_stmt_size(const struct vy_stmt *statement)
{
	return sizeof(struct vy_stmt) + statement->data_size +
	       statement->data_offset * sizeof(uint32_t);
}

/**
 * There are two groups of comparators - for raw data and for full statements.
 * Specialized comparators are faster than general-purpose comparators.
 * For example, vy_stmt_compare - slowest comparator because it in worst case
 * checks all combinations of key and tuple types, but
 * vy_key_compare - fastest comparator, because it shouldn't check statement
 * types.
 */

/**
 * Compare key statements by their raw data.
 * @param key_a Left operand of comparison.
 * @param key_b Right operand of comparison.
 * @param key_def Definition of the format of both statements.
 *
 * @retval 0   if key_a == key_b
 * @retval > 0 if key_a > key_b
 * @retval < 0 if key_a < key_b
 */
static inline int
vy_key_compare_raw(const char *key_a, const char *key_b,
		   const struct key_def *key_def)
{
	uint32_t part_count_a = mp_decode_array(&key_a);
	uint32_t part_count_b = mp_decode_array(&key_b);
	return tuple_compare_key_raw(key_a, part_count_a, key_b, part_count_b,
				     key_def);
}

/** @sa vy_key_compare_raw. */
static inline int
vy_key_compare(const struct vy_stmt *left, const struct vy_stmt *right,
	       const struct key_def *key_def)
{
	assert(left->type == IPROTO_SELECT || left->type == IPROTO_DELETE);
	assert(right->type == IPROTO_SELECT || right->type == IPROTO_DELETE);
	return vy_key_compare_raw(left->raw, right->raw, key_def);
}

/**
 * Compare statements by their raw data.
 * @param stmt_a Left operand of comparison.
 * @param stmt_b Right operand of comparison.
 * @param a_type iproto_type of stmt_data_a
 * @param b_type iproto_type of stmt_data_b
 * @param key_def Definition of the format of both statements.
 *
 * @retval 0   if a == b
 * @retval > 0 if a > b
 * @retval < 0 if a < b
 */
static inline int
vy_tuple_compare(const struct vy_stmt *left, const struct vy_stmt *right,
		 const struct tuple_format *format,
		 const struct key_def *key_def)
{
	assert(left->type == IPROTO_REPLACE || left->type == IPROTO_UPSERT);
	assert(right->type == IPROTO_REPLACE || right->type == IPROTO_UPSERT);
	uint32_t *left_offsets = (uint32_t *) left->raw;
	uint32_t *right_offsets = (uint32_t *) right->raw;
	const char *ldata = left->raw + right->data_offset * sizeof(uint32_t);
	assert(mp_typeof(*ldata) == MP_ARRAY);

	const char *rdata = right->raw + right->data_offset * sizeof(uint32_t);
	assert(mp_typeof(*rdata) == MP_ARRAY);
	return tuple_compare_default_raw(format, ldata, left_offsets,
					 format, rdata, right_offsets,
					 key_def);
}

/*
 * Compare a tuple statement with a key statement using their raw data.
 * @param tuple_stmt the raw data of a tuple statement
 * @param key raw data of a key statement
 *
 * @retval > 0  tuple > key.
 * @retval == 0 tuple == key in all fields
 * @retval == 0 tuple is prefix of key
 * @retval == 0 key is a prefix of tuple
 * @retval < 0  tuple < key.
 */
static inline int
vy_tuple_compare_with_key_raw(const char *tuple, const char *key,
			      const struct tuple_format *format,
			      const struct key_def *key_def)
{
	uint32_t *tuple_offsets = (uint32_t *) tuple;
	tuple += format->field_map_size;
	assert(mp_typeof(*tuple) == MP_ARRAY);
	uint32_t part_count = mp_decode_array(&key);
	return tuple_compare_with_key_default_raw(format, tuple, tuple_offsets,
						  key, part_count, key_def);
}

/** @sa vy_stmt_compare_raw. */
static inline int
vy_stmt_compare(const struct vy_stmt *a, const struct vy_stmt *b,
		const struct tuple_format *format,
		const struct key_def *key_def)
{
	bool a_is_tuple = a->type == IPROTO_REPLACE || a->type == IPROTO_UPSERT;
	bool b_is_tuple = b->type == IPROTO_REPLACE || b->type == IPROTO_UPSERT;

	if (a_is_tuple && b_is_tuple) {
		return vy_tuple_compare(a, b, format, key_def);
	} else if (a_is_tuple && !b_is_tuple) {
		return vy_tuple_compare_with_key_raw(a->raw, b->raw, format,
						     key_def);
	} else if (!a_is_tuple && b_is_tuple) {
		return -vy_tuple_compare_with_key_raw(b->raw, a->raw, format,
						      key_def);
	} else {
		assert(!a_is_tuple && !b_is_tuple);
		return vy_key_compare_raw(a->raw, b->raw, key_def);
	}
}

/** @sa vy_stmt_compare_with_raw_key. */
static inline int
vy_stmt_compare_with_key(const struct vy_stmt *stmt,
			 const struct vy_stmt *key,
			 const struct tuple_format *format,
			 const struct key_def *key_def)
{
	assert(key->type == IPROTO_SELECT || key->type == IPROTO_DELETE);
	if (stmt->type == IPROTO_REPLACE || stmt->type == IPROTO_UPSERT)
		return vy_tuple_compare_with_key_raw(stmt->raw, key->raw,
						     format, key_def);
	return vy_key_compare_raw(stmt->raw, key->raw, key_def);
}

/**
 * Create the SELECT statement from raw MessagePack data.
 * @param key MessagePack data that contain an array of fields WITHOUT the
 *            array header.
 * @param part_count Count of the key fields that will be saved as result.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct vy_stmt *
vy_stmt_new_select(const char *key, uint32_t part_count);

/**
 * Create the DELETE statement from raw MessagePack data.
 * @param key MessagePack data that contain an array of fields WITHOUT the
 *            array header.
 * @param part_count Count of the key fields that will be saved as result.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct vy_stmt *
vy_stmt_new_delete(const char *key, uint32_t part_count);

/**
 * Create the REPLACE statement from raw MessagePack data.
 * @param tuple_begin MessagePack data that contain an array of fields WITH the
 *                    array header.
 * @param tuple_end End of the array that begins from @param tuple_begin.
 * @param format Format of a tuple for offsets generating.
 * @param part_count Part count from key definition.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct vy_stmt *
vy_stmt_new_replace(const char *tuple_begin, const char *tuple_end,
		    const struct tuple_format *format,
		    uint32_t part_count);

 /**
 * Create the UPSERT statement from raw MessagePack data.
 * @param tuple_begin MessagePack data that contain an array of fields WITH the
 *                    array header.
 * @param tuple_end End of the array that begins from @param tuple_begin.
 * @param format Format of a tuple for offsets generating.
 * @param part_count Part count from key definition.
 * @param operations Vector of update operations.
 * @param ops_cnt Length of the update operations vector.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct vy_stmt *
vy_stmt_new_upsert(const char *tuple_begin, const char *tuple_end,
		   const struct tuple_format *format, uint32_t part_count,
		   struct iovec *operations, uint32_t ops_cnt);

/**
 * Create REPLACE statement from UPSERT statement.
 *
 * @param upsert upsert statement.
 * @return stmt REPLACE.
 */
struct vy_stmt *
vy_stmt_replace_from_upsert(const struct vy_stmt *upsert);

/**
 * Extract MessagePack data from the SELECT/DELETE statement.
 * @param stmt    An SELECT or DELETE statement.
 *
 * @return MessagePack array of key parts.
 */
static inline const char *
vy_key_data(const struct vy_stmt *stmt)
{
	assert(stmt->type == IPROTO_SELECT || stmt->type == IPROTO_DELETE);
	return stmt->raw;
}

/**
 * Extract MessagePack data from the SELECT/DELETE statement.
 * @param stmt An SELECT or DELETE statement.
 * @param[out] p_size Size of the MessagePack array in bytes.
 *
 * @return MessagePack array of key parts.
 */
static inline const char *
vy_key_data_range(const struct vy_stmt *stmt, uint32_t *p_size)
{
	assert(stmt->type == IPROTO_SELECT || stmt->type == IPROTO_DELETE);
	*p_size = stmt->data_size;
	return stmt->raw;
}

/* TODO: rename to vy_key_part_count */
static inline uint32_t
vy_stmt_part_count(const struct vy_stmt *stmt)
{
	if (stmt->type == IPROTO_SELECT || stmt->type == IPROTO_DELETE) {
		const char *data = stmt->raw;
		return mp_decode_array(&data);
	}
	const char *data = stmt->raw + stmt->data_offset * sizeof(uint32_t);
	return mp_decode_array(&data);
}

/**
 * Extract MessagePack data from the REPLACE/UPSERT statement.
 * @param stmt    An UPSERT or REPLACE statement.
 *
 * @return MessagePack array of tuple fields.
 */
static inline const char *
vy_tuple_data(const struct vy_stmt *stmt)
{
	assert(stmt->type == IPROTO_REPLACE || stmt->type == IPROTO_UPSERT);
	return stmt->raw + stmt->data_offset * sizeof(uint32_t);
}

/**
 * Extract MessagePack data from the REPLACE/UPSERT statement.
 * @param stmt An UPSERT or REPLACE statement.
 * @param[out] p_size Size of the MessagePack array in bytes.
 *
 * @return MessagePack array of tuple fields.
 */
static inline const char *
vy_tuple_data_range(const struct vy_stmt *stmt, uint32_t *p_size)
{
	assert(stmt->type == IPROTO_REPLACE || stmt->type == IPROTO_UPSERT);
	const char *mp = vy_tuple_data(stmt);
	assert(mp_typeof(*mp) == MP_ARRAY);
	if (stmt->type == IPROTO_REPLACE) {
		*p_size = stmt->data_size;
		return mp;
	}
	const char *mp_end = mp;
	mp_next(&mp_end);
	assert(mp < mp_end);
	*p_size = mp_end - mp;
	return mp;
}

/**
 * Extract the operations array from the UPSERT statement.
 * @param stmt An UPSERT statement.
 * @param mp_size Out parameter for size of the returned array.
 *
 * @retval Pointer on MessagePack array of update operations.
 */
static inline const char *
vy_stmt_upsert_ops(const struct vy_stmt *stmt, uint32_t *mp_size)
{
	assert(stmt->type == IPROTO_UPSERT);
	const char *mp = vy_tuple_data(stmt);
	mp_next(&mp);
	*mp_size = stmt->raw + stmt->data_offset * sizeof(uint32_t) +
		   stmt->data_size - mp;
	return mp;
}

/**
 * Extract a SELECT statement with only indexed fields from raw data.
 * @param stmt Raw data of struct vy_stmt.
 * @param key_def key definition.
 *
 * @retval not NULL Success.
 * @retval NULL Memory allocation error.
 */
struct vy_stmt *
vy_stmt_extract_key(const struct vy_stmt *stmt, const struct key_def *key_def);

/**
 * Create the SELECT statement from MessagePack array.
 * @param key
 * @param key_def
 *
 * @retval not NULL Success.
 * @retval     NULL Memory error.
 */
static inline struct vy_stmt *
vy_key_from_message_pack(const char *key, const struct key_def *key_def)
{
	(void) key_def; /* unused in release. */
	uint32_t part_count;
	/*
	 * The statement already is a key, so simply copy it in new
	 * struct vy_stmt as SELECT.
	 */
	part_count = mp_decode_array(&key);
	assert(part_count <= key_def->part_count);
	return vy_stmt_new_select(key, part_count);
}

/**
 * Encode vy_stmt as xrow_header
 *
 * @retval 0 if OK
 * @retval -1 if error
 */
int
vy_stmt_encode(const struct vy_stmt *value, const struct key_def *key_def,
	       struct xrow_header *xrow);

/**
 * Reconstruct vinyl tuple info and data from xrow
 *
 * @retval stmt on success
 * @retval NULL on error
 */
struct vy_stmt *
vy_stmt_decode(struct xrow_header *xrow, const struct tuple_format *format,
	       uint32_t part_count);

/**
 * Format a key into string.
 * Example: [1, 2, "string"]
 * \sa mp_snprint()
 */
int
vy_key_snprint(char *buf, int size, const char *key);

/**
 * Format a statement into string.
 * Example: REPLACE([1, 2, "string"], lsn=48)
 */
int
vy_stmt_snprint(char *buf, int size, const struct vy_stmt *stmt);

/*
* Format a key into string using a static buffer.
* Useful for gdb and say_debug().
* \sa vy_key_snprint()
*/
const char *
vy_key_str(const char *key);

/*
* Format a statement into string using a static buffer.
* Useful for gdb and say_debug().
* \sa vy_stmt_snprint()
*/
const char *
vy_stmt_str(const struct vy_stmt *stmt);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* INCLUDES_TARANTOOL_BOX_VY_STMT_H */
