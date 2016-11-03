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
#include "tuple_compare.h"
#include "tuple.h"

/* {{{ tuple_compare */

/*
 * Compare two tuple fields.
 * Separate version exists since compare is a very
 * often used operation, so any performance speed up
 * in it can have dramatic impact on the overall
 * server performance.
 */
inline __attribute__((always_inline)) int
mp_compare_uint(const char **data_a, const char **data_b);

enum mp_class {
	MP_CLASS_NIL = 0,
	MP_CLASS_BOOL,
	MP_CLASS_NUMBER,
	MP_CLASS_STR,
	MP_CLASS_BIN,
	MP_CLASS_ARRAY,
	MP_CLASS_MAP
};

static enum mp_class mp_classes[] = {
	/* .MP_NIL     = */ MP_CLASS_NIL,
	/* .MP_UINT    = */ MP_CLASS_NUMBER,
	/* .MP_INT     = */ MP_CLASS_NUMBER,
	/* .MP_STR     = */ MP_CLASS_STR,
	/* .MP_BIN     = */ MP_CLASS_BIN,
	/* .MP_ARRAY   = */ MP_CLASS_ARRAY,
	/* .MP_MAP     = */ MP_CLASS_MAP,
	/* .MP_BOOL    = */ MP_CLASS_BOOL,
	/* .MP_FLOAT   = */ MP_CLASS_NUMBER,
	/* .MP_DOUBLE  = */ MP_CLASS_NUMBER,
	/* .MP_BIN     = */ MP_CLASS_BIN
};

#define COMPARE_RESULT(a, b) (a < b ? -1 : a > b)

static enum mp_class
mp_classof(enum mp_type type)
{
	return mp_classes[type];
}

static inline double
mp_decode_number(const char **data)
{
	double val;
	switch (mp_typeof(**data)) {
	case MP_UINT:
		val = mp_decode_uint(data);
		break;
	case MP_INT:
		val = mp_decode_int(data);
		break;
	case MP_FLOAT:
		val = mp_decode_float(data);
		break;
	case MP_DOUBLE:
		val = mp_decode_double(data);
		break;
	default:
		unreachable();
	}
	return val;
}

static int
mp_compare_bool(const char *field_a, const char *field_b)
{
	int a_val = mp_decode_bool(&field_a);
	int b_val = mp_decode_bool(&field_b);
	return COMPARE_RESULT(a_val, b_val);
}

static int
mp_compare_integer(const char *field_a, const char *field_b)
{
	enum mp_type a_type = mp_typeof(*field_a);
	enum mp_type b_type = mp_typeof(*field_b);
	assert(mp_classof(a_type) == MP_CLASS_NUMBER);
	assert(mp_classof(b_type) == MP_CLASS_NUMBER);
	if (a_type == MP_UINT) {
		uint64_t a_val = mp_decode_uint(&field_a);
		if (b_type == MP_UINT) {
			uint64_t b_val = mp_decode_uint(&field_b);
			return COMPARE_RESULT(a_val, b_val);
		} else {
			int64_t b_val = mp_decode_int(&field_b);
			if (b_val < 0)
				return 1;
			return COMPARE_RESULT(a_val, (uint64_t)b_val);
		}
	} else {
		int64_t a_val = mp_decode_int(&field_a);
		if (b_type == MP_UINT) {
			uint64_t b_val = mp_decode_uint(&field_b);
			if (a_val < 0)
				return -1;
			return COMPARE_RESULT((uint64_t)a_val, b_val);
		} else {
			int64_t b_val = mp_decode_int(&field_b);
			return COMPARE_RESULT(a_val, b_val);
		}
	}
}

static int
mp_compare_number(const char *field_a, const char *field_b)
{
	enum mp_type a_type = mp_typeof(*field_a);
	enum mp_type b_type = mp_typeof(*field_b);
	assert(mp_classof(a_type) == MP_CLASS_NUMBER);
	assert(mp_classof(b_type) == MP_CLASS_NUMBER);
	if (a_type == MP_FLOAT || a_type == MP_DOUBLE ||
	    b_type == MP_FLOAT || b_type == MP_DOUBLE) {
		double a_val = mp_decode_number(&field_a);
		double b_val = mp_decode_number(&field_b);
		return COMPARE_RESULT(a_val, b_val);
	}
	return mp_compare_integer(field_a, field_b);
}

static inline int
mp_compare_str(const char *field_a, const char *field_b)
{
	uint32_t size_a = mp_decode_strl(&field_a);
	uint32_t size_b = mp_decode_strl(&field_b);
	int r = memcmp(field_a, field_b, MIN(size_a, size_b));
	if (r != 0)
		return r;
	return COMPARE_RESULT(size_a, size_b);
}

static inline int
mp_compare_bin(const char *field_a, const char *field_b)
{
	uint32_t size_a = mp_decode_binl(&field_a);
	uint32_t size_b = mp_decode_binl(&field_b);
	int r = memcmp(field_a, field_b, MIN(size_a, size_b));
	if (r != 0)
		return r;
	return COMPARE_RESULT(size_a, size_b);
}

typedef int (*mp_compare_f)(const char *, const char *);
static mp_compare_f mp_class_comparators[] = {
	/* .MP_CLASS_NIL    = */ NULL,
	/* .MP_CLASS_BOOL   = */ mp_compare_bool,
	/* .MP_CLASS_NUMBER = */ mp_compare_number,
	/* .MP_CLASS_STR    = */ mp_compare_str,
	/* .MP_CLASS_BIN    = */ mp_compare_bin,
	/* .MP_CLASS_ARRAY  = */ NULL,
	/* .MP_CLASS_MAP    = */ NULL,
};

static int
mp_compare_scalar(const char *field_a, const char *field_b)
{
	enum mp_type a_type = mp_typeof(*field_a);
	enum mp_type b_type = mp_typeof(*field_b);
	enum mp_class a_class = mp_classof(a_type);
	enum mp_class b_class = mp_classof(b_type);
	if (a_class != b_class)
		return COMPARE_RESULT(a_class, b_class);
	mp_compare_f cmp = mp_class_comparators[a_class];
	assert(cmp != NULL);
	return cmp(field_a, field_b);
}

int
tuple_compare_field(const char *field_a, const char *field_b,
		    enum field_type type)
{
	switch (type) {
	case FIELD_TYPE_UNSIGNED:
		return mp_compare_uint(field_a, field_b);
	case FIELD_TYPE_STRING:
		return mp_compare_str(field_a, field_b);
	case FIELD_TYPE_INTEGER:
		return mp_compare_integer(field_a, field_b);
	case FIELD_TYPE_NUMBER:
		return mp_compare_number(field_a, field_b);
	case FIELD_TYPE_SCALAR:
		return mp_compare_scalar(field_a, field_b);
	default:
		unreachable();
		return 0;
	}
}

int
tuple_compare_default_raw(const struct tuple_format *format_a,
			  const char *tuple_a, const uint32_t *field_map_a,
			  const struct tuple_format *format_b,
			  const char *tuple_b, const uint32_t *field_map_b,
			  const struct key_def *key_def)
{
	const struct key_part *part = key_def->parts;
	if (key_def->part_count == 1 && part->fieldno == 0) {
		mp_decode_array(&tuple_a);
		mp_decode_array(&tuple_b);
		return tuple_compare_field(tuple_a, tuple_b, part->type);
	}

	const struct key_part *end = part + key_def->part_count;
	const char *field_a;
	const char *field_b;
	int r = 0;

	for (; part < end; part++) {
		field_a = tuple_field_raw(format_a, tuple_a, field_map_a,
					  part->fieldno);
		field_b = tuple_field_raw(format_b, tuple_b, field_map_b,
					  part->fieldno);
		assert(field_a != NULL && field_b != NULL);
		if ((r = tuple_compare_field(field_a, field_b, part->type)))
			break;
	}
	return r;
}

int
tuple_compare(const struct tuple *tuple_a, const struct tuple *tuple_b,
	      const struct key_def *key_def)
{
	return key_def->tuple_compare_raw(tuple_format(tuple_a), tuple_a->data,
					 tuple_field_map(tuple_a),
					 tuple_format(tuple_b), tuple_b->data,
					 tuple_field_map(tuple_b), key_def);
}

int
tuple_compare_default(const struct tuple *tuple_a, const struct tuple *tuple_b,
		      const struct key_def *key_def)
{
	return tuple_compare_default_raw(tuple_format(tuple_a), tuple_a->data,
					 tuple_field_map(tuple_a),
					 tuple_format(tuple_b), tuple_b->data,
					 tuple_field_map(tuple_b), key_def);
}

int
tuple_compare_key_raw(const char *key_a, uint32_t part_count_a,
		      const char *key_b, uint32_t part_count_b,
		      const struct key_def *key_def)
{
	assert(key_a != NULL || part_count_a == 0);
	assert(key_b != NULL || part_count_b == 0);
	assert(part_count_a <= key_def->part_count);
	assert(part_count_b <= key_def->part_count);
	const struct key_part *part = key_def->parts;
	uint32_t part_count;
	part_count = MIN(MIN(part_count_a, key_def->part_count), part_count_b);
	if (likely(part_count == 1))
		return tuple_compare_field(key_a, key_b, part->type);
	const struct key_part *end;
	end = part + part_count;
	int r = 0; /* Part count can be 0 in wildcard searches. */
	for (; part < end; part++) {
		r = tuple_compare_field(key_a, key_b, part->type);
		if (r != 0)
			break;
		mp_next(&key_a);
		mp_next(&key_b);
	}
	return r;
}

int
tuple_compare_with_key_default_raw(const struct tuple_format *format,
				   const char *tuple, const uint32_t *field_map,
				   const char *key, uint32_t part_count,
				   const struct key_def *key_def)
{
	assert(key != NULL || part_count == 0);
	assert(part_count <= key_def->part_count);
	const struct key_part *part = key_def->parts;
	if (likely(part_count == 1)) {
		const char *field;
		field = tuple_field_raw(format, tuple, field_map,
					part->fieldno);
		return tuple_compare_field(field, key, part->type);
	}

	const struct key_part *end = part + MIN(part_count, key_def->part_count);
	int r = 0; /* Part count can be 0 in wildcard searches. */
	for (; part < end; part++) {
		const char *field;
		field = tuple_field_raw(format, tuple, field_map,
					part->fieldno);
		r = tuple_compare_field(field, key, part->type);
		if (r != 0)
			break;
		mp_next(&key);
	}
	return r;
}

int
tuple_compare_with_key_default(const struct tuple *tuple, const char *key,
			       uint32_t part_count,
			       const struct key_def *key_def)
{
	return tuple_compare_with_key_default_raw(tuple_format(tuple),
						  tuple->data,
						  (uint32_t *) tuple, key,
						  part_count, key_def);
}

int
tuple_compare_with_key(const struct tuple *tuple, const char *key,
		       uint32_t part_count, const struct key_def *key_def)
{
	return key_def->tuple_compare_with_key(tuple, key, part_count, key_def);
}

template <int TYPE>
static inline int
field_compare(const char **field_a, const char **field_b);

template <>
inline int
field_compare<FIELD_TYPE_UNSIGNED>(const char **field_a, const char **field_b)
{
	return mp_compare_uint(*field_a, *field_b);
}

template <>
inline int
field_compare<FIELD_TYPE_STRING>(const char **field_a, const char **field_b)
{
	uint32_t size_a, size_b;
	size_a = mp_decode_strl(field_a);
	size_b = mp_decode_strl(field_b);
	int r = memcmp(*field_a, *field_b, MIN(size_a, size_b));
	if (r == 0)
		r = size_a < size_b ? -1 : size_a > size_b;
	return r;
}

/*
 * Compare two MessagePack encoded values and propagate both pointers to next
 * values.
 */
template <int TYPE>
static inline int
field_compare_and_next(const char **field_a, const char **field_b);

template <>
inline int
field_compare_and_next<FIELD_TYPE_UNSIGNED>(const char **field_a,
					    const char **field_b)
{
	int r = mp_compare_uint(*field_a, *field_b);
	mp_next(field_a);
	mp_next(field_b);
	return r;
}

template <>
inline int
field_compare_and_next<FIELD_TYPE_STRING>(const char **field_a,
					  const char **field_b)
{
	uint32_t size_a, size_b;
	size_a = mp_decode_strl(field_a);
	size_b = mp_decode_strl(field_b);
	int r = memcmp(*field_a, *field_b, MIN(size_a, size_b));
	if (r == 0)
		r = size_a < size_b ? -1 : size_a > size_b;
	*field_a += size_a;
	*field_b += size_b;
	return r;
}

/* Tuple comparator */
namespace /* local symbols */ {

template <int IDX, int TYPE, int ...MORE_TYPES> struct FieldCompare { };

/*
 * Struct for comparison two tuples by their key fields. We need to use
 * struct with static method instead of template function, because partial
 * specialization of a template function isn't enabled.
 * @param IDX Index of the current field for the comparison.
 * @param TYPE Type of the IDX field.
 * @param IDX2 Index of the next field for the comparison.
 * @param TYPE2 Type of the IDX2 field.
 * @param MORE_TYPES Variable count of other pairs (IDX_i, TYPE_i), i > 2.
 */
template <int IDX, int TYPE, int IDX2, int TYPE2, int ...MORE_TYPES>
struct FieldCompare<IDX, TYPE, IDX2, TYPE2, MORE_TYPES...>
{
	/*
	 * Recursively compare MessagePack encoded tuples by key fields.
	 * @param format_a Format of the first tuple.
	 * @param tuple_a MessagePack encoded array of fields of the first
	 *                tuple.
	 * @param field_map_a Field map with offsets to key fields of the first
	 *                    tuple.
	 * @param field_a IDX key field of the first tuple.
	 * @param format_b Format of the second tuple.
	 * @param tuple_b MessagePack encoded array of fields of the second
	 *                tuple.
	 * @param field_map_b Field map with offsets to key fields of the second
	 *                    tuple.
	 * @param field_b IDX key field of the second tuple.
	 */
	inline static int
	compare(const struct tuple_format *format_a, const char *tuple_a,
		const uint32_t *field_map_a, const char *field_a,
		const struct tuple_format *format_b, const char *tuple_b,
		const uint32_t *field_map_b, const char *field_b)
	{
		int r;
		/*
		 * If the next key is right after the current key then mp_next
		 * is faster then lookup in field_map for the next key offset.
		 */
		if (IDX + 1 == IDX2) {
			if ((r = field_compare_and_next<TYPE>(&field_a,
							      &field_b)) != 0)
				return r;
		} else {
			if ((r = field_compare<TYPE>(&field_a, &field_b)) != 0)
				return r;
			/* Get the next pair of key fields. */
			field_a = tuple_field_raw(format_a, tuple_a,
						  field_map_a, IDX2);
			field_b = tuple_field_raw(format_b, tuple_b,
						  field_map_b, IDX2);
		}
		/* Continue the comparison from next key fields. */
		return FieldCompare<IDX2, TYPE2, MORE_TYPES...>::
			compare(format_a, tuple_a, field_map_a, field_a,
				format_b, tuple_b, field_map_b, field_b);
	}
};

/*
 * Special case of the main FieldCompare template. This not recursive case
 * works for simple one part keys.
 */
template <int IDX, int TYPE>
struct FieldCompare<IDX, TYPE>
{
	/* Compare two single key fields. */
	inline static int
	compare(const struct tuple_format *, const char *, const uint32_t *,
		const char *field_a, const struct tuple_format *, const char *,
		const uint32_t *, const char *field_b)
	{
		return field_compare<TYPE>(&field_a, &field_b);
	}
};

/* @sa FieldCompare. */
template <int IDX, int TYPE, int ...MORE_TYPES>
struct TupleCompare
{
	/* First call of the recursive FieldCompare with first key fields. */
	static int
	compare(const struct tuple_format *format_a, const char *tuple_a,
		const uint32_t *field_map_a,
		const struct tuple_format *format_b, const char *tuple_b,
		const uint32_t *field_map_b, const struct key_def *)
	{
		const char *field_a, *field_b;
		field_a = tuple_field_raw(format_a, tuple_a, field_map_a, IDX);
		field_b = tuple_field_raw(format_b, tuple_b, field_map_b, IDX);
		return FieldCompare<IDX, TYPE, MORE_TYPES...>::
			compare(format_a, tuple_a, field_map_a, field_a,
				format_b, tuple_b, field_map_b, field_b);
	}
};

/*
 * Special case of the main TupleCompare template. This specialization works
 * if the first field of tuples is indexed, that is if first part of the
 * @param key_def is {fieldno = 0, type = ...}, so mp_decode_array call is
 * faster then tuple_field_raw with lookup to the field_map.
 */
template <int TYPE, int ...MORE_TYPES>
struct TupleCompare<0, TYPE, MORE_TYPES...>
{
	static int
	compare(const struct tuple_format *format_a, const char *tuple_a,
		const uint32_t *field_map_a,
		const struct tuple_format *format_b, const char *tuple_b,
		const uint32_t *field_map_b, const struct key_def *)
	{
		const char *field_a = tuple_a;
		const char *field_b = tuple_b;
		mp_decode_array(&field_a);
		mp_decode_array(&field_b);
		return FieldCompare<0, TYPE, MORE_TYPES...>::
			compare(format_a, tuple_a, field_map_a, field_a,
				format_b, tuple_b, field_map_b, field_b);
	}
};
} /* end of anonymous namespace */

struct comparator_signature {
	tuple_compare_raw_t f;
	uint32_t p[64];
};
#define COMPARATOR(...) \
	{ TupleCompare<__VA_ARGS__>::compare, { __VA_ARGS__, UINT32_MAX } },

/**
 * field1 no, field1 type, field2 no, field2 type, ...
 */
static const comparator_signature cmp_arr[] = {
	COMPARATOR(0, FIELD_TYPE_UNSIGNED)
	COMPARATOR(0, FIELD_TYPE_STRING)
	COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_UNSIGNED)
	COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_UNSIGNED)
	COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_STRING)
	COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_STRING)
	COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_UNSIGNED)
	COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_UNSIGNED)
	COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_UNSIGNED)
	COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_UNSIGNED)
	COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_STRING)
	COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_STRING)
	COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_STRING)
	COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_STRING)
};

#undef COMPARATOR

static tuple_compare_raw_t
tuple_compare_create_raw(const struct key_def *def) {
	for (uint32_t k = 0; k < sizeof(cmp_arr) / sizeof(cmp_arr[0]); k++) {
		uint32_t i = 0;
		for (; i < def->part_count; i++)
			if (def->parts[i].fieldno != cmp_arr[k].p[i * 2] ||
			    def->parts[i].type != cmp_arr[k].p[i * 2 + 1])
				break;
		if (i == def->part_count && cmp_arr[k].p[i * 2] == UINT32_MAX)
			return cmp_arr[k].f;
	}
	return tuple_compare_default_raw;
}

/* }}} tuple_compare */

/* {{{ tuple_compare_with_key */

/* Tuple with key comparator */
namespace /* local symbols */ {

template <int FLD_ID, int IDX, int TYPE, int ...MORE_TYPES>
struct FieldCompareWithKey {};

/*
 * Struct for comparison a tuple and a key. Why need to use struct instead of
 * template function - @sa struct FieldCompare description.
 * @param FLD_ID Index of the current field of the key.
 * @param IDX Index of the current key field of the tuple.
 * @param TYPE Type of the IDX field of the tuple.
 * @param IDX2 Index of the next key field of the tuple.
 * @param TYPE2 Type of the IDX2 field of the tuple.
 * @param MORE_TYPES Variable count of other pairs (IDX_i, TYPE_i), i > 2.
 */
template <int FLD_ID, int IDX, int TYPE, int IDX2, int TYPE2, int ...MORE_TYPES>
struct FieldCompareWithKey<FLD_ID, IDX, TYPE, IDX2, TYPE2, MORE_TYPES...>
{
	/*
	 * Recursively compare the MessagePack encoded tuple and the key.
	 * @param format Format of the tuple.
	 * @param tuple MessagePack encoded array of fields of the tuple.
	 * @param field_map Field map with offsets to key fields of the tuple.
	 * @param key FLD_ID part of the key.
	 * @param part_count Part count of the key.
	 * @param field IDX key field of the tuple.
	 */
	inline static int
	compare(const struct tuple_format *format, const char *tuple,
		const uint32_t *field_map,  const char *key,
		uint32_t part_count, const char *field)
	{
		int r;
		/*
		 * If the next key field of the tuple is right after the current
		 * one then mp_next is faster then lookup in field_map for the
		 * next key offset.
		 */
		if (IDX + 1 == IDX2) {
			r = field_compare_and_next<TYPE>(&field, &key);
			if (r || part_count == FLD_ID + 1)
				return r;
		} else {
			r = field_compare<TYPE>(&field, &key);
			if (r || part_count == FLD_ID + 1)
				return r;
			/*
			 * Get the next key field of the tuple and propagate the
			 * key.
			 */
			field = tuple_field_raw(format, tuple, field_map, IDX2);
			mp_next(&key);
		}
		/* Continue the comparison from next key fields. */
		return FieldCompareWithKey<FLD_ID + 1, IDX2, TYPE2,
					      MORE_TYPES...>::compare(format,
					      tuple, field_map, key, part_count,
					      field);
	}
};

/*
 * Special case of the main FieldCompareWithKey template. This not recursive
 * case works for simple one part keys.
 */
template <int FLD_ID, int IDX, int TYPE>
struct FieldCompareWithKey<FLD_ID, IDX, TYPE>
{
	/* Compare two single key fields. */
	inline static int
	compare(const struct tuple_format *, const char *, const uint32_t *,
		const char *key, uint32_t, const char *field)
	{
		return field_compare<TYPE>(&field, &key);
	}
};

/* @sa FieldCompareWithKey. */
template <int FLD_ID, int IDX, int TYPE, int ...MORE_TYPES>
struct TupleCompareWithKey
{
	static int
	compare(const struct tuple_format *format, const char *tuple,
		const uint32_t *field_map, const char *key, uint32_t part_count,
		const struct key_def *)
	{
		/* Part count can be 0 in wildcard searches. */
		if (part_count == 0)
			return 0;
		const char *field = tuple_field_raw(format, tuple, field_map,
						    IDX);
		return FieldCompareWithKey<FLD_ID, IDX, TYPE,
					   MORE_TYPES...>::compare(format,
					   tuple, field_map, key, part_count,
					   field);
	}
};

/*
 * Special case of the main TupleCompareWithKey template. This specialization
 * works if the first field of tuple is indexed, that is if first part of the
 * @param key_def is {fieldno = 0, type = ...}, so mp_decode_array call is
 * faster then tuple_field_raw with lookup to the field_map.
 */
template <int TYPE, int ...MORE_TYPES>
struct TupleCompareWithKey<0, 0, TYPE, MORE_TYPES...>
{
	static int
	compare(const struct tuple_format *format, const char *tuple,
		const uint32_t *field_map, const char *key, uint32_t part_count,
		const struct key_def *)
	{
		/* Part count can be 0 in wildcard searches. */
		if (part_count == 0)
			return 0;
		const char *field = tuple;
		mp_decode_array(&field);
		return FieldCompareWithKey<0, 0, TYPE, MORE_TYPES...>::
			compare(format, tuple, field_map, key, part_count,
				field);
	}
};

} /* end of anonymous namespace */

struct comparator_with_key_signature
{
	tuple_compare_with_key_raw_t f;
	uint32_t p[64];
};

#define KEY_COMPARATOR(...) \
	{ TupleCompareWithKey<0, __VA_ARGS__>::compare, { __VA_ARGS__ } },

static const comparator_with_key_signature cmp_wk_arr[] = {
	KEY_COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_UNSIGNED)
	KEY_COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_UNSIGNED)
	KEY_COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_UNSIGNED)
	KEY_COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_UNSIGNED)
	KEY_COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_STRING)
	KEY_COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_STRING)
	KEY_COMPARATOR(0, FIELD_TYPE_UNSIGNED, 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_STRING)
	KEY_COMPARATOR(0, FIELD_TYPE_STRING  , 1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_STRING)

	KEY_COMPARATOR(1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_UNSIGNED)
	KEY_COMPARATOR(1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_UNSIGNED)
	KEY_COMPARATOR(1, FIELD_TYPE_UNSIGNED, 2, FIELD_TYPE_STRING)
	KEY_COMPARATOR(1, FIELD_TYPE_STRING  , 2, FIELD_TYPE_STRING)
};

#undef KEY_COMPARATOR

static tuple_compare_with_key_raw_t
tuple_compare_with_key_create_raw(const struct key_def *def)
{
	for (uint32_t k = 0; k < sizeof(cmp_wk_arr) / sizeof(cmp_wk_arr[0]);
	     k++) {

		uint32_t i = 0;
		for (; i < def->part_count; i++) {

			if (def->parts[i].fieldno != cmp_wk_arr[k].p[i * 2] ||
			    def->parts[i].type != cmp_wk_arr[k].p[i * 2 + 1]) {

				break;
			}
		}
		if (i == def->part_count)
			return cmp_wk_arr[k].f;
	}
	return tuple_compare_with_key_default_raw;
}

static inline int
tuple_compare_from_raw(const struct tuple *tuple_a, const struct tuple *tuple_b,
		       const struct key_def *key_def)
{
	return key_def->tuple_compare_raw(tuple_format(tuple_a), tuple_a->data,
					  (uint32_t *) tuple_a,
					  tuple_format(tuple_b), tuple_b->data,
					  (uint32_t *) tuple_b, key_def);
}

static inline int
tuple_compare_with_key_from_raw(const struct tuple *tuple, const char *key,
				uint32_t part_count,
				const struct key_def *key_def)
{
	return key_def->tuple_compare_with_key_raw(tuple_format(tuple),
						   tuple->data,
						   (uint32_t *) tuple, key,
						   part_count, key_def);
}

void
tuple_compare_init(struct key_def *key_def)
{
	key_def->tuple_compare = tuple_compare_from_raw;
	key_def->tuple_compare_with_key = tuple_compare_with_key_from_raw;

	key_def->tuple_compare_raw = tuple_compare_create_raw(key_def);
	key_def->tuple_compare_with_key_raw =
		tuple_compare_with_key_create_raw(key_def);
}

/* }}} tuple_compare_with_key */
