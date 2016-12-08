#ifndef TARANTOOL_BOX_INFO_H_INCLUDED
#define TARANTOOL_BOX_INFO_H_INCLUDED
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

#include <stdint.h>
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * Functions for creation and filling lua tables.
 * All functions assume that there is at least one parent table on
 * a lua stack.
 */

/**
 * Create a new table with the string header.
 * {
 *     ...,
 *     name: {
 *         -- new table, data will inserted here.
 *     }
 * }
 * @param ctx  lua_State.
 * @param name Name of the new table.
 */
void
info_begin_str(void *ctx, const char *name);

/**
 * Create a new table with the uint64 header.
 * {
 *     ...,
 *     val: {
 *         -- new table, data will inserted here.
 *     }
 * }
 * @param ctx lua_State.
 * @param val Header of the table.
 */
void
info_begin_u64(void *ctx, uint64_t val);

/**
 * Create a new table with the uint32 header.
 * {
 *     ...,
 *     val: {
 *         -- new table, data will inserted here.
 *     }
 * }
 * @param ctx lua_State.
 * @param val Header of the table.
 */
void
info_begin_u32(void *ctx, uint32_t val);

/**
 * Close the current table.
 * {
 *     ...,
 *     cur_tb: { ... } -- table closed.
 *     -- new data will be inserted here.
 * }
 * @param ctx lua_State.
 */
void
info_end(void *ctx);

/**
 * Set a new key value pair to the current table:
 * current_table[name] = val.
 *
 * @param ctx  lua_State
 * @param name Name of the new field.
 * @param val  String value of the new field.
 */
void
info_push_str(void *ctx, const char *name, const char *val);

/**
 * Set a new key value pair to the current table:
 * current_table[name] = val.
 *
 * @param ctx  lua_State
 * @param name Name of the new field.
 * @param val  Uint32 value of the new field.
 */
void
info_push_u32(void *ctx, const char *name, uint32_t val);

/**
 * Set a new key value pair to the current table:
 * current_table[name] = val.
 *
 * @param ctx  lua_State
 * @param name Name of the new field.
 * @param val  Uint64 value of the new field.
 */
void
info_push_u64(void *ctx, const char *name, uint64_t val);

/**
 * Set a new key value pair to the current table:
 * current_table[name] = val.
 *
 * @param ctx  lua_State
 * @param name Name of the new field.
 * @param val  Int64 value of the new field.
 */
void
info_push_i64(void *ctx, const char *name, int64_t val);

/**
 * Set a new key value pair to the current table:
 * current_table[name] = f.
 *
 * @param ctx  lua_State
 * @param name Name of the new field.
 * @param f    Bool value of the new field.
 */
void
info_push_bool(void *ctx, const char *name, bool f);

#if defined(__cplusplus)
}
#endif /* defined(__cplusplus) */

#endif
