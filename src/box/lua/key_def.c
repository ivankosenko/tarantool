/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
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

#include "box/lua/key_def.h"

#include <lua.h>
#include <lauxlib.h>
#include "fiber.h"
#include "diag.h"
#include "tuple.h"
#include "box/key_def.h"
#include "box/box.h"
#include "box/tuple.h"
#include "box/coll_id_cache.h"
#include "lua/utils.h"
#include "box/tuple_format.h" /* TUPLE_INDEX_BASE */

static uint32_t key_def_type_id = 0;

/**
 * Set key_part_def from a table on top of a Lua stack.
 *
 * When successful return 0, otherwise return -1 and set a diag.
 */
static int
luaT_key_def_set_part(struct lua_State *L, struct key_part_def *parts,
		      int part_idx)
{
	struct key_part_def *part = &parts[part_idx];
	/* Set part->fieldno. */
	lua_pushstring(L, "fieldno");
	lua_gettable(L, -2);
	if (lua_isnil(L, -1)) {
		diag_set(IllegalParams, "fieldno must not be nil");
		return -1;
	}
	/*
	 * Transform one-based Lua fieldno to zero-based
	 * fieldno to use in key_def_new().
	 */
	part->fieldno = lua_tointeger(L, -1) - TUPLE_INDEX_BASE;
	lua_pop(L, 1);

	/* Set part->type. */
	lua_pushstring(L, "type");
	lua_gettable(L, -2);
	if (lua_isnil(L, -1)) {
		diag_set(IllegalParams, "type must not be nil");
		return -1;
	}
	size_t type_len;
	const char *type_name = lua_tolstring(L, -1, &type_len);
	lua_pop(L, 1);
	part->type = field_type_by_name(type_name, type_len);
	switch (part->type) {
	case FIELD_TYPE_ANY:
	case FIELD_TYPE_ARRAY:
	case FIELD_TYPE_MAP:
		/* Tuple comparators don't support these types. */
		diag_set(IllegalParams, "Unsupported field type: %s",
			 type_name);
		return -1;
	case field_type_MAX:
		diag_set(IllegalParams, "Unknown field type: %s", type_name);
		return -1;
	default:
		/* Pass though. */
		break;
	}

	/* Set part->is_nullable and part->nullable_action. */
	lua_pushstring(L, "is_nullable");
	lua_gettable(L, -2);
	if (lua_isnil(L, -1)) {
		part->is_nullable = false;
	} else {
		part->is_nullable = lua_toboolean(L, -1);
	}
	lua_pop(L, 1);

	/*
	 * Set part->coll_id using collation_id.
	 *
	 * The value will be checked in key_def_new().
	 */
	lua_pushstring(L, "collation_id");
	lua_gettable(L, -2);
	if (lua_isnil(L, -1))
		part->coll_id = COLL_NONE;
	else
		part->coll_id = lua_tointeger(L, -1);
	lua_pop(L, 1);

	/* Set part->coll_id using collation. */
	lua_pushstring(L, "collation");
	lua_gettable(L, -2);
	if (!lua_isnil(L, -1)) {
		/* Check for conflicting options. */
		if (part->coll_id != COLL_NONE) {
			diag_set(IllegalParams, "Conflicting options: "
				 "collation_id and collation");
			return -1;
		}

		size_t coll_name_len;
		const char *coll_name = lua_tolstring(L, -1, &coll_name_len);
		struct coll_id *coll_id = coll_by_name(coll_name,
						       coll_name_len);
		if (coll_id == NULL) {
			diag_set(IllegalParams, "Unknown collation: \"%s\"",
				 coll_name);
			return -1;
		}
		part->coll_id = coll_id->id;
	}
	lua_pop(L, 1);

	return 0;
}

void
lbox_push_key_part(struct lua_State *L, const struct key_part *part)
{
	lua_newtable(L);

	lua_pushstring(L, field_type_strs[part->type]);
	lua_setfield(L, -2, "type");

	lua_pushnumber(L, part->fieldno + TUPLE_INDEX_BASE);
	lua_setfield(L, -2, "fieldno");

	lua_pushboolean(L, part->is_nullable);
	lua_setfield(L, -2, "is_nullable");

	if (part->coll_id != COLL_NONE) {
		struct coll_id *coll_id = coll_by_id(part->coll_id);
		assert(coll_id != NULL);
		lua_pushstring(L, coll_id->name);
		lua_setfield(L, -2, "collation");
	}
}

struct key_def *
check_key_def(struct lua_State *L, int idx)
{
	if (lua_type(L, idx) != LUA_TCDATA)
		return NULL;

	uint32_t cdata_type;
	struct key_def **key_def_ptr = luaL_checkcdata(L, idx, &cdata_type);
	if (key_def_ptr == NULL || cdata_type != key_def_type_id)
		return NULL;
	return *key_def_ptr;
}

/**
 * Free a key_def from a Lua code.
 */
static int
lbox_key_def_gc(struct lua_State *L)
{
	struct key_def *key_def = check_key_def(L, 1);
	if (key_def == NULL)
		return 0;
	box_key_def_delete(key_def);
	return 0;
}

static int
lbox_key_def_extract_key(lua_State *L)
{
	struct key_def *key_def;
	struct tuple *tuple;
	if (lua_gettop(L) != 2 || (key_def = check_key_def(L, 1)) == NULL ||
	   (tuple = luaT_istuple(L, 2)) == NULL)
		return luaL_error(L, "Usage: key_def:extract_key(tuple)");

	uint32_t key_size;
	char *key = tuple_extract_key(tuple, key_def, &key_size);
	if (key == NULL)
		return luaT_error(L);

	struct tuple *ret =
		box_tuple_new(box_tuple_format_default(), key, key + key_size);
	if (ret == NULL)
		return luaT_error(L);
	luaT_pushtuple(L, ret);
	return 1;
}

static int
lbox_key_def_compare(lua_State *L)
{
	struct key_def *key_def;
	struct tuple *tuple_a, *tuple_b;
	if (lua_gettop(L) != 3 || (key_def = check_key_def(L, 1)) == NULL ||
	   (tuple_a = luaT_istuple(L, 2)) == NULL ||
	   (tuple_b = luaT_istuple(L, 3)) == NULL)
		return luaL_error(L, "Usage: key_def:compare(tuple_a, tuple_b)");

	int rc = tuple_compare(tuple_a, tuple_b, key_def);
	lua_pushinteger(L, rc);
	return 1;
}

static int
lbox_key_def_compare_with_key(lua_State *L)
{
	struct key_def *key_def;
	struct tuple *tuple, *key_tuple;
	if (lua_gettop(L) != 3 || (key_def = check_key_def(L, 1)) == NULL ||
	   (tuple = luaT_istuple(L, 2)) == NULL)
		goto usage_error;

	lua_remove(L, 1);
	lua_remove(L, 1);
	if (luaT_tuple_new(L, box_tuple_format_default()) != 1 ||
	    (key_tuple = luaT_istuple(L, -1)) == NULL)
		goto usage_error;

	const char *key = tuple_data(key_tuple);
	assert(mp_typeof(*key) == MP_ARRAY);
	uint32_t part_count = mp_decode_array(&key);

	int rc = tuple_compare_with_key(tuple, key, part_count, key_def);
	lua_pushinteger(L, rc);
	return 1;
usage_error:
	return luaL_error(L, "Usage: key_def:compare_with_key(tuple, key)");
}

static int
lbox_key_def_merge(lua_State *L)
{
	struct key_def *key_def_a, *key_def_b;
	if (lua_gettop(L) != 2 || (key_def_a = check_key_def(L, 1)) == NULL ||
	   (key_def_b = check_key_def(L, 2)) == NULL)
		return luaL_error(L, "Usage: key_def:merge(second_key_def)");

	struct key_def *new_key_def = key_def_merge(key_def_a, key_def_b);
	if (new_key_def == NULL)
		return luaT_error(L);

	*(struct key_def **) luaL_pushcdata(L, key_def_type_id) = new_key_def;
	lua_pushcfunction(L, lbox_key_def_gc);
	luaL_setcdatagc(L, -2);
	return 1;
}

static int
lbox_key_def_to_table(struct lua_State *L)
{
	struct key_def *key_def;
	if (lua_gettop(L) != 1 || (key_def = check_key_def(L, 1)) == NULL)
		return luaL_error(L, "Usage: key_def:to_table()");

	lua_createtable(L, key_def->part_count, 0);
	for (uint32_t i = 0; i < key_def->part_count; ++i) {
		lbox_push_key_part(L, &key_def->parts[i]);
		lua_rawseti(L, -2, i + 1);
	}
	return 1;
}

/**
 * Create a new key_def from a Lua table.
 *
 * Expected a table of key parts on the Lua stack. The format is
 * the same as box.space.<...>.index.<...>.parts or corresponding
 * net.box's one.
 *
 * Return the new key_def as cdata.
 */
static int
lbox_key_def_new(struct lua_State *L)
{
	if (lua_gettop(L) != 1 || lua_istable(L, 1) != 1)
		return luaL_error(L, "Bad params, use: key_def.new({"
				  "{fieldno = fieldno, type = type"
				  "[, is_nullable = <boolean>]"
				  "[, collation_id = <number>]"
				  "[, collation = <string>]}, ...}");

	uint32_t part_count = lua_objlen(L, 1);
	const ssize_t parts_size = sizeof(struct key_part_def) * part_count;
	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	struct key_part_def *parts = region_alloc(region, parts_size);
	if (parts == NULL) {
		diag_set(OutOfMemory, parts_size, "region", "parts");
		return luaT_error(L);
	}

	for (uint32_t i = 0; i < part_count; ++i) {
		lua_pushinteger(L, i + 1);
		lua_gettable(L, 1);
		if (luaT_key_def_set_part(L, parts, i) != 0) {
			region_truncate(region, region_svp);
			return luaT_error(L);
		}
	}

	struct key_def *key_def = key_def_new(parts, part_count);
	region_truncate(region, region_svp);
	if (key_def == NULL)
		return luaT_error(L);

	*(struct key_def **) luaL_pushcdata(L, key_def_type_id) = key_def;
	lua_pushcfunction(L, lbox_key_def_gc);
	luaL_setcdatagc(L, -2);

	return 1;
}

LUA_API int
luaopen_key_def(struct lua_State *L)
{
	luaL_cdef(L, "struct key_def;");
	key_def_type_id = luaL_ctypeid(L, "struct key_def&");

	/* Export C functions to Lua. */
	static const struct luaL_Reg meta[] = {
		{"new", lbox_key_def_new},
		{NULL, NULL}
	};
	luaL_register_module(L, "key_def", meta);

	lua_newtable(L); /* key_def.internal */
	lua_pushcfunction(L, lbox_key_def_extract_key);
	lua_setfield(L, -2, "extract_key");
	lua_pushcfunction(L, lbox_key_def_compare);
	lua_setfield(L, -2, "compare");
	lua_pushcfunction(L, lbox_key_def_compare_with_key);
	lua_setfield(L, -2, "compare_with_key");
	lua_pushcfunction(L, lbox_key_def_merge);
	lua_setfield(L, -2, "merge");
	lua_pushcfunction(L, lbox_key_def_to_table);
	lua_setfield(L, -2, "to_table");
	lua_setfield(L, -2, "internal");

	return 1;
}
