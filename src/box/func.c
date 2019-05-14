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
#include "func.h"
#include "trivia/config.h"
#include "assoc.h"
#include "lua/trigger.h"
#include "lua/utils.h"
#include "error.h"
#include "diag.h"
#include "fiber.h"
#include "schema.h"
#include <dlfcn.h>

/**
 * Parsed symbol and package names.
 */
struct func_name {
	/** Null-terminated symbol name, e.g. "func" for "mod.submod.func" */
	const char *sym;
	/** Package name, e.g. "mod.submod" for "mod.submod.func" */
	const char *package;
	/** A pointer to the last character in ->package + 1 */
	const char *package_end;
};

/***
 * Split function name to symbol and package names.
 * For example, str = foo.bar.baz => sym = baz, package = foo.bar
 * @param str function name, e.g. "module.submodule.function".
 * @param[out] name parsed symbol and package names.
 */
static void
func_split_name(const char *str, struct func_name *name)
{
	name->package = str;
	name->package_end = strrchr(str, '.');
	if (name->package_end != NULL) {
		/* module.submodule.function => module.submodule, function */
		name->sym = name->package_end + 1; /* skip '.' */
	} else {
		/* package == function => function, function */
		name->sym = name->package;
		name->package_end = str + strlen(str);
	}
}

/**
 * Arguments for luaT_module_find used by lua_cpcall()
 */
struct module_find_ctx {
	const char *package;
	const char *package_end;
	char *path;
	size_t path_len;
};

/**
 * A cpcall() helper for module_find()
 */
static int
luaT_module_find(lua_State *L)
{
	struct module_find_ctx *ctx = (struct module_find_ctx *)
		lua_topointer(L, 1);

	/*
	 * Call package.searchpath(name, package.cpath) and use
	 * the path to the function in dlopen().
	 */
	lua_getglobal(L, "package");

	lua_getfield(L, -1, "search");

	/* Argument of search: name */
	lua_pushlstring(L, ctx->package, ctx->package_end - ctx->package);

	lua_call(L, 1, 1);
	if (lua_isnil(L, -1))
		return luaL_error(L, "module not found");
	/* Convert path to absolute */
	char resolved[PATH_MAX];
	if (realpath(lua_tostring(L, -1), resolved) == NULL) {
		diag_set(SystemError, "realpath");
		return luaT_error(L);
	}

	snprintf(ctx->path, ctx->path_len, "%s", resolved);
	return 0;
}

/**
 * Find path to module using Lua's package.cpath
 * @param package package name
 * @param package_end a pointer to the last byte in @a package + 1
 * @param[out] path path to shared library
 * @param path_len size of @a path buffer
 * @retval 0 on success
 * @retval -1 on error, diag is set
 */
static int
module_find(const char *package, const char *package_end, char *path,
	    size_t path_len)
{
	struct module_find_ctx ctx = { package, package_end, path, path_len };
	lua_State *L = tarantool_L;
	int top = lua_gettop(L);
	if (luaT_cpcall(L, luaT_module_find, &ctx) != 0) {
		int package_len = (int) (package_end - package);
		diag_set(ClientError, ER_LOAD_MODULE, package_len, package,
			 lua_tostring(L, -1));
		lua_settop(L, top);
		return -1;
	}
	assert(top == lua_gettop(L)); /* cpcall discard results */
	return 0;
}

static struct mh_strnptr_t *modules = NULL;

static void
module_gc(struct module *module);

int
module_init(void)
{
	modules = mh_strnptr_new();
	if (modules == NULL) {
		diag_set(OutOfMemory, sizeof(*modules), "malloc",
			  "modules hash table");
		return -1;
	}
	return 0;
}

void
module_free(void)
{
	while (mh_size(modules) > 0) {
		mh_int_t i = mh_first(modules);
		struct module *module =
			(struct module *) mh_strnptr_node(modules, i)->val;
		/* Can't delete modules if they have active calls */
		module_gc(module);
	}
	mh_strnptr_delete(modules);
}

/**
 * Look up a module in the modules cache.
 */
static struct module *
module_cache_find(const char *name, const char *name_end)
{
	mh_int_t i = mh_strnptr_find_inp(modules, name, name_end - name);
	if (i == mh_end(modules))
		return NULL;
	return (struct module *)mh_strnptr_node(modules, i)->val;
}

/**
 * Save module to the module cache.
 */
static inline int
module_cache_put(const char *name, const char *name_end, struct module *module)
{
	size_t name_len = name_end - name;
	uint32_t name_hash = mh_strn_hash(name, name_len);
	const struct mh_strnptr_node_t strnode = {
		name, name_len, name_hash, module};

	if (mh_strnptr_put(modules, &strnode, NULL, NULL) == mh_end(modules)) {
		diag_set(OutOfMemory, sizeof(strnode), "malloc", "modules");
		return -1;
	}
	return 0;
}

/**
 * Delete a module from the module cache
 */
static void
module_cache_del(const char *name, const char *name_end)
{
	mh_int_t i = mh_strnptr_find_inp(modules, name, name_end - name);
	if (i == mh_end(modules))
		return;
	mh_strnptr_del(modules, i, NULL);
}

/*
 * Load a dso.
 * Create a new symlink based on temporary directory and try to
 * load via this symink to load a dso twice for cases of a function
 * reload.
 */
static struct module *
module_load(const char *package, const char *package_end)
{
	char path[PATH_MAX];
	if (module_find(package, package_end, path, sizeof(path)) != 0)
		return NULL;

	struct module *module = (struct module *) malloc(sizeof(*module));
	if (module == NULL) {
		diag_set(OutOfMemory, sizeof(struct module), "malloc",
			 "struct module");
		return NULL;
	}
	rlist_create(&module->funcs);
	module->calls = 0;
	module->is_unloading = false;
	char dir_name[] = "/tmp/tntXXXXXX";
	if (mkdtemp(dir_name) == NULL) {
		diag_set(SystemError, "failed to create unique dir name");
		goto error;
	}
	char load_name[PATH_MAX + 1];
	snprintf(load_name, sizeof(load_name), "%s/%.*s." TARANTOOL_LIBEXT,
		 dir_name, (int)(package_end - package), package);
	if (symlink(path, load_name) < 0) {
		diag_set(SystemError, "failed to create dso link");
		goto error;
	}
	module->handle = dlopen(load_name, RTLD_NOW | RTLD_LOCAL);
	if (unlink(load_name) != 0)
		say_warn("failed to unlink dso link %s", load_name);
	if (rmdir(dir_name) != 0)
		say_warn("failed to delete temporary dir %s", dir_name);
	if (module->handle == NULL) {
		int package_len = (int) (package_end - package_end);
		diag_set(ClientError, ER_LOAD_MODULE, package_len,
			  package, dlerror());
		goto error;
	}

	return module;
error:
	free(module);
	return NULL;
}

static void
module_delete(struct module *module)
{
	dlclose(module->handle);
	TRASH(module);
	free(module);
}

/*
 * Check if a dso is unused and can be closed.
 */
static void
module_gc(struct module *module)
{
	if (!module->is_unloading || !rlist_empty(&module->funcs) ||
	     module->calls != 0)
		return;
	module_delete(module);
}

/*
 * Import a function from the module.
 */
static box_function_f
module_sym(struct module *module, const char *name)
{
	box_function_f f = (box_function_f)dlsym(module->handle, name);
	if (f == NULL) {
		diag_set(ClientError, ER_LOAD_FUNCTION, name, dlerror());
		return NULL;
	}
	return f;
}

int
module_reload(const char *package, const char *package_end, struct module **module)
{
	struct module *old_module = module_cache_find(package, package_end);
	if (old_module == NULL) {
		/* Module wasn't loaded - do nothing. */
		*module = NULL;
		return 0;
	}

	struct module *new_module = module_load(package, package_end);
	if (new_module == NULL)
		return -1;

	struct func *func, *tmp_func;
	rlist_foreach_entry_safe(func, &old_module->funcs, item, tmp_func) {
		struct func_name name;
		func_split_name(func->def->name, &name);
		func->func = module_sym(new_module, name.sym);
		if (func->func == NULL)
			goto restore;
		func->module = new_module;
		rlist_move(&new_module->funcs, &func->item);
	}
	module_cache_del(package, package_end);
	if (module_cache_put(package, package_end, new_module) != 0)
		goto restore;
	old_module->is_unloading = true;
	module_gc(old_module);
	*module = new_module;
	return 0;
restore:
	/*
	 * Some old-dso func can't be load from new module, restore old
	 * functions.
	 */
	do {
		struct func_name name;
		func_split_name(func->def->name, &name);
		func->func = module_sym(old_module, name.sym);
		if (func->func == NULL) {
			/*
			 * Something strange was happen, an early loaden
			 * function was not found in an old dso.
			 */
			panic("Can't restore module function, "
			      "server state is inconsistent");
		}
		func->module = old_module;
		rlist_move(&old_module->funcs, &func->item);
	} while (func != rlist_first_entry(&old_module->funcs,
					   struct func, item));
	assert(rlist_empty(&new_module->funcs));
	module_delete(new_module);
	return -1;
}

/**
 * Assemble a Lua function object on Lua stack and return
 * the reference.
 * Returns func object reference on success, LUA_REFNIL otherwise.
 */
static int
func_lua_code_load(struct func_def *def)
{
	int rc = LUA_REFNIL;
	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	struct lua_State *L = lua_newthread(tarantool_L);
	int coro_ref = luaL_ref(tarantool_L, LUA_REGISTRYINDEX);

	/*
	 * Assemble a Lua function object with luaL_loadstring of
	 * special 'return FUNCTION_BODY' expression and call it.
	 * Set default sandbox to configure it to use only a
	 * limited number of functions and modules.
	 */
	const char *load_pref = "return ";
	uint32_t load_str_sz = strlen(load_pref) + strlen(def->body) + 1;
	char *load_str = region_alloc(region, load_str_sz);
	if (load_str == NULL) {
		diag_set(OutOfMemory, load_str_sz, "region", "load_str");
		goto end;
	}
	sprintf(load_str, "%s%s", load_pref, def->body);
	if (luaL_loadstring(L, load_str) != 0 ||
	    lua_pcall(L, 0, 1, 0) != 0 || !lua_isfunction(L, -1) ||
	    luaT_get_sandbox(L) != 0) {
		diag_set(ClientError, ER_LOAD_FUNCTION, def->name,
			def->body);
		goto end;
	}
	lua_setfenv(L, -2);
	rc = luaL_ref(L, LUA_REGISTRYINDEX);
end:
	region_truncate(region, region_svp);
	luaL_unref(L, LUA_REGISTRYINDEX, coro_ref);
	return rc;
}

struct func *
func_new(struct func_def *def)
{
	struct func *func = (struct func *) malloc(sizeof(struct func));
	if (func == NULL) {
		diag_set(OutOfMemory, sizeof(*func), "malloc", "func");
		return NULL;
	}
	func->def = def;
	/** Nobody has access to the function but the owner. */
	memset(func->access, 0, sizeof(func->access));
	/*
	 * Do not initialize the privilege cache right away since
	 * when loading up a function definition during recovery,
	 * user cache may not be filled up yet (space _user is
	 * recovered after space _func), so no user cache entry
	 * may exist yet for such user.  The cache will be filled
	 * up on demand upon first access.
	 *
	 * Later on consistency of the cache is ensured by DDL
	 * checks (see user_has_data()).
	 */
	func->owner_credentials.auth_token = BOX_USER_MAX; /* invalid value */
	func->func = NULL;
	func->module = NULL;
	if (func->def->body != NULL) {
		func->lua_func_ref = func_lua_code_load(def);
		if (func->lua_func_ref == LUA_REFNIL) {
			free(func);
			return NULL;
		}
	} else {
		func->lua_func_ref = LUA_REFNIL;
	}
	return func;
}

static void
func_unload(struct func *func)
{
	if (func->module) {
		rlist_del(&func->item);
		if (rlist_empty(&func->module->funcs)) {
			struct func_name name;
			func_split_name(func->def->name, &name);
			module_cache_del(name.package, name.package_end);
		}
		module_gc(func->module);
	}
	if (func->lua_func_ref != -1)
		luaL_unref(tarantool_L, LUA_REGISTRYINDEX, func->lua_func_ref);
	func->module = NULL;
	func->func = NULL;
	func->lua_func_ref = -1;
}

/**
 * Resolve func->func (find the respective DLL and fetch the
 * symbol from it).
 */
static int
func_load(struct func *func)
{
	assert(func->func == NULL);

	struct func_name name;
	func_split_name(func->def->name, &name);

	struct module *module = module_cache_find(name.package,
						  name.package_end);
	if (module == NULL) {
		/* Try to find loaded module in the cache */
		module = module_load(name.package, name.package_end);
		if (module == NULL)
			return -1;
		if (module_cache_put(name.package, name.package_end, module)) {
			module_delete(module);
			return -1;
		}
	}

	func->func = module_sym(module, name.sym);
	if (func->func == NULL)
		return -1;
	func->module = module;
	rlist_add(&module->funcs, &func->item);
	return 0;
}

int
func_call(struct func *func, box_function_ctx_t *ctx, const char *args,
	  const char *args_end)
{
	if (func->func == NULL) {
		if (func_load(func) != 0)
			return -1;
	}

	/* Module can be changed after function reload. */
	struct module *module = func->module;
	assert(module != NULL);
	++module->calls;
	int rc = func->func(ctx, args, args_end);
	--module->calls;
	module_gc(module);
	return rc;
}

void
func_delete(struct func *func)
{
	func_unload(func);
	free(func->def);
	free(func);
}

void
func_capture_module(struct func *new_func, struct func *old_func)
{
	new_func->module = old_func->module;
	new_func->func = old_func->func;
	old_func->module = NULL;
	old_func->func = NULL;
}

static void
box_lua_func_new(struct lua_State *L, struct func *func)
{
	lua_getfield(L, LUA_GLOBALSINDEX, "box");
	lua_getfield(L, -1, "schema");
	if (!lua_istable(L, -1)) {
		lua_pop(L, 1); /* pop nil */
		lua_newtable(L);
		lua_setfield(L, -2, "schema");
		lua_getfield(L, -1, "schema");
	}
	lua_getfield(L, -1, "func");
	if (!lua_istable(L, -1)) {
		lua_pop(L, 1); /* pop nil */
		lua_newtable(L);
		lua_setfield(L, -2, "func");
		lua_getfield(L, -1, "func");
	}
	lua_getfield(L, -1, "persistent");
	if (!lua_istable(L, -1)) {
		lua_pop(L, 1); /* pop nil */
		lua_newtable(L);
		lua_setfield(L, -2, "persistent");
		lua_getfield(L, -1, "persistent");
	}
	lua_rawgeti(L, -1, func->def->fid);
	if (lua_isnil(L, -1)) {
		/*
		 * If the function already exists, modify it,
		 * rather than create a new one -- to not
		 * invalidate Lua variable references to old func
		 * outside the box.schema.func[].
		 */
		lua_pop(L, 1);
		lua_newtable(L);
		lua_rawseti(L, -2, func->def->fid);
		lua_rawgeti(L, -1, func->def->fid);
	} else {
		/* Clear the reference to old space by old name. */
		lua_getfield(L, -1, "name");
		lua_pushnil(L);
		lua_settable(L, -4);
	}

	int top = lua_gettop(L);
	lua_pushstring(L, "id");
	lua_pushnumber(L, func->def->fid);
	lua_settable(L, top);

	lua_pushstring(L, "name");
	lua_pushstring(L, func->def->name);
	lua_settable(L, top);

	lua_pushstring(L, "is_deterministic");
	lua_pushboolean(L, func->def->opts.is_deterministic);
	lua_settable(L, top);

	lua_pushstring(L, "call");
	lua_rawgeti(L, LUA_REGISTRYINDEX, func->lua_func_ref);
	lua_settable(L, top);

	lua_setfield(L, -2, func->def->name);

	lua_pop(L, 4); /* box, schema, func, persistent */
}


static void
box_lua_func_delete(struct lua_State *L, uint32_t fid)
{
	lua_getfield(L, LUA_GLOBALSINDEX, "box");
	lua_getfield(L, -1, "schema");
	lua_getfield(L, -1, "func");
	lua_getfield(L, -1, "persistent");
	lua_rawgeti(L, -1, fid);
	if (!lua_isnil(L, -1)) {
		lua_getfield(L, -1, "name");
		lua_pushnil(L);
		lua_rawset(L, -4);
		lua_pop(L, 1); /* pop func */

		lua_pushnil(L);
		lua_rawseti(L, -2, fid);
	} else {
		lua_pop(L, 1);
	}
	lua_pop(L, 4); /* box, schema, func, persistent */
}

static void
box_lua_func_new_or_delete(struct trigger *trigger, void *event)
{
	struct lua_State *L = (struct lua_State *) trigger->data;
	uint32_t fid = (uint32_t)(uintptr_t)event;
	struct func *func = func_by_id(fid);
	/* Export only persistent Lua functions. */
	if (func != NULL && func->def->language == FUNC_LANGUAGE_LUA &&
	    func->def->body != NULL)
		box_lua_func_new(L, func);
	else
		box_lua_func_delete(L, fid);
}

static struct trigger on_alter_func_in_lua = {
	RLIST_LINK_INITIALIZER, box_lua_func_new_or_delete, NULL, NULL
};

void
box_lua_func_init(struct lua_State *L)
{
	/*
	 * Register the trigger that will push persistent
	 * Lua functions objects to Lua.
	 */
	on_alter_func_in_lua.data = L;
	trigger_add(&on_alter_func, &on_alter_func_in_lua);
}
