/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
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

/*
 *
 * This file contains functions used to access the internal hash tables
 * of user defined functions and collation sequences.
 */

#include "box/coll_id_cache.h"
#include "sqlInt.h"
#include "box/session.h"

struct coll *
sql_get_coll_seq(Parse *parser, const char *name, uint32_t *coll_id)
{
	if (name == NULL) {
		*coll_id = COLL_NONE;
		return coll_by_id(COLL_NONE)->coll;
	}
	struct coll_id *p = coll_by_name(name, strlen(name));
	if (p == NULL) {
		diag_set(ClientError, ER_NO_SUCH_COLLATION, name);
		parser->is_aborted = true;
		return NULL;
	} else {
		*coll_id = p->id;
		return p->coll;
	}
}

/* During the search for the best function definition, this procedure
 * is called to test how well the function passed as the first argument
 * matches the request for a function with nArg arguments in a system
 * that uses encoding enc. The value returned indicates how well the
 * request is matched. A higher value indicates a better match.
 *
 * If nArg is -1 that means to only return a match (non-zero) if p->nArg
 * is also -1.  In other words, we are searching for a function that
 * takes a variable number of arguments.
 *
 * If nArg is -2 that means that we are searching for any function
 * regardless of the number of arguments it uses, so return a positive
 * match score for any
 *
 * The returned value is always between 0 and 6, as follows:
 *
 * 0: Not a match.
 * 1: UTF8/16 conversion required and function takes any number of arguments.
 * 2: UTF16 byte order change required and function takes any number of args.
 * 3: encoding matches and function takes any number of arguments
 * 4: UTF8/16 conversion required - argument count matches exactly
 * 5: UTF16 byte order conversion required - argument count matches exactly
 * 6: Perfect match:  encoding and argument count match exactly.
 *
 * If nArg==(-2) then any function with a non-null xSFunc is
 * a perfect match and any function with xSFunc NULL is
 * a non-match.
 */
#define FUNC_PERFECT_MATCH 4	/* The score for a perfect match */
static int
matchQuality(FuncDef * p,	/* The function we are evaluating for match quality */
	     int nArg		/* Desired number of arguments.  (-1)==any */
    )
{
	int match;

	/* nArg of -2 is a special case */
	if (nArg == (-2))
		return (p->xSFunc == 0) ? 0 : FUNC_PERFECT_MATCH;

	/* Wrong number of arguments means "no match" */
	if (p->nArg != nArg && p->nArg >= 0)
		return 0;

	/* Give a better score to a function with a specific number of arguments
	 * than to function that accepts any number of arguments.
	 */
	if (p->nArg == nArg) {
		match = 4;
	} else {
		match = 1;
	}

	return match;
}

/*
 * Search a FuncDefHash for a function with the given name.  Return
 * a pointer to the matching FuncDef if found, or 0 if there is no match.
 */
static FuncDef *
functionSearch(int h,		/* Hash of the name */
	       const char *zFunc	/* Name of function */
    )
{
	FuncDef *p;
	for (p = sqlBuiltinFunctions.a[h]; p; p = p->u.pHash) {
		if (sqlStrICmp(p->zName, zFunc) == 0) {
			return p;
		}
	}
	return 0;
}

/**
 * Cache function is used to organise sqlBuiltinFunctions table.
 * @param func_name The name of builtin function.
 * @param func_name_len The length of the @a name.
 * @retval Hash value is calculated for given name.
 */
static int
sql_builtin_func_name_hash(const char *func_name, uint32_t func_name_len)
{
	return (sqlUpperToLower[(u8) func_name[0]] +
		func_name_len) % SQL_FUNC_HASH_SZ;
}

/*
 * Insert a new FuncDef into a FuncDefHash hash table.
 */
void
sqlInsertBuiltinFuncs(FuncDef * aDef,	/* List of global functions to be inserted */
			  int nDef	/* Length of the apDef[] list */
    )
{
	int i;
	for (i = 0; i < nDef; i++) {
		FuncDef *pOther;
		const char *zName = aDef[i].zName;
		int nName = sqlStrlen30(zName);
		int h = sql_builtin_func_name_hash(zName, nName);
		pOther = functionSearch(h, zName);
		if (pOther) {
			assert(pOther != &aDef[i] && pOther->pNext != &aDef[i]);
			aDef[i].pNext = pOther->pNext;
			pOther->pNext = &aDef[i];
		} else {
			aDef[i].pNext = 0;
			aDef[i].u.pHash = sqlBuiltinFunctions.a[h];
			sqlBuiltinFunctions.a[h] = &aDef[i];
		}
	}
}

struct FuncDef *
sql_find_function(struct sql *db, const char *func_name, int arg_count,
		  bool is_builtin, bool is_create)
{
	assert(arg_count >= -2);
	assert(arg_count >= -1 || !is_create);
	assert(!is_create || !is_builtin);
	uint32_t func_name_len = strlen(func_name);
	/*
	 * The 'score' of the best match is calculated
	 * with matchQuality estimator.
	 */
	int func_score = 0;
	/*
	 * The pointer of a function having the highest 'score'
	 * for given name and arg_count parameters.
	 */
	struct FuncDef *func = NULL;
	if (is_builtin)
		goto lookup_for_builtin;
	/* Search amongst the user-defined functions. */
	for (struct FuncDef *p = sqlHashFind(&db->aFunc, func_name);
	     p != NULL; p = p->pNext) {
		int score = matchQuality(p, arg_count);
		if (score > func_score) {
			func = p;
			func_score = score;
		}
	}
	if (!is_create && func == NULL) {
lookup_for_builtin:
		func_score = 0;
		int h = sql_builtin_func_name_hash(func_name, func_name_len);
		for (struct FuncDef *p = functionSearch(h, func_name);
		     p != NULL; p = p->pNext) {
			int score = matchQuality(p, arg_count);
			if (score > func_score) {
				func = p;
				func_score = score;
			}
		}
	}
	/*
	 * If the is_create parameter is true and the search did
	 * not reveal an exact match for the name, number of
	 * arguments, then add a new entry to the hash table and
	 * return it.
	 */
	if (is_create && func_score < FUNC_PERFECT_MATCH) {
		uint32_t func_sz = sizeof(func[0]) + func_name_len + 1;
		func = sqlDbMallocZero(db, func_sz);
		if (func == NULL) {
			diag_set(OutOfMemory, func_sz, "sqlDbMallocZero",
				 "func");
			return NULL;
		}
		func->zName = (const char *) &func[1];
		func->nArg = (u16) arg_count;
		func->funcFlags = 0;
		memcpy((char *)&func[1], func_name, func_name_len + 1);
		struct FuncDef *old_func =
		    (struct FuncDef *) sqlHashInsert(&db->aFunc, func->zName,
						     func);
		if (old_func == func) {
			sqlDbFree(db, func);
			sqlOomFault(db);
			diag_set(OutOfMemory, func_sz, "sqlHashInsert",
				 "func");
			return NULL;
		} else {
			func->pNext = old_func;
		}
	}
	return func != NULL &&
	       (func->xSFunc != NULL || is_create || is_builtin) ? func : NULL;
}
