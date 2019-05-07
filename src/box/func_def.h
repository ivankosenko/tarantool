#ifndef TARANTOOL_BOX_FUNC_DEF_H_INCLUDED
#define TARANTOOL_BOX_FUNC_DEF_H_INCLUDED
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

#include "trivia/util.h"
#include "opt_def.h"
#include <stdbool.h>

/**
 * The supported language of the stored function.
 */
enum func_language {
	FUNC_LANGUAGE_LUA,
	FUNC_LANGUAGE_C,
	func_language_MAX,
};

extern const char *func_language_strs[];

/** Function options. */
struct func_opts {
	/**
	 * Whether the routine is deterministic (can produce
	 * only one result for a given list of parameters)
	 * or not.
	 */
	bool is_deterministic;
};

extern const struct func_opts func_opts_default;
extern const struct opt_def func_opts_reg[];

/**
 * Definition of a function. Function body is not stored
 * or replicated (yet).
 */
struct func_def {
	/** Function id. */
	uint32_t fid;
	/** Owner of the function. */
	uint32_t uid;
	/** Function name. */
	char *name;
	/** Definition of the routine. */
	char *body;
	/**
	 * True if the function requires change of user id before
	 * invocation.
	 */
	bool setuid;
	/**
	 * The language of the stored function.
	 */
	enum func_language language;
	/** The function options. */
	struct func_opts opts;
};

/**
 * @param name_len length of func_def->name
 * @returns size in bytes needed to allocate for struct func_def
 * for a function of length @a a name_len.
 */
static inline size_t
func_def_sizeof(uint32_t name_len, uint32_t body_len)
{
	/* +1 for '\0' name terminating. */
	size_t sz = sizeof(struct func_def) + name_len + 1;
	if (body_len > 0)
		sz += body_len + 1;
	return sz;
}

/** Create index options using default values. */
static inline void
func_opts_create(struct func_opts *opts)
{
	*opts = func_opts_default;
}

/**
 * API of C stored function.
 */
typedef struct box_function_ctx box_function_ctx_t;
typedef int (*box_function_f)(box_function_ctx_t *ctx,
	     const char *args, const char *args_end);

#endif /* TARANTOOL_BOX_FUNC_DEF_H_INCLUDED */
