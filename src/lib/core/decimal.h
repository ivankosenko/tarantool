#ifndef TARANTOOL_LIB_CORE_DECIMAL_H_INCLUDED
#define TARANTOOL_LIB_CORE_DECIMAL_H_INCLUDED
/*
 * Copyright 2019, Tarantool AUTHORS, please see AUTHORS file.
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
#define TARANTOOL_MAX_DECIMAL_DIGITS 38
#define DECNUMDIGITS TARANTOOL_MAX_DECIMAL_DIGITS
#define DECSTRING_NO_EXPONENT
#include "third_party/decNumber/decNumber.h"

struct decimal {
	uint8_t precision;
	uint8_t scale;
	decNumber number;
};

struct decimal *
decimal_zero(struct decimal *dec, uint8_t precision, uint8_t scale);

struct decimal *
decimal_from_string(struct decimal *dec, const char *str, uint8_t precision,
		    uint8_t scale);

struct decimal *
decimal_from_int(struct decimal *dec, int32_t num, uint8_t precision,
		 uint8_t scale);

struct decimal *
decimal_from_uint(struct decimal *dec, uint32_t numb, uint8_t precision,
		  uint8_t scale);

char *
decimal_to_string(const struct decimal *dec, char *buf);

int32_t
decimal_to_int(const struct decimal *dec);

uint32_t
decimal_to_uint(const struct decimal *dec);

int
decimal_compare(const struct decimal *lhs, const struct decimal *rhs);

struct decimal *
decimal_abs(struct decimal *res, const struct decimal *dec);

struct decimal *
decimal_add(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs);

struct decimal *
decimal_sub(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs);

struct decimal *
decimal_mul(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs);

struct decimal *
decimal_div(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs);

struct decimal *
decimal_log10(struct decimal *res, const struct decimal *lhs);

struct decimal *
decimal_ln(struct decimal *res, const struct decimal *lhs);

struct decimal *
decimal_pow(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs);

struct decimal *
decimal_exp(struct decimal *res, const struct decimal *lhs);

struct decimal *
decimal_sqrt(struct decimal *res, const struct decimal *lhs);

#endif /* TARANTOOL_LIB_CORE_DECIMAL_H_INCLUDED */
