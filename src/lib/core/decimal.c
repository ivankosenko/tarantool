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

#include "decimal.h"
#include "third_party/decNumber/decContext.h"
#include <stdlib.h>
#include <assert.h>

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) > (b) ? (b) : (a))

/** A single context for all the decimal operations. */
static decContext decimal_context = {
	/* Maximum precision during operations. */
	TARANTOOL_MAX_DECIMAL_DIGITS,
	/* Maximum decimal lagarithm of the number. */
	TARANTOOL_MAX_DECIMAL_DIGITS - 1,
	/* Minimum decimal logarithm of the number. */
	-TARANTOOL_MAX_DECIMAL_DIGITS + 1,
	/* Rounding mode: .5 rounds away from 0. */
	DEC_ROUND_HALF_UP,
	/* Turn off signalling for failed operations. */
	0,
	/* Status holding occured events. Initially empty. */
	0,
	/*
	 * Clamp exponenents when they get too big.
	 * Doesn't really happen since they are shifted
	 * on each operation.
	 */
	1
};

/**
 * Check whether there were errors during the operation
 * and clear the status for future checks.
 *
 * @return 0 if ok, bitwise or of decNumber errors, if any.
 */
static inline uint32_t
decimal_check_op_status()
{
	uint32_t status = decContextGetStatus(&decimal_context);
	decContextZeroStatus(&decimal_context);
	assert(!(status & DEC_Clamped));
	/*
	 * Clear warnings. Rounding is ok, subnormal values will get rounded in
	 * the folowwing decimal_finalize() code.
	 */
	return status & ~(uint32_t)(DEC_Inexact | DEC_Rounded | DEC_Subnormal);
}

/**
 * A finalizer to make sure every operation ends with a valid fixed-point
 * decimal. Set exponent to a correct scale and check boundaries. Also check
 * for errors during operation and raise an error.
 *
 * @return NULL if finalization failed.
 *         finalized number pointer otherwise.
 */
static inline struct decimal *
decimal_finalize(struct decimal *res, uint8_t precision, uint8_t scale)
{
        uint32_t status = decimal_check_op_status();
	if (status) {
		return NULL;
	}
	res->precision = precision;
	res->scale = scale;
	decNumber exponent;
	decNumberFromInt32(&exponent, -scale);
	decNumberRescale(&res->number, &res->number, &exponent, &decimal_context);
	status = decimal_check_op_status();
	if (res->number.digits > precision || status) {
		return NULL;
	}
	return res;
}

/**
 * A common method for all the initializers. Check precision and
 * scale boundaries, and set them.
 *
 * @return NULL on error, number pointer otherwise.
 */
static inline struct decimal *
decimal_set_prec_scale(struct decimal *dec, uint8_t precision, uint8_t scale)
{
	if (precision < scale || precision > TARANTOOL_MAX_DECIMAL_DIGITS)
		return NULL;
	dec->precision = precision;
	dec->scale = scale;
	return dec;
}

/**
 * Initialize a zero-value decimal with given precision and scale.
 *
 * @return NULL if precision and scale are out of bounds.
 */
struct decimal *
decimal_zero(struct decimal *dec, uint8_t precision, uint8_t scale)
{
	if (decimal_set_prec_scale(dec, precision, scale) == NULL)
		return NULL;
	decNumberZero(&dec->number);
	return dec;
}

/**
 * Initialize a decimal with a value from the string.
 *
 * @return NULL if precision is insufficient to hold
 * the value or precision/scale are out of bounds.
 */
struct decimal *
decimal_from_string(struct decimal *dec, const char *str, uint8_t precision,
		    uint8_t scale)
{
	if(decimal_set_prec_scale(dec, precision, scale) == NULL)
		return NULL;
	decNumberFromString(&dec->number, str, &decimal_context);
	return decimal_finalize(dec, precision, scale);
}

/**
 * Initialize a decimal with an integer value.
 *
 * @return NULL if precicion is insufficient to hold
 * the value or precision/scale are out of bounds.
 */
struct decimal *
decimal_from_int(struct decimal *dec, int32_t num, uint8_t precision,
		 uint8_t scale)
{
	if (decimal_set_prec_scale(dec, precision, scale) == NULL)
		return NULL;
	decNumberFromInt32(&dec->number, num);
	return decimal_finalize(dec, precision, scale);
}

/** @copydoc decimal_from_int */
struct decimal *
decimal_from_uint(struct decimal *dec, uint32_t num, uint8_t precision, uint8_t scale)
{
	if (decimal_set_prec_scale(dec, precision, scale) == NULL)
		return NULL;
	decNumberFromUInt32(&dec->number, num);
	return decimal_finalize(dec, precision, scale);
}

/**
 * Write the decimal to a string.
 * A string has to be at least dec->precision + 3 bytes in size.
 */
char *
decimal_to_string(const struct decimal *dec, char *buf)
{
	return decNumberToString(&dec->number, buf);
}

/**
 * Cast decimal to an integer value. The number will be rounded
 * if it has a fractional part.
 */
int32_t
decimal_to_int(const struct decimal *dec)
{
	decNumber res;
	decNumberToIntegralValue(&res, &dec->number, &decimal_context);
	return decNumberToInt32(&res, &decimal_context);
}

/** @copydoc decimal_to_int */
uint32_t
decimal_to_uint(const struct decimal *dec)
{
	decNumber res;
	decNumberToIntegralValue(&res, &dec->number, &decimal_context);
	return decNumberToUInt32(&dec->number, &decimal_context);
}

/**
 * Compare 2 decimal values.
 * @return -1, lhs < rhs,
 *	    0, lhs = rhs,
 *	    1, lhs > rhs
 */
int
decimal_compare(const struct decimal *lhs, const struct decimal *rhs)
{
	decNumber res;
	decNumberCompare(&res, &lhs->number, &rhs->number, &decimal_context);
	return decNumberToInt32(&res, &decimal_context);
}

/**
 * res is set to the absolute value of dec
 * decimal_abs(&a, &a) is allowed.
 */
struct decimal *
decimal_abs(struct decimal *res, const struct decimal *dec)
{
	decNumberAbs(&res->number, &dec->number, &decimal_context);
	return res;
}

/**
 * Calculate the number of decimal digits needed to hold the
 * result of adding or subtracting lhs and rhs.
 */
static inline uint8_t addsub_precision(const struct decimal *lhs,
				       const struct decimal *rhs)
{
	uint8_t precision = MAX(lhs->scale, rhs->scale) + 1;
	precision += MAX(lhs->precision - lhs->scale,
			 rhs->precision - rhs->scale);
	return MIN(TARANTOOL_MAX_DECIMAL_DIGITS, precision);
}

/**
 * Calculate the number of digits after the decimal point for the result
 * of adding or subtracting lhs and rhs.
 */
static inline uint8_t addsub_scale(uint8_t precision, const struct decimal *lhs,
				   const struct decimal *rhs)
{
	uint8_t scale = MAX(lhs->scale, rhs->scale);
	if (precision - scale < MAX(lhs->precision - lhs->scale,
				    rhs->precision - rhs->scale)) {
		/*
		 * Not enough digits to store integral part.
		 * Try to round by decreasing scale.
		 */
		scale = precision - MAX(lhs->precision - lhs->scale,
					rhs->precision - rhs->scale);
	}
	return scale;
}

struct decimal *
decimal_add(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs)
{
	decNumberAdd(&res->number, &lhs->number, &rhs->number, &decimal_context);

	uint8_t precision = addsub_precision(lhs, rhs);
	uint8_t scale = addsub_scale(precision, lhs, rhs);
	return decimal_finalize(res,  precision, scale);
}

struct decimal *
decimal_sub(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs)
{
	decNumberSubtract(&res->number, &lhs->number, &rhs->number, &decimal_context);

	uint8_t precision = addsub_precision(lhs, rhs);
	uint8_t scale = addsub_scale(precision, lhs, rhs);
	return decimal_finalize(res,  precision, scale);
}

/** @copydoc addsub_precision */
static inline uint8_t mul_precision(const struct decimal *lhs,
				    const struct decimal *rhs)
{
	return lhs->precision + rhs->precision;
}

/** @copydoc addsub_scale */
static inline uint8_t mul_scale(uint8_t precision, const struct decimal *lhs,
				const struct decimal *rhs)
{
	uint8_t scale = lhs->scale + rhs->scale;
	if (precision - scale > 38)
		return 0;
	return MIN(scale, 38 - (precision - scale));
}

struct decimal *
decimal_mul(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs)
{
	decNumberMultiply(&res->number, &lhs->number, &rhs->number, &decimal_context);

	uint8_t precision = mul_precision(lhs, rhs);
	uint8_t scale = mul_scale(precision, lhs, rhs);
	/*
	 * Need to clamp precision, it is used unbounded in scale
	 * calculations. Scale is already clamped.
	 */
	precision = MIN(TARANTOOL_MAX_DECIMAL_DIGITS, precision);
	return decimal_finalize(res,  precision, scale);
}

/** @copydoc addsub_precision */
static inline uint8_t div_precision(const struct decimal *lhs,
				    const struct decimal *rhs)
{
	return lhs->precision + rhs->precision + 1;
}

/** @copydoc addsub_scale */
static inline uint8_t div_scale(uint8_t precision, const struct decimal *lhs,
				const struct decimal *rhs)
{
	uint8_t scale = lhs->scale + rhs->precision + 1;
	if (precision - scale > 38)
		return 0;
	return MIN(scale, 38 - (precision - scale));
}

struct decimal *
decimal_div(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs)
{
	decNumberDivide(&res->number, &lhs->number, &rhs->number, &decimal_context);

	uint8_t precision = div_precision(lhs, rhs);
	uint8_t scale = div_scale(precision, lhs, rhs);
	/*
	 * Need to clamp precision, it is used unbounded in scale
	 * calculations. Scale is already clamped.
	 */
	precision = MIN(TARANTOOL_MAX_DECIMAL_DIGITS, precision);
	return decimal_finalize(res,  precision, scale);
}

/** log10, ln, pow, exp, sqrt.
 * For these operations the scale and precision are taken from the
 * res parameter, e.g.:
 * decimal_zero(&res, 10, 5);
 * decimal_log10(&res, &some_value) -> decimal(10,5)
 */
struct decimal *
decimal_log10(struct decimal *res, const struct decimal *lhs)
{
	decNumberLog10(&res->number, &lhs->number, &decimal_context);
	return decimal_finalize(res,  res->precision, res->scale);
}

struct decimal *
decimal_ln(struct decimal *res, const struct decimal *lhs)
{
	decNumberLn(&res->number, &lhs->number, &decimal_context);
	return decimal_finalize(res,  res->precision, res->scale);
}

struct decimal *
decimal_pow(struct decimal *res, const struct decimal *lhs, const struct decimal *rhs)
{
	decNumberPower(&res->number, &lhs->number, &rhs->number, &decimal_context);
	return decimal_finalize(res,  res->precision, res->scale);
}

struct decimal *
decimal_exp(struct decimal *res, const struct decimal *lhs)
{
	decNumberExp(&res->number, &lhs->number, &decimal_context);
	return decimal_finalize(res, res->precision, res->scale);
}

struct decimal *
decimal_sqrt(struct decimal *res, const struct decimal *lhs)
{
	decNumberSquareRoot(&res->number, &lhs->number, &decimal_context);
	return decimal_finalize(res, res->precision, res->scale);
}
