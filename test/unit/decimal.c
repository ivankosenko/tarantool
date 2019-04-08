#include "unit.h"
#include "decimal.h"
#include <limits.h>
#include <string.h>

int
main(void)
{
	plan(52);

	char buf[TARANTOOL_MAX_DECIMAL_DIGITS + 3];
	char buf2[TARANTOOL_MAX_DECIMAL_DIGITS + 3];
	char *b = "2.718281828";

	struct decimal s;
	struct decimal *ret;

	ret = decimal_from_string(&s, b, 10, 9);
	isnt(ret, NULL, "Basic construction from string.");
	decimal_to_string(&s, buf);
	is(strcmp(buf, b), 0, "Correct construction and to_string conversion.");

	ret = decimal_from_int(&s, INT_MAX, 10, 0);
	decimal_to_string(&s, buf);
	sprintf(buf2, "%d", INT_MAX);
	is(strcmp(buf, buf2), 0, "Correct construction from INT_MAX.");
	is(decimal_to_int(&s), INT_MAX, "Simple conversion back to int INT_MAX");

	ret = decimal_from_int(&s, INT_MIN, 10, 0);
	decimal_to_string(&s, buf);
	sprintf(buf2, "%d", INT_MIN);
	is(strcmp(buf, buf2), 0, "Correct construction from INT_MIN.");
	is(decimal_to_int(&s), INT_MIN, "Simple conversion bact to int INT_MIN");

	char *up1 = "2.5";
	char *down1 = "2.49";
	decimal_from_string(&s, up1, 2, 1);
	is(decimal_to_int(&s), 3, ".5 Rounds up");
	decimal_from_string(&s, down1, 3, 2);
	is(decimal_to_int(&s), 2, ".49 Rounds down");

	ret = decimal_from_string(&s, b, 9, 9);
	is(ret, NULL, "Construction with insufficient precision fails.");
	ret = decimal_from_string(&s, b, 20, 8);
	isnt(ret, NULL, "Construction with insufficient scale - rounding happens.");
	ret = decimal_zero(&s, 17, 13);
	ok(s.precision == 17 && s.scale == 13 , "Construction is correct.");

	ret = decimal_zero(&s, 5, 6);
	is(ret, NULL, "Construction with scale > precision fails.");
	ret = decimal_zero(&s, TARANTOOL_MAX_DECIMAL_DIGITS + 1, TARANTOOL_MAX_DECIMAL_DIGITS);
	is(ret, NULL, "Construction with precision > TARANTOOL_MAX_DECIMAL_DIGITS fails.");

	/* 38 digits. */
	char *long_str = "0.0000000000000000000000000000000000001";
	ret = decimal_from_string(&s, long_str, 38, 37);
	isnt(ret, NULL, "Construncting the smallest possible number from string");
	decimal_to_string(&s, buf);
	is(strcmp(buf, long_str), 0, "Correct representation of smallest possible number");

	/* Comparsions. */
	char *max_str = "3.11", *min_str = "3.0999";
	struct decimal max, min;
	decimal_from_string(&max, max_str, 3, 2);
	decimal_from_string(&min, min_str, 5, 4);
	is(decimal_compare(&max, &min), 1, "max > min");
	is(decimal_compare(&min, &max), -1, "min < max");
	is(decimal_compare(&max, &max), 0, "max == max");

	ret = decimal_from_string(&s, "-3.456", 4, 3);
	isnt(ret, NULL, "Construction from negative numbers");
	decimal_to_string(&s, buf);
	is(strcmp(buf, "-3.456"), 0, "Correct construction for negatives");
	ret = decimal_abs(&s, &s);
	isnt(ret, NULL, "Abs");
	decimal_to_string(&s, buf);
	is(strcmp(buf, "3.456"), 0, "Correct abs");

	/* Arithmetic ops. */
	struct decimal d, check;
	ret = decimal_from_string(&s, b, 10, 9);
	ret = decimal_from_string(&d, "1.25", 3, 2);
	sprintf(buf2, "%.9f", 1.25 + 2.718281828);
	ret = decimal_add(&d, &d, &s);
	isnt(ret, NULL, "Simple addition");
	decimal_to_string(&d, buf);
	is(strcmp(buf, buf2), 0, "Simple addition is correct");

	ret = decimal_sub(&d, &d, &s);
	isnt(ret, NULL, "Simple subtraction");
	decimal_from_string(&check, "1.25", 3, 2);
	is(decimal_compare(&d, &check), 0, "Simple subtraction is correct");

	decimal_from_int(&s, 4, 1, 0);
	ret = decimal_mul(&s, &s, &d);
	isnt(ret, NULL, "Simple multiplication");
	decimal_from_string(&check, "5.0", 2, 1);
	is(decimal_compare(&s, &check), 0 , "Simple multiplication is correct");

	ret = decimal_div(&s, &s, &d);
	isnt(ret, NULL, "Simple division");
	decimal_from_string(&check, "4.0", 2, 1);
	is(decimal_compare(&s, &check), 0, "Simple division is correct");

	/* Math. */
	ret = decimal_from_string(&s, "40.96", 4, 2);
	ret = decimal_from_string(&check, "6.4", 2, 1);
	ret = decimal_sqrt(&s, &s);
	isnt(ret, NULL, "sqrt");
	is(decimal_compare(&s, &check), 0, "sqrt is correct");

	ret = decimal_from_string(&s, "40.96", 4, 2);
	ret = decimal_from_string(&d, "0.5", 2, 1);
	ret = decimal_pow(&s, &s, &d);
	isnt(ret, NULL, "pow");
	is(decimal_compare(&s, &check), 0, "pow is correct");

	ret = decimal_from_int(&s, 2, 1, 0);
	ret = decimal_exp(&d, &s);
	isnt(ret, NULL, "exp");
	/*
	 * precision and scale are taken from the operand to store result in.
	 * d is decimal(2, 1) (from the last test) Additionally checks that
	 * rounding works.
	 */
	ret = decimal_from_string(&check, "7.4", 2, 1);
	is(decimal_compare(&d, &check), 0, "exp is correct")

	ret = decimal_ln(&d, &d);
	isnt(ret, NULL, "ln");
	is(decimal_compare(&d, &s), 0, "ln is correct");

				      /* 10^3.5 */
	ret = decimal_from_string(&s, "3162.27766", 9, 5);
	/* d still is decimal(2, 1) */
	ret = decimal_log10(&d, &s);
	isnt(ret, NULL, "log10");
	ret = decimal_from_string(&check, "3.5", 2, 1);
	is(decimal_compare(&d, &check), 0, "log10 is correct");

	/* Advanced test. */
	/* 38 digits. */
	char *bignum = "33.333333333333333333333333333333333333";
	char *test =   "133.33333333333333333333333333333333333";
	ret = decimal_from_string(&s, bignum, 38, 36);
	ret = decimal_from_int(&d, 4, 1, 0);
	ret = decimal_mul(&s, &s, &d);
	isnt(ret, NULL, "Rounding when more than TARANTOOL_MAX_DECIMAL_DIGITS digits");
	ret = decimal_from_string(&check, test, 38, 35);
	is(decimal_compare(&s, &check), 0, "Rounding is correct");
	is(s.precision, 38, "Correct precision");
	is(s.scale, 35, "Correct scale");

	char *small = "0.00000000000000000001";
	ret = decimal_from_string(&s, small, 21, 21);
	ret = decimal_mul(&s, &s, &s);
	isnt(ret, NULL, "Rounding too small number to zero");
	ret = decimal_from_int(&check, 0, 1, 0);
	is(decimal_compare(&s, &check), 0, "Rounding is correct");
	is(s.precision, 38, "Correct precision");
	is(s.scale, 38, "Correct scale");

	decimal_from_string(&s, small, 21, 21);
	decimal_from_string(&d, "10000000000000000000", 20, 0);
	ret = decimal_div(&s, &s, &d);
	isnt(ret, NULL, "Rounding too small number to zero");
	is(decimal_compare(&s, &check), 0, "Rounding is correct");
	decimal_to_string(&s, buf);
	is(s.precision, 38, "Correct precision");
	is(s.scale, 38, "Correct scale");

	check_plan();
}
