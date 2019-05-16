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
#include "box/session.h"
#include "bind.h"
#include "ck_constraint.h"
#include "errcode.h"
#include "schema.h"
#include "session.h"
#include "sql.h"
#include "sql/sqlInt.h"
#include "sql/vdbeInt.h"
#include "tuple.h"

const char *ck_constraint_language_strs[] = {"SQL"};

/**
 * Resolve space_def references for check constraint via AST
 * tree traversal.
 * @param ck_constraint Check constraint object to update.
 * @param space_def Space definition to use.
 * @param column_mask[out] The "smart" column mask of fields are
 *                         referenced by AST.
 * @retval 0 on success.
 * @retval -1 on error.
 */
static int
ck_constraint_resolve_field_names(struct Expr *expr,
				  struct space_def *space_def,
				  uint64_t *column_mask)
{
	struct Parse parser;
	sql_parser_create(&parser, sql_get(), default_flags);
	parser.parse_only = true;
	sql_resolve_self_reference(&parser, space_def, NC_IsCheck, expr, NULL,
				   column_mask);
	int rc = parser.is_aborted ? -1 : 0;
	sql_parser_destroy(&parser);
	return rc;
}

/**
 * Create VDBE machine for ck constraint by given definition and
 * expression AST. The generated instructions consist of
 * prologue code that maps tuple fields via bindings and ck
 * constraint code which implements given expression.
 * In case of ck constraint error during VDBE execution, it is
 * aborted and error is handled as diag message.
 * @param ck_constraint_def Check constraint definition to prepare
 *                          error description.
 * @param expr Check constraint expression AST is built for
 *             given @ck_constraint_def, see for
 *             (sql_expr_compile +
 *              ck_constraint_resolve_space_def) implementation.
 * @param column_mask The "smart" column mask of fields are
 *                    referenced by @a expr.
 * @param space_def The space definition of the space this check
 *                  constraint is constructed for.
 * @retval not NULL sql_stmt program pointer on success.
 * @retval NULL otherwise.
 */
static struct sql_stmt *
ck_constraint_program_compile(struct ck_constraint_def *ck_constraint_def,
			      struct Expr *expr, uint64_t column_mask,
			      struct space_def *space_def)
{
	struct sql *db = sql_get();
	struct Parse parser;
	sql_parser_create(&parser, db, default_flags);
	struct Vdbe *v = sqlGetVdbe(&parser);
	if (v == NULL) {
		diag_set(OutOfMemory, sizeof(struct Vdbe), "sqlGetVdbe",
			 "vdbe");
		return NULL;
	}
	/*
	 * Generate a prologue code that introduces variables to
	 * bind tuple fields there before execution.
	 */
	uint32_t field_count = space_def->field_count;
	/*
	 * Use column mask to prepare binding only for
	 * referenced fields.
	 */
	int bind_tuple_reg = sqlGetTempRange(&parser, field_count);
	struct bit_iterator it;
	bit_iterator_init(&it, &column_mask, sizeof(uint64_t), true);
	size_t used_fieldno = bit_iterator_next(&it);
	for (; used_fieldno < SIZE_MAX; used_fieldno = bit_iterator_next(&it)) {
		sqlVdbeAddOp2(v, OP_Variable, ++parser.nVar,
			      bind_tuple_reg + used_fieldno);
	}
	if (column_mask_fieldno_is_set(column_mask, 63)) {
		used_fieldno = 64;
		for (; used_fieldno < (size_t)field_count; used_fieldno++) {
			sqlVdbeAddOp2(v, OP_Variable, ++parser.nVar,
				      bind_tuple_reg + used_fieldno);
		}
	}
	/* Generate ck constraint test code. */
	vdbe_emit_ck_constraint(&parser, expr, ck_constraint_def->name,
				ck_constraint_def->expr_str, bind_tuple_reg);

	/* Clean-up and restore user-defined sql context. */
	bool is_error = parser.is_aborted;
	sql_finish_coding(&parser);
	sql_parser_destroy(&parser);

	if (is_error) {
		diag_set(ClientError, ER_CREATE_CK_CONSTRAINT,
			 ck_constraint_def->name,
			 box_error_message(box_error_last()));
		sql_finalize((struct sql_stmt *) v);
		return NULL;
	}
	return (struct sql_stmt *) v;
}

/**
 * Run bytecode implementing check constraint on new tuple
 * before insert or replace in space space_def.
 * @param ck_constraint Check constraint to run.
 * @param space_def The space definition of the space this check
 *                  constraint is constructed for.
 * @param new_tuple The tuple to be inserted in space.
 * @retval 0 if check constraint test is passed, -1 otherwise.
 */
static int
ck_constraint_program_run(struct ck_constraint *ck_constraint,
			  const char *new_tuple)
{
	/*
	 * Prepare parameters for checks->stmt execution:
	 * Map new tuple fields to Vdbe memory variables in range:
	 * [1, field_count]
	 */
	struct space *space = space_by_id(ck_constraint->space_id);
	assert(space != NULL);
	/* Use column mask to bind only referenced fields. */
	struct bit_iterator it;
	bit_iterator_init(&it, &ck_constraint->column_mask,
			  sizeof(uint64_t), true);
	size_t used_fieldno = bit_iterator_next(&it);
	if (used_fieldno == SIZE_MAX)
		return 0;
	/*
	 * When last format fields are nullable, they are
	 * 'optional' i.e. they may not be present in the tuple.
	 */
	uint32_t tuple_field_count = mp_decode_array(&new_tuple);
	uint32_t field_count =
		MIN(tuple_field_count, space->def->field_count);
	uint32_t bind_pos = 1;
	for (uint32_t fieldno = 0; fieldno < field_count; fieldno++) {
		if (used_fieldno == SIZE_MAX &&
		    !column_mask_fieldno_is_set(ck_constraint->column_mask,
						63)) {
			/* No more required fields are left. */
			break;
		}
		if (used_fieldno != SIZE_MAX &&
		    (size_t)fieldno < used_fieldno) {
			/*
			 * Skip unused fields is mentioned in
			 * column mask.
			 */
			mp_next(&new_tuple);
			continue;
		}
		struct sql_bind bind;
		if (sql_bind_decode(&bind, bind_pos, &new_tuple) != 0 ||
		    sql_bind_column(ck_constraint->stmt, &bind, bind_pos) != 0) {
			diag_set(ClientError, ER_CK_CONSTRAINT_FAILED,
				 ck_constraint->def->name,
				 ck_constraint->def->expr_str);
			return -1;
		}
		bind_pos++;
		used_fieldno = bit_iterator_next(&it);
	}
	if (used_fieldno != SIZE_MAX && field_count < space->def->field_count) {
		struct sql_bind bind = {.bytes = 1, .type = MP_NIL};
		for (; used_fieldno != SIZE_MAX;
		     used_fieldno = bit_iterator_next(&it)) {
			if (sql_bind_column(ck_constraint->stmt, &bind,
					    bind_pos) != 0) {
				diag_set(ClientError, ER_CK_CONSTRAINT_FAILED,
					 ck_constraint->def->name,
					 ck_constraint->def->expr_str);
				return -1;
			}
			bind_pos++;
		}
	}
	/* Checks VDBE can't expire, reset expired flag and go. */
	struct Vdbe *v = (struct Vdbe *) ck_constraint->stmt;
	v->expired = 0;
	while (sql_step(ck_constraint->stmt) == SQL_ROW) {}
	/*
	 * Get VDBE execution state and reset VM to run it
	 * next time.
	 */
	return sql_reset(ck_constraint->stmt) != SQL_OK ? -1 : 0;
}

/**
 * Ck constraint trigger function. It ss expected to be executed
 * in space::on_replace trigger.
 *
 * It extracts all ck constraint required context from event and
 * run bytecode implementing check constraint to test a new tuple
 * before it will be inserted in destination space.
 */
static void
ck_constraint_on_replace_trigger(struct trigger *trigger, void *event)
{
	struct ck_constraint *ck_constraint =
		(struct ck_constraint *) trigger->data;
	struct txn *txn = (struct txn *) event;
	struct txn_stmt *stmt = txn_current_stmt(txn);
	assert(stmt != NULL);
	struct tuple *new_tuple = stmt->new_tuple;
	if (new_tuple == NULL)
		return;
	if (ck_constraint_program_run(ck_constraint,
				      tuple_data(new_tuple)) != 0)
		diag_raise();
}

struct ck_constraint *
ck_constraint_new(struct ck_constraint_def *ck_constraint_def,
		  struct space_def *space_def)
{
	if (space_def->field_count == 0) {
		diag_set(ClientError, ER_UNSUPPORTED, "Tarantool",
			 "CK constraint for space without format");
		return NULL;
	}
	struct ck_constraint *ck_constraint = malloc(sizeof(*ck_constraint));
	if (ck_constraint == NULL) {
		diag_set(OutOfMemory, sizeof(*ck_constraint), "malloc",
			 "ck_constraint");
		return NULL;
	}
	ck_constraint->def = NULL;
	ck_constraint->stmt = NULL;
	ck_constraint->space_id = space_def->id;
	rlist_create(&ck_constraint->link);
	trigger_create(&ck_constraint->trigger,
		       ck_constraint_on_replace_trigger, ck_constraint,
		       NULL);
	struct Expr *expr =
		sql_expr_compile(sql_get(), ck_constraint_def->expr_str,
				 strlen(ck_constraint_def->expr_str));
	if (expr == NULL ||
	    ck_constraint_resolve_field_names(expr, space_def,
					&ck_constraint->column_mask) != 0) {
		diag_set(ClientError, ER_CREATE_CK_CONSTRAINT,
			 ck_constraint_def->name,
			 box_error_message(box_error_last()));
		goto error;
	}
	ck_constraint->stmt =
		ck_constraint_program_compile(ck_constraint_def, expr,
					      ck_constraint->column_mask,
					      space_def);
	if (ck_constraint->stmt == NULL)
		goto error;

	sql_expr_delete(sql_get(), expr, false);
	ck_constraint->def = ck_constraint_def;
	return ck_constraint;
error:
	sql_expr_delete(sql_get(), expr, false);
	ck_constraint_delete(ck_constraint);
	return NULL;
}

void
ck_constraint_delete(struct ck_constraint *ck_constraint)
{
	assert(rlist_empty(&ck_constraint->trigger.link));
	sql_finalize(ck_constraint->stmt);
	free(ck_constraint->def);
	TRASH(ck_constraint);
	free(ck_constraint);
}

struct ck_constraint *
space_ck_constraint_by_name(struct space *space, const char *name,
			    uint32_t name_len)
{
	struct ck_constraint *ck_constraint = NULL;
	rlist_foreach_entry(ck_constraint, &space->ck_constraint, link) {
		if (strlen(ck_constraint->def->name) == name_len &&
		    memcmp(ck_constraint->def->name, name, name_len) == 0)
			return ck_constraint;
	}
	return NULL;
}
