/*
 * json_op.c
 *
 * Portions Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/json_op.c
 *
 */

#define jsonb_exists		json_exists
#define jsonb_exists_any	json_exists_any
#define jsonb_exists_all	json_exists_all
#define jsonb_contains		json_contains
#define jsonb_contained		json_contained
#define jsonb_ne			json_ne
#define jsonb_lt			json_lt
#define jsonb_gt			json_gt
#define jsonb_le			json_le
#define jsonb_ge			json_ge
#define jsonb_eq			json_eq
#define jsonb_cmp			json_cmp
#define jsonb_hash			json_hash

#include "utils/json_generic.h"

#undef PG_GETARG_JSONB
#define PG_GETARG_JSONB(x)	DatumGetJsont(PG_GETARG_DATUM(x))

#include "jsonb_op.c"
