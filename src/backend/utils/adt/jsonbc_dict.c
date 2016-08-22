/*
 * dict.c
 *
 *  Created on: 18 мая 2015 г.
 *      Author: smagen
 */

#include "postgres.h"
#include "access/hash.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "jsonbc_dict.h"

static bool initialized = false;
static HTAB *idToNameHash, *nameToIdHash;
static SPIPlanPtr savedPlanInsert = NULL;
static SPIPlanPtr savedPlanSelect = NULL;
static SPIPlanPtr savedPlanNewDict = NULL;

typedef struct IdToNameKey
{
	JsonbcDictId	dict;
	JsonbcKeyId		id;
} IdToNameKey;

typedef struct NameToIdKey
{
	JsonbcDictId	dict;
	JsonbcKeyName	name;
} NameToIdKey;

typedef struct
{
	IdToNameKey		key;
	JsonbcKeyName	name;
} IdToName;

typedef struct
{
	NameToIdKey		key;
	JsonbcKeyId		id;
} NameToId;

static uint32
jsonbcDictNameHash(const void *key, Size keysize)
{
	const NameToIdKey *k = (const NameToIdKey *) key;

	return DatumGetUInt32(hash_uint32((uint32) k->dict) ^
						  hash_any((unsigned char *) k->name.s, k->name.len));
}

static int
jsonbcDictNameMatch(const void *key1, const void *key2, Size keysize)
{
	const NameToIdKey *k1 = (const NameToIdKey *) key1;
	const NameToIdKey *k2 = (const NameToIdKey *) key2;

	if (k1->dict != k2->dict)
		return k1->dict > k2->dict ? 1 : -1;

	if (k1->name.len == k2->name.len)
		return memcmp(k1->name.s, k2->name.s, k1->name.len);

	return (k1->name.len > k2->name.len) ? 1 : -1;
}

static void
jsonbcDictCheckInit()
{
	HASHCTL ctl;

	if (initialized)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.hash = tag_hash;
	ctl.hcxt = TopMemoryContext;
	ctl.keysize = sizeof(IdToNameKey);
	ctl.entrysize = sizeof(IdToName);
	idToNameHash = hash_create("Id to name map", 1024, &ctl,
								HASH_FUNCTION | HASH_CONTEXT | HASH_ELEM);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(NameToIdKey);
	ctl.entrysize = sizeof(NameToId);
	ctl.hash = jsonbcDictNameHash;
	ctl.match = jsonbcDictNameMatch;
	ctl.hcxt = TopMemoryContext;
	nameToIdHash = hash_create("Name to id map", 1024, &ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	initialized = true;
}

static IdToName *
jsonbcDictAddEntry(JsonbcDictId dict, JsonbcKeyId id, JsonbcKeyName name)
{
	NameToIdKey nameToIdKey;
	IdToNameKey idToNameKey;
	NameToId   *nameToId;
	IdToName   *idToName;
	bool		found;

	nameToIdKey.dict = dict;
	nameToIdKey.name.len = name.len;
	nameToIdKey.name.s = MemoryContextAlloc(TopMemoryContext, name.len);
	memcpy((char *) nameToIdKey.name.s, name.s, name.len);

	nameToId = (NameToId *) hash_search(nameToIdHash,
										(const void *) &nameToIdKey,
										HASH_ENTER, &found);
	nameToId->id = id;

	idToNameKey.dict = dict;
	idToNameKey.id = id;

	idToName = (IdToName *) hash_search(idToNameHash,
										(const void *) &idToNameKey,
										HASH_ENTER, &found);
	idToName->name = nameToIdKey.name;

	return idToName;
}

static JsonbcKeyId
jsonbcDictGetIdByNameSlow(JsonbcDictId dict, JsonbcKeyName name)
{
	Oid		argTypes[2] = {JsonbcDictIdTypeOid, TEXTOID};
	Datum	args[2];
	JsonbcKeyId	id;
	bool	null;

	SPI_connect();

	if (!savedPlanInsert)
	{
		savedPlanInsert = SPI_prepare(
			"WITH select_data AS ( "
			"	SELECT id FROM jsonbc_dict WHERE dict = $1 AND name = $2 "
			"), "
			"insert_data AS ( "
			"	INSERT INTO jsonbc_dict (dict, name) "
			"		(SELECT $1, $2 WHERE NOT EXISTS "
			"			(SELECT id FROM select_data)) RETURNING id "
			") "
			"SELECT id FROM select_data "
			"	UNION ALL "
			"SELECT id FROM insert_data;", 2, argTypes);
		if (!savedPlanInsert)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanInsert))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = JsonbcDictIdGetDatum(dict);
	args[1] = PointerGetDatum(cstring_to_text_with_len(name.s, name.len));

	if (SPI_execute_plan(savedPlanInsert, args, NULL, false, 1) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "Failed to insert into dictionary");

	id = DatumGetJsonbcKeyId(SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc, 1, &null));
	jsonbcDictAddEntry(dict, id, name);

	SPI_finish();
	return id;
}

JsonbcKeyId
jsonbcDictGetIdByName(JsonbcDictId dict, JsonbcKeyName name)
{
	NameToIdKey	nameToIdKey;
	NameToId   *nameToId;
	bool		found;

	jsonbcDictCheckInit();

	nameToIdKey.dict = dict;
	nameToIdKey.name = name;

	nameToId = (NameToId *) hash_search(nameToIdHash,
										(const void *) &nameToIdKey,
										HASH_FIND, &found);

	return found ? nameToId->id : jsonbcDictGetIdByNameSlow(dict, name);
}

static JsonbcKeyName
jsonbcDictGetNameByIdSlow(JsonbcDictId dict, JsonbcKeyId id)
{
	Oid			argTypes[2] = {JsonbcDictIdTypeOid, JsonbcKeyIdTypeOid};
	Datum		args[2];
	JsonbcKeyName name;
	text 	   *nameText;
	IdToName   *result;
	bool		null;

	SPI_connect();

	if (!savedPlanSelect)
	{
		savedPlanSelect = SPI_prepare(
			"SELECT name FROM jsonbc_dict WHERE dict = $1 AND id = $2;",
			2, argTypes);
		if (!savedPlanSelect)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanSelect))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = JsonbcDictIdGetDatum(dict);
	args[1] = JsonbcKeyIdGetDatum(id);

	if (SPI_execute_plan(savedPlanSelect, args, NULL, false, 1) < 0)
		elog(ERROR, "Failed to select from dictionary");

	if (SPI_processed < 1)
	{
		SPI_finish();

		name.s = NULL;
		name.len = 0;
		return name;
	}

	nameText = DatumGetTextPP(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &null));
	name.s = VARDATA_ANY(nameText);
	name.len = VARSIZE_ANY_EXHDR(nameText);
	result = jsonbcDictAddEntry(dict, id, name);

	SPI_finish();

	return result->name;
}

JsonbcKeyName
jsonbcDictGetNameById(JsonbcDictId dict, JsonbcKeyId id)
{
	IdToNameKey	key;
	IdToName   *result;
	bool		found;

	jsonbcDictCheckInit();

	key.dict = dict;
	key.id = id;

	result = (IdToName *) hash_search(idToNameHash,
									  (const void *) &key,
									  HASH_FIND, &found);

	return found ? result->name : jsonbcDictGetNameByIdSlow(dict, id);
}

void
jsonbcDictAddRef(Form_pg_attribute attr, JsonbcDictId dict)
{
}

void
jsonbcDictRemoveRef(Form_pg_attribute att, JsonbcDictId dict)
{
}

JsonbcDictId
jsonbcDictCreate(Form_pg_attribute att)
{
	JsonbcDictId	id;
	bool			null;

	SPI_connect();

	if (!savedPlanNewDict)
	{
		savedPlanNewDict = SPI_prepare(
			"INSERT INTO jsonbc_dict(id) VALUES (0) RETURNING dict", 0, NULL);
		if (!savedPlanNewDict)
			elog(ERROR, "jsonbc: error preparing query");
		if (SPI_keepplan(savedPlanNewDict))
			elog(ERROR, "jsonbc: error keeping plan");
	}

	if (SPI_execute_plan(savedPlanNewDict, NULL, NULL, false, 1) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "jsonbc: failed to create new dictionary");

	id = DatumGetJsonbcDictId(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &null));
	SPI_finish();

	return id;
}

PG_FUNCTION_INFO_V1(jsonbc_get_id_by_name);
PG_FUNCTION_INFO_V1(jsonbc_get_name_by_id);

Datum
jsonbc_get_id_by_name(PG_FUNCTION_ARGS)
{
	JsonbcDictId	dict = DatumGetJsonbcDictId(PG_GETARG_DATUM(0));
	text		   *nameText = PG_GETARG_TEXT_PP(1);
	JsonbcKeyId		id;
	JsonbcKeyName	name;

	name.s = VARDATA_ANY(nameText);
	name.len = VARSIZE_ANY_EXHDR(nameText);
	id = jsonbcDictGetIdByName(dict, name);

	PG_RETURN_DATUM(JsonbcKeyIdGetDatum(id));
}

Datum
jsonbc_get_name_by_id(PG_FUNCTION_ARGS)
{
	JsonbcDictId	dict = DatumGetJsonbcDictId(PG_GETARG_DATUM(0));
	JsonbcKeyId		id = PG_GETARG_INT32(1);
	JsonbcKeyName	name;

	name = jsonbcDictGetNameById(dict, id);
	if (name.s)
		PG_RETURN_TEXT_P(cstring_to_text_with_len(name.s, name.len));
	else
		PG_RETURN_NULL();
}
