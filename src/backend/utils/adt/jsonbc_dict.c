/*
 * dict.c
 *
 *  Created on: 18 мая 2015 г.
 *      Author: smagen
 */

#include "postgres.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/json_generic.h"
#include "utils/jsonbc_dict.h"
#include "utils/memutils.h"
#ifdef JSONBC_DICT_SYSCACHE
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_jsonbc_dict.h"
#include "catalog/pg_enum.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#else
#include "access/hash.h"
#include "utils/hsearch.h"
#endif
#ifdef JSONBC_DICT_SEQUENCES
#include "access/xact.h"
#include "catalog/dependency.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#endif

#define JSONBC_DICT_TAB "pg_jsonbc_dict"

#ifdef JSONBC_DICT_SYSCACHE
JsonbcKeyId
jsonbcDictGetIdByNameSeqCached(JsonbcDictId dict, JsonbcKeyName name)
{
	text	   *txt = cstring_to_text_with_len(name.s, name.len);
	HeapTuple	tuple = SearchSysCache2(JSONBCDICTNAME,
										JsonbcDictIdGetDatum(dict),
										PointerGetDatum(txt));
	JsonbcKeyId	id;

	pfree(txt);

	if (!HeapTupleIsValid(tuple))
		return JsonbcInvalidKeyId;

	id = ((Form_pg_jsonbc_dict) GETSTRUCT(tuple))->id;
	ReleaseSysCache(tuple);

	return id;
}

static JsonbcKeyId
jsonbcDictGetIdByNameSeq(JsonbcDictId dict, JsonbcKeyName name, bool insert)
{
	JsonbcKeyId	id = jsonbcDictGetIdByNameSeqCached(dict, name);

	if (id == JsonbcInvalidKeyId && insert)
	{
#ifndef JSONBC_DICT_UPSERT
		Datum		values[Natts_pg_jsonbc_dict];
		bool		nulls[Natts_pg_jsonbc_dict];
		Relation	rel;

		id = (JsonbcKeyId) nextval_internal(dict);

		values[Anum_pg_jsonbc_dict_dict - 1] = JsonbcDictIdGetDatum(dict);
		values[Anum_pg_jsonbc_dict_id - 1] = JsonbcKeyIdGetDatum(id);
		values[Anum_pg_jsonbc_dict_name - 1] = PointerGetDatum(txt);
		MemSet(nulls, false, sizeof(nulls));

		rel = relation_open(JsonbcDictionaryRelationId, RowExclusiveLock);

		tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

		simple_heap_insert(rel, tuple);

		CatalogUpdateIndexes(rel, tuple);

		relation_close(rel, RowExclusiveLock);

		heap_freetuple(tuple);
#else
		JsonbcKeyId nextKeyId = (JsonbcKeyId) nextval_internal(dict);

# ifndef JSONBC_DICT_NO_WORKER
		id = jsonbcDictWorkerGetIdByName(dict, name, nextKeyId);
# else
		id = jsonbcDictGetIdByNameSlow(dict, name, nextKeyId);
# endif
#endif
	}

	return id;
}

static JsonbcKeyId
jsonbcDictGetIdByNameEnum(JsonbcDictId dict, JsonbcKeyName name)
{
	Oid			enumOid = JsonbcDictIdGetEnumOid(dict);
	NameData	nameData;
	HeapTuple	tuple;
	JsonbcKeyId	id;

	if (name.len >= NAMEDATALEN)
		return JsonbcInvalidKeyId;

	memcpy(NameStr(nameData), name.s, name.len);
	NameStr(nameData)[name.len] = '\0';

	tuple = SearchSysCache2(ENUMTYPOIDNAME,
							ObjectIdGetDatum(enumOid),
							NameGetDatum(&nameData));

	if (!HeapTupleIsValid(tuple))
		return JsonbcInvalidKeyId;

	id = HeapTupleGetOid(tuple);

	ReleaseSysCache(tuple);

	return id;
}

JsonbcKeyId
jsonbcDictGetIdByName(JsonbcDictId dict, JsonbcKeyName name, bool insert)
{
	return JsonbcDictIdIsEnum(dict)
				? jsonbcDictGetIdByNameEnum(dict, name)
				: jsonbcDictGetIdByNameSeq(dict, name, insert);
}

static JsonbcKeyName
jsonbcDictGetNameByIdSeq(JsonbcDictId dict, JsonbcKeyId id)
{
	JsonbcKeyName	name;
	HeapTuple		tuple = SearchSysCache2(JSONBCDICTID,
											JsonbcDictIdGetDatum(dict),
											JsonbcKeyIdGetDatum(id));
	if (HeapTupleIsValid(tuple))
	{
		text   *text = &((Form_pg_jsonbc_dict) GETSTRUCT(tuple))->name;

		name.len = VARSIZE_ANY_EXHDR(text);
		name.s = memcpy(palloc(name.len), VARDATA_ANY(text), name.len);

		ReleaseSysCache(tuple);
	}
	else
	{
		name.s = NULL;
		name.len = 0;
	}

	return name;
}

static JsonbcKeyName
jsonbcDictGetNameByIdEnum(JsonbcDictId dict, JsonbcKeyId id)
{
	JsonbcKeyName	name;
	HeapTuple		tuple = SearchSysCache1(ENUMOID,
											ObjectIdGetDatum((Oid) id));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_enum	enumTuple = (Form_pg_enum) GETSTRUCT(tuple);
		Name			label = &enumTuple->enumlabel;

		Assert(JsonbcDictIdGetEnumOid(dict) == enumTuple->enumtypid);

		name.len = strlen(NameStr(*label));
		name.s = memcpy(palloc(name.len), NameStr(*label), name.len);

		ReleaseSysCache(tuple);
	}
	else
	{
		name.s = NULL;
		name.len = 0;
	}

	return name;
}

JsonbcKeyName
jsonbcDictGetNameById(JsonbcDictId dict, JsonbcKeyId id)
{
	return JsonbcDictIdIsEnum(dict) ? jsonbcDictGetNameByIdEnum(dict, id)
									: jsonbcDictGetNameByIdSeq(dict, id);
}

#else
static bool initialized = false;
static HTAB *idToNameHash, *nameToIdHash;
static SPIPlanPtr savedPlanSelect = NULL;
#ifndef JSONBC_DICT_SEQUENCES
static SPIPlanPtr savedPlanNewDict = NULL;
#endif

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
#endif

#if !defined(JSONBC_DICT_SYSCACHE) || defined(JSONBC_DICT_UPSERT)
static SPIPlanPtr savedPlanInsert = NULL;

JsonbcKeyId
jsonbcDictGetIdByNameSlow(JsonbcDictId dict, JsonbcKeyName name,
						  JsonbcKeyId nextKeyId)
{
#ifdef JSONBC_DICT_SEQUENCES
	Oid		argTypes[3] = {JsonbcDictIdTypeOid, TEXTOID, JsonbcKeyIdTypeOid};
	Datum	args[3];
#else
	Oid		argTypes[2] = {JsonbcDictIdTypeOid, TEXTOID};
	Datum	args[2];
#endif
	JsonbcKeyId	id;
	bool	null;

	SPI_connect();

	if (!savedPlanInsert)
	{
		savedPlanInsert = SPI_prepare(
#ifdef JSONBC_DICT_UPSERT
# ifdef JSONBC_DICT_SEQUENCES
			"INSERT INTO "JSONBC_DICT_TAB" (dict, name, id) VALUES ($1, $2, $3)"
# else
			"INSERT INTO "JSONBC_DICT_TAB" (dict, name) VALUES ($1, $2)"
# endif
			" ON CONFLICT (dict, name) DO UPDATE SET id = "JSONBC_DICT_TAB".id"
			" RETURNING id;",
#else
			"WITH select_data AS ( "
			"	SELECT id FROM "JSONBC_DICT_TAB" WHERE dict = $1 AND name = $2"
			"), "
			"insert_data AS ( "
#ifdef JSONBC_DICT_SEQUENCES
			"	INSERT INTO "JSONBC_DICT_TAB" (dict, name, id) "
			"		(SELECT $1, $2, $3 WHERE NOT EXISTS "
			"			(SELECT id FROM select_data)) RETURNING id "
#else
			"	INSERT INTO "JSONBC_DICT_TAB" (dict, name) "
			"		(SELECT $1, $2 WHERE NOT EXISTS "
			"			(SELECT id FROM select_data)) RETURNING id "
#endif
			") "
			"SELECT id FROM select_data "
			"	UNION ALL "
			"SELECT id FROM insert_data;",
#endif
			lengthof(argTypes), argTypes);
		if (!savedPlanInsert)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanInsert))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = JsonbcDictIdGetDatum(dict);
	args[1] = PointerGetDatum(cstring_to_text_with_len(name.s, name.len));
#ifdef JSONBC_DICT_SEQUENCES
	args[2] = JsonbcKeyIdGetDatum(nextKeyId);
#endif

	if (SPI_execute_plan(savedPlanInsert, args, NULL, false, 1) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "Failed to insert into dictionary");

	id = DatumGetJsonbcKeyId(SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc, 1, &null));
#ifndef JSONBC_DICT_SYSCACHE
	jsonbcDictAddEntry(dict, id, name);
#endif

	SPI_finish();
	return id;
}
#endif

#ifndef JSONBC_DICT_SYSCACHE
JsonbcKeyId
jsonbcDictGetIdByName(JsonbcDictId dict, JsonbcKeyName name, bool insert)
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

	return found ? nameToId->id : jsonbcDictGetIdByNameSlow(dict, name, 0);
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
			"SELECT name FROM "JSONBC_DICT_TAB" WHERE dict = $1 AND id = $2;",
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
#endif

void
jsonbcDictAddRef(Form_pg_attribute attr, JsonbcDictId dict)
{
#ifdef JSONBC_DICT_SEQUENCES
	ObjectAddress	dep;
	ObjectAddress	ref;

	if (JsonbcDictIdIsEnum(dict))
	{
		ObjectAddressSubSet(dep, RelationRelationId, attr->attrelid, attr->attnum);
		ObjectAddressSet(ref, TypeRelationId, JsonbcDictIdGetEnumOid(dict));
		recordDependencyOn(&dep, &ref, DEPENDENCY_NORMAL);
	}
	else
	{
		ObjectAddressSet(dep, RelationRelationId, dict);
		ObjectAddressSubSet(ref, RelationRelationId, attr->attrelid, attr->attnum);
		recordDependencyOn(&dep, &ref, DEPENDENCY_INTERNAL);
	}
#endif
}

static SPIPlanPtr savedPlanClear = NULL;

static void
jsonbcClearDictionary(JsonbcDictId dict)
{
	Oid		argTypes[1] = {JsonbcDictIdTypeOid};
	Datum	args[1];

	SPI_connect();

	if (!savedPlanClear)
	{
		savedPlanClear = SPI_prepare(
				"DELETE FROM "JSONBC_DICT_TAB" WHERE dict = $1;",
				lengthof(argTypes), argTypes);
		if (!savedPlanClear)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanClear))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = JsonbcDictIdGetDatum(dict);

	if (SPI_execute_plan(savedPlanClear, args, NULL, false, 0) < 0)
		elog(ERROR, "Failed to clear dictionary");

	SPI_finish();
}

void
jsonbcDictRemoveRef(Form_pg_attribute att, JsonbcDictId dict)
{
#ifdef JSONBC_DICT_SEQUENCES
	long	totalCount;
	int		cnt;

	if (JsonbcDictIdIsEnum(dict))
	{
		changeDependencyFor(RelationRelationId, att->attrelid, att->attnum,
							TypeRelationId, JsonbcDictIdGetEnumOid(dict),
							InvalidOid, &totalCount);
		return;
	}

	cnt = changeDependencyFor(RelationRelationId, dict,
							  RelationRelationId, att->attrelid, att->attnum,
							  InvalidOid, &totalCount);

	Assert(cnt <= 1);

	if (cnt == totalCount)
	{
		if (cnt > 0)
		{
			ObjectAddress seqaddr;
			ObjectAddressSet(seqaddr, RelationRelationId, dict);
			CommandCounterIncrement(); /* FIXME */
			performDeletion(&seqaddr, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
		}

		jsonbcClearDictionary(dict);
	}
#endif
}

JsonbcDictId
jsonbcDictCreate(Form_pg_attribute attr)
{
#ifdef JSONBC_DICT_SEQUENCES
	CreateSeqStmt	   *stmt = makeNode(CreateSeqStmt);
	Relation			rel;
#if 0
	List			   *attnamelist;
#endif
	char			   *name;
	char			   *namespace;
	Oid					namespaceid;
	ObjectAddress		addr;

	rel = relation_open(attr->attrelid, NoLock);

	namespaceid = RelationGetNamespace(rel);
	namespace = get_namespace_name(namespaceid);
	name = ChooseRelationName(RelationGetRelationName(rel),
							  NameStr(attr->attname),
							  "jsonbc_dict_seq",
							  namespaceid);

	stmt->sequence = makeRangeVar(namespace, name, -1);
	stmt->ownerId = rel->rd_rel->relowner;
#if 0
	attnamelist = list_make3(makeString(namespace),
							 makeString(RelationGetRelationName(rel)),
							 makeString(NameStr(attr->attname)));
	stmt->options = list_make1(makeDefElem("owned_by", (Node *) attnamelist));
#else
	stmt->options = NIL;
#endif
	stmt->if_not_exists = false;

	addr = DefineSequence(stmt);

	relation_close(rel, NoLock);

	return addr.objectId;
#else
	JsonbcDictId	id;
	bool			null;

	SPI_connect();

	if (!savedPlanNewDict)
	{
		savedPlanNewDict = SPI_prepare(
			"INSERT INTO "JSONBC_DICT_TAB" (id) VALUES (0) RETURNING dict",
			0, NULL);
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
#endif
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
	id = jsonbcDictGetIdByName(dict, name, false);

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
