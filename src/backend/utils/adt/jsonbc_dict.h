#ifndef JSONBC_DICT_H
#define JSONBC_DICT_H

#include "c.h"
#include "catalog/pg_attribute.h"

#define JSONBC_DICT_SEQUENCES
#define JSONBC_DICT_SYSCACHE

typedef int32 JsonbcKeyId;
#define JsonbcKeyIdTypeOid	INT4OID
#define JsonbcKeyIdGetDatum(datum)	Int32GetDatum(datum)

#ifdef JSONBC_DICT_SEQUENCES
typedef Oid JsonbcDictId;
#define JsonbcDictIdTypeOid	OIDOID
#define JsonbcDictIdGetDatum(datum)	ObjectIdGetDatum(datum)
#define DatumGetJsonbcDictId(datum)	DatumGetObjectId(datum)
#else
typedef int32 JsonbcDictId;
#define JsonbcDictIdTypeOid	INT4OID
#define JsonbcDictIdGetDatum(datum)	Int32GetDatum(datum)
#define DatumGetJsonbcDictId(datum)	DatumGetInt32(datum)
#endif

typedef struct
{
	const char *s;
	int			len;
} JsonbcKeyName;

extern JsonbcDictId		jsonbcDictCreate(Form_pg_attribute attr);
extern void				jsonbcDictAddRef(Form_pg_attribute attr,
										JsonbcDictId dict);
extern void				jsonbcDictRemoveRef(Form_pg_attribute attr,
											JsonbcDictId dict);
extern JsonbcKeyId		jsonbcDictGetIdByName(JsonbcDictId dict,
											  JsonbcKeyName name);
extern JsonbcKeyName	jsonbcDictGetNameById(JsonbcDictId dict, JsonbcKeyId id);

#endif /* JSONBC_DICT_H */
