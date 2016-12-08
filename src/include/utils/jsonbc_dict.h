#ifndef JSONBC_DICT_H
#define JSONBC_DICT_H

#include "c.h"
#include "catalog/pg_attribute.h"

#define JSONBC_DICT_SEQUENCES
#define JSONBC_DICT_SYSCACHE
#define JSONBC_DICT_UPSERT

typedef int32 JsonbcKeyId;
#define JsonbcKeyIdTypeOid	INT4OID
#define JsonbcKeyIdGetDatum(datum)	Int32GetDatum(datum)
#define JsonbcInvalidKeyId	(-1)

#ifdef JSONBC_DICT_SEQUENCES
typedef Oid JsonbcDictId;
#define JsonbcDictIdEnumFlag			0x80000000
#define JsonbcDictIdIsEnum(dictId)		(((dictId) &  JsonbcDictIdEnumFlag) != 0)
#define JsonbcDictIdGetEnumOid(dictId)	 ((dictId) & ~JsonbcDictIdEnumFlag)
#define JsonbcDictIdTypeOid	OIDOID
#define JsonbcDictIdGetDatum(datum)	ObjectIdGetDatum(datum)
#define DatumGetJsonbcDictId(datum)	DatumGetObjectId(datum)
#else
typedef int32 JsonbcDictId;
#define JsonbcDictIdTypeOid	INT4OID
#define JsonbcDictIdGetDatum(datum)	Int32GetDatum(datum)
#define DatumGetJsonbcDictId(datum)	DatumGetInt32(datum)
#endif

#define DatumGetJsonbcKeyId(datum)	DatumGetInt32(datum)

extern int jsonbc_max_workers; /* GUC parameter */

typedef struct
{
	const char *s;
	int			len;
} JsonbcKeyName;

extern JsonbcDictId		jsonbcDictCreate(Form_pg_attribute attr);
extern void			jsonbcDictAddRef(Form_pg_attribute attr,
										 JsonbcDictId dict);
extern void			jsonbcDictRemoveRef(Form_pg_attribute attr,
											JsonbcDictId dict);
extern JsonbcKeyId		jsonbcDictGetIdByName(JsonbcDictId dict,
											  JsonbcKeyName name, bool insert);
extern JsonbcKeyId		jsonbcDictGetIdByNameSlow(JsonbcDictId dict,
												  JsonbcKeyName name,
												  JsonbcKeyId nextKeyId);
extern JsonbcKeyId		jsonbcDictGetIdByNameSeqCached(JsonbcDictId dict,
													   JsonbcKeyName name);
extern	JsonbcKeyId		jsonbcDictWorkerGetIdByName(JsonbcDictId dict,
													JsonbcKeyName key,
													JsonbcKeyId nextKeyId);
extern JsonbcKeyName	jsonbcDictGetNameById(JsonbcDictId dict, JsonbcKeyId id);

extern	void			JsonbcDictWorkerShmemInit();

#endif /* JSONBC_DICT_H */
