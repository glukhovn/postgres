#ifndef JSONBC_DICT_H
#define JSONBC_DICT_H

typedef struct
{
	const char *s;
	int			len;
} KeyName;

extern int32 getIdByName(KeyName name);
extern KeyName getNameById(int32 id);

#endif /* JSONBC_DICT_H */
