CREATE COMPRESSION METHOD jsonb2 FOR json HANDLER no_such_handler;

CREATE COMPRESSION METHOD jsonb FOR json HANDLER jsonb_handler;

CREATE COMPRESSION METHOD jsonb2 FOR json HANDLER jsonb_handler;

CREATE TABLE jstest(
	js text COMPRESSED jsonb
);

CREATE TABLE jstest(
	js text COMPRESSED jsonb2
);

CREATE TABLE jstest(
	js1 json,
	js2 json COMPRESSED jsonb2,
	js3 json COMPRESSED jsonbc,
	js4 json COMPRESSED jsonbc,
	js5 jsonb
);

INSERT INTO jstest
SELECT js, js, js, js, js
FROM (VALUES
	('{"key1": "val1", "key2": ["val2", 3, 4, 5]}'::json),
	('["val1", 2, {"k1": "v1", "k2": 2}, "", 5, "6"]'),
	('"val"'),
	('12345'),
	(('[' || repeat('"test", ', 10000) || '"test"]')::json)
) AS jsvals(js);

SELECT
	substring(js1::text for 100),
	substring(js2::text for 100),
	substring(js3::text for 100),
	substring(js4::text for 100),
	substring(js5::text for 100)
FROM jstest;

-- check page items size
CREATE EXTENSION pageinspect;

SELECT
	lp, lp_off, lp_flags, lp_len,
	CASE lp WHEN 5 THEN NULL ELSE t_data END
FROM
	heap_page_items(get_raw_page('jstest', 0));

SELECT
	lp,
	length(unnest(t_attrs)),
	CASE lp WHEN 5 THEN NULL ELSE unnest(t_attrs) END
FROM
	heap_page_item_attrs(
		get_raw_page('jstest', 0),
		'jstest'::regclass,
		false
	);

DROP EXTENSION pageinspect;

-- copy json values with different compression

INSERT INTO jstest SELECT js1, js2, js3, js4, js5 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js2, js3, js4, js5, js1 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js3, js4, js5, js1, js2 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js4, js5, js1, js2, js3 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js5, js1, js2, js3, js4 FROM jstest LIMIT 5;

SELECT
	substring(js1::text for 100),
	substring(js2::text for 100),
	substring(js3::text for 100),
	substring(js4::text for 100),
	substring(js5::text for 100)
FROM jstest;

-- alter column compression methods

SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest ALTER js1 SET COMPRESSED jsonbc;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest ALTER js2 SET NOT COMPRESSED;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest ALTER js3 SET NOT COMPRESSED;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest ALTER js3 SET COMPRESSED jsonb2;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest ADD js6 json COMPRESSED jsonbc;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

-- try to use existing jsonbc dictionary
DO
$$
DECLARE
	dict_id oid;
BEGIN
	SELECT substring(attcmoptions[1] from 9)
	INTO STRICT dict_id
	FROM pg_attribute
	WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'jstest')
	AND attname = 'js6';

	EXECUTE 'ALTER TABLE jstest ADD js7 json COMPRESSED jsonbc WITH (dict_id ''' || dict_id || ''')';
END
$$;

SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest DROP js6;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest ALTER js6 SET NOT COMPRESSED;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';

ALTER TABLE jstest ALTER js7 SET COMPRESSED jsonb2;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';


-- Try to drop compression method: fail because of dependent objects
DROP COMPRESSION METHOD jsonb2;

-- Drop compression method cascade
DROP COMPRESSION METHOD jsonb2 CASCADE;

SELECT * FROM jstest LIMIT 0;

DROP TABLE jstest;
SELECT relname, relkind FROM pg_class WHERE relname LIKE 'jstest%';


-- Test ALTER TYPE SET COMPRESSED
ALTER TYPE json SET COMPRESSED jsonb;
CREATE TABLE jstest (js json);

SELECT attcompression FROM pg_attribute WHERE attrelid = 'jstest'::regclass AND attnum = 1;

INSERT INTO jstest VALUES ('[ 123,  "abc", { "k" : "v" }  ]');
SELECT * FROM jstest;
DROP TABLE jstest;

ALTER TYPE json SET NOT COMPRESSED;
CREATE TABLE jstest (js json);

SELECT attcompression FROM pg_attribute WHERE attrelid = 'jstest'::regclass AND attnum = 1;

INSERT INTO jstest VALUES ('[ 123,  "abc", { "k" : "v" }  ]');
SELECT * FROM jstest;
DROP TABLE jstest;

-- Test compressable type creation
CREATE TYPE json2;

CREATE TEMP TABLE json2_procs AS
SELECT * FROM pg_proc p WHERE proname IN ('json_in', 'json_out', 'json_null_cm_handler');

UPDATE json2_procs
SET proname = replace(proname, 'json_', 'json2_');

UPDATE json2_procs
SET prorettype = (SELECT oid FROM pg_type WHERE typname = 'json2')
WHERE proname = 'json2_in';

UPDATE json2_procs
SET proargtypes = (SELECT oid::text::oidvector FROM pg_type WHERE typname = 'json2')
WHERE proname = 'json2_out';

INSERT INTO pg_proc
SELECT * FROM json2_procs;

CREATE COMPRESSION METHOD json2_null FOR json2 HANDLER json2_null_cm_handler;

CREATE TYPE json2 (
	INPUT  = json2_in,
	OUTPUT = json2_out,
	NULLCM = json2_null
);

CREATE TEMP TABLE tjson2(js json2);
INSERT INTO tjson2 VALUES ('abc');
INSERT INTO tjson2 VALUES ('["abc", {"key": 123}, null]');
SELECT * FROM tjson2;

DROP FUNCTION json2_null_cm_handler(internal);
DROP FUNCTION json2_in(cstring);
DROP FUNCTION json2_out(json2);
DROP FUNCTION json2_out(json2) CASCADE;
DROP FUNCTION json2_in(cstring);
DROP FUNCTION json2_null_cm_handler(internal);

DROP TABLE tjson2;

-- Test compression methods on domains
CREATE DOMAIN json_not_null AS json NOT NULL;

CREATE TEMP TABLE json_domain_test1(js json_not_null);
SELECT attcompression FROM pg_attribute WHERE attrelid = 'json_domain_test1'::regclass AND attnum = 1;
DROP TABLE json_domain_test1;

CREATE TEMP TABLE json_domain_test2(js json_not_null compressed jsonb);
SELECT attcompression FROM pg_attribute WHERE attrelid = 'json_domain_test2'::regclass AND attnum = 1;
DROP TABLE json_domain_test2;
