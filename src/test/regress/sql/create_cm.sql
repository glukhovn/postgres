CREATE COMPRESSION METHOD jsonb2 HANDLER no_such_handler;

CREATE COMPRESSION METHOD jsonb HANDLER jsonb_handler;

CREATE COMPRESSION METHOD jsonb2 HANDLER jsonb_handler;

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
