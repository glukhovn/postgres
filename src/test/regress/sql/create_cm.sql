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



-- Try to drop compression method: fail because of dependent objects
DROP COMPRESSION METHOD jsonb2;

-- Drop compression method cascade
DROP COMPRESSION METHOD jsonb2 CASCADE;

SELECT * FROM jstest LIMIT 0;

DROP TABLE jstest;
