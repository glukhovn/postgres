--
-- BTREE_INDEX
-- test retrieval of min/max keys for each index
--

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno < 1;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno >= 9999;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno = 4500;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno < '1'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno >= '9999'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno = '4500'::name;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno < '1'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno >= '9999'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno = '4500'::text;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno < '1'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno >= '9999'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno = '4500'::float8;

--
-- Check correct optimization of LIKE (special index operator support)
-- for both indexscan and bitmapscan cases
--

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;

set enable_indexscan to false;
set enable_bitmapscan to true;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;

--
-- Test B-tree page deletion. In particular, deleting a non-leaf page.
--

-- First create a tree that's at least four levels deep. The text inserted
-- is long and poorly compressible. That way only a few index tuples fit on
-- each page, allowing us to get a tall tree with fewer pages.
create table btree_tall_tbl(id int4, t text);
create index btree_tall_idx on btree_tall_tbl (id, t) with (fillfactor = 10);
insert into btree_tall_tbl
  select g, g::text || '_' ||
          (select string_agg(md5(i::text), '_') from generate_series(1, 50) i)
from generate_series(1, 100) g;

-- Delete most entries, and vacuum. This causes page deletions.
delete from btree_tall_tbl where id < 950;
vacuum btree_tall_tbl;

--
-- Test B-tree insertion with a metapage update (XLOG_BTREE_INSERT_META
-- WAL record type). This happens when a "fast root" page is split.
--

-- The vacuum above should've turned the leaf page into a fast root. We just
-- need to insert some rows to cause the fast root page to split.
insert into btree_tall_tbl (id, t)
  select g, repeat('x', 100) from generate_series(1, 500) g;

---
--- Test B-tree distance ordering
---

SET enable_bitmapscan = OFF;

-- temporarily disable bt_i4_index index on bt_i4_heap(seqno)
UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'bt_i4_index'::regclass;

CREATE INDEX bt_i4_heap_random_idx ON bt_i4_heap USING btree(random, seqno);

-- test unsupported orderings (by non-first index attribute or by more than one order keys)
EXPLAIN (COSTS OFF) SELECT * FROM bt_i4_heap ORDER BY seqno <-> 0;
EXPLAIN (COSTS OFF) SELECT * FROM bt_i4_heap ORDER BY random <-> 0, seqno <-> 0;
EXPLAIN (COSTS OFF) SELECT * FROM bt_i4_heap ORDER BY random <-> 0, random <-> 1;

EXPLAIN (COSTS OFF)
SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 4000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 4000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 10000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 0;

EXPLAIN (COSTS OFF)
SELECT * FROM bt_i4_heap
WHERE
	random > 1000000 AND (random, seqno) < (6000000, 0) AND
	random IN (1809552, 1919087, 2321799, 2648497, 3000193, 3013326, 4157193, 4488889, 5257716, 5593978, NULL)
ORDER BY random <-> 3000000;

SELECT * FROM bt_i4_heap
WHERE
	random > 1000000 AND (random, seqno) < (6000000, 0) AND
	random IN (1809552, 1919087, 2321799, 2648497, 3000193, 3013326, 4157193, 4488889, 5257716, 5593978, NULL)
ORDER BY random <-> 3000000;

DROP INDEX bt_i4_heap_random_idx;

CREATE INDEX bt_i4_heap_random_idx ON bt_i4_heap USING btree(random DESC, seqno);

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 4000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 10000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 0;

DROP INDEX bt_i4_heap_random_idx;

-- enable bt_i4_index index on bt_i4_heap(seqno)
UPDATE pg_index SET indisvalid = true WHERE indexrelid = 'bt_i4_index'::regclass;


CREATE TABLE tenk3 AS SELECT thousand, tenthous FROM tenk1;

INSERT INTO tenk3 VALUES (NULL, 1), (NULL, 2), (NULL, 3);

-- Test distance ordering by ASC index
CREATE INDEX tenk3_idx ON tenk3 USING btree(thousand, tenthous);

EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 998;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 998;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 0;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000) AND thousand < 1000
ORDER BY thousand <-> 10000;

SELECT thousand, tenthous FROM tenk3
ORDER BY thousand <-> 500
OFFSET 9970;

EXPLAIN (COSTS OFF)
SELECT * FROM tenk3
WHERE thousand > 100 AND thousand < 800 AND
	thousand = ANY(ARRAY[0, 123, 234, 345, 456, 678, 901, NULL]::int2[])
ORDER BY thousand <-> 300::int8;

SELECT * FROM tenk3
WHERE thousand > 100 AND thousand < 800 AND
	thousand = ANY(ARRAY[0, 123, 234, 345, 456, 678, 901, NULL]::int2[])
ORDER BY thousand <-> 300::int8;

DROP INDEX tenk3_idx;

-- Test distance ordering by DESC index
CREATE INDEX tenk3_idx ON tenk3 USING btree(thousand DESC, tenthous);

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 998;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 0;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000) AND thousand < 1000
ORDER BY thousand <-> 10000;

SELECT thousand, tenthous FROM tenk3
ORDER BY thousand <-> 500
OFFSET 9970;

DROP INDEX tenk3_idx;

DROP TABLE tenk3;

-- Test distance ordering on by-ref types
CREATE TABLE knn_btree_ts (ts timestamp);

INSERT INTO knn_btree_ts
SELECT timestamp '2017-05-03 00:00:00' + tenthous * interval '1 hour'
FROM tenk1;

CREATE INDEX knn_btree_ts_idx ON knn_btree_ts USING btree(ts);

SELECT ts, ts <-> timestamp '2017-05-01 00:00:00' FROM knn_btree_ts ORDER BY 2 LIMIT 20;
SELECT ts, ts <-> timestamp '2018-01-01 00:00:00' FROM knn_btree_ts ORDER BY 2 LIMIT 20;

DROP TABLE knn_btree_ts;

RESET enable_bitmapscan;
