--
-- CIRCLE
--

CREATE TABLE CIRCLE_TBL (f1 circle);

INSERT INTO CIRCLE_TBL VALUES ('<(5,1),3>');

INSERT INTO CIRCLE_TBL VALUES ('<(1,2),100>');

INSERT INTO CIRCLE_TBL VALUES ('1,3,5');

INSERT INTO CIRCLE_TBL VALUES ('((1,2),3)');

INSERT INTO CIRCLE_TBL VALUES ('<(100,200),10>');

INSERT INTO CIRCLE_TBL VALUES ('<(100,1),115>');

-- bad values

INSERT INTO CIRCLE_TBL VALUES ('<(-100,0),-100>');

INSERT INTO CIRCLE_TBL VALUES ('1abc,3,5');

INSERT INTO CIRCLE_TBL VALUES ('(3,(1,2),3)');

SELECT * FROM CIRCLE_TBL;

SELECT '' AS six, center(f1) AS center
  FROM CIRCLE_TBL;

SELECT '' AS six, radius(f1) AS radius
  FROM CIRCLE_TBL;

SELECT '' AS six, diameter(f1) AS diameter
  FROM CIRCLE_TBL;

SELECT '' AS two, f1 FROM CIRCLE_TBL WHERE radius(f1) < 5;

SELECT '' AS four, f1 FROM CIRCLE_TBL WHERE diameter(f1) >= 10;

SELECT '' as five, c1.f1 AS one, c2.f1 AS two, (c1.f1 <-> c2.f1) AS distance
  FROM CIRCLE_TBL c1, CIRCLE_TBL c2
  WHERE (c1.f1 < c2.f1) AND ((c1.f1 <-> c2.f1) > 0)
  ORDER BY distance, area(c1.f1), area(c2.f1);

--
-- Test the SP-GiST index
--

CREATE TEMPORARY TABLE quad_circle_tbl (id int, c circle);

INSERT INTO quad_circle_tbl
	SELECT (x - 1) * 100 + y, circle(point(x * 10, y * 10), 1 + (x + y) % 10)
	FROM generate_series(1, 100) x,
		 generate_series(1, 100) y;

INSERT INTO quad_circle_tbl
	SELECT i, '<(200, 300), 5>'
	FROM generate_series(10001, 11000) AS i;

INSERT INTO quad_circle_tbl
	VALUES
		(11001, NULL),
		(11002, NULL),
		(11003, '<(0,100), infinity>'),
		(11004, '<(-infinity,0),1000>'),
		(11005, '<(infinity,-infinity),infinity>');

CREATE INDEX quad_circle_tbl_idx ON quad_circle_tbl USING spgist(c);

-- get reference results for ORDER BY distance from seq scan
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

CREATE TEMP TABLE quad_circle_tbl_ord_seq1 AS
SELECT rank() OVER (ORDER BY c <-> point '123,456') n, c <-> point '123,456' dist, id
FROM quad_circle_tbl;

CREATE TEMP TABLE quad_circle_tbl_ord_seq2 AS
SELECT rank() OVER (ORDER BY c <-> point '123,456') n, c <-> point '123,456' dist, id
FROM quad_circle_tbl WHERE c <@ circle '<(300,400),200>';

-- check results results from index scan
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_bitmapscan = ON;

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c << circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c << circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c &< circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c &< circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c && circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c && circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c &> circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c &> circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c >> circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c >> circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c <<| circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c <<| circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c &<| circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c &<| circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c |&> circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c |&> circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c |>> circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c |>> circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c @> circle '<(300,400),1>';
SELECT count(*) FROM quad_circle_tbl WHERE c @> circle '<(300,400),1>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c <@ circle '<(300,400),200>';
SELECT count(*) FROM quad_circle_tbl WHERE c <@ circle '<(300,400),200>';

EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_circle_tbl WHERE c ~= circle '<(300,400),1>';
SELECT count(*) FROM quad_circle_tbl WHERE c ~= circle '<(300,400),1>';

-- test ORDER BY distance
SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF)
SELECT rank() OVER (ORDER BY c <-> point '123,456') n, c <-> point '123,456' dist, id
FROM quad_circle_tbl;

CREATE TEMP TABLE quad_circle_tbl_ord_idx1 AS
SELECT rank() OVER (ORDER BY c <-> point '123,456') n, c <-> point '123,456' dist, id
FROM quad_circle_tbl;

SELECT *
FROM quad_circle_tbl_ord_seq1 seq FULL JOIN quad_circle_tbl_ord_idx1 idx
	ON seq.n = idx.n AND seq.id = idx.id AND
		(seq.dist = idx.dist OR seq.dist IS NULL AND idx.dist IS NULL)
WHERE seq.id IS NULL OR idx.id IS NULL;


EXPLAIN (COSTS OFF)
SELECT rank() OVER (ORDER BY c <-> point '123,456') n, c <-> point '123,456' dist, id
FROM quad_circle_tbl WHERE c <@ circle '<(300,400),200>';

CREATE TEMP TABLE quad_circle_tbl_ord_idx2 AS
SELECT rank() OVER (ORDER BY c <-> point '123,456') n, c <-> point '123,456' dist, id
FROM quad_circle_tbl WHERE c <@ circle '<(300,400),200>';

SELECT *
FROM quad_circle_tbl_ord_seq2 seq FULL JOIN quad_circle_tbl_ord_idx2 idx
	ON seq.n = idx.n AND seq.id = idx.id AND
		(seq.dist = idx.dist OR seq.dist IS NULL AND idx.dist IS NULL)
WHERE seq.id IS NULL OR idx.id IS NULL;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
