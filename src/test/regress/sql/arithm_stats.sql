﻿-- create table
DROP TABLE IF EXISTS arithm_stats_test;

CREATE TABLE arithm_stats_test(
	i2 int2,
	i4 int4,
	i8 int8,
	f4 float4,
	f8 float8,
	num numeric,
	mon money,
	i interval,
	dt date,
	tm time,
	tmtz time with time zone,
	ts timestamp,
	tstz timestamp with time zone
);

-- insert test data
INSERT INTO arithm_stats_test SELECT NULL FROM generate_series(1, 1000);

CREATE OR REPLACE FUNCTION seconds_interval(seconds int) RETURNS interval
AS $$ SELECT interval '1 second' * seconds $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION seconds_to_time(seconds int) RETURNS time
AS $$ SELECT time '00:00:00' + interval '1 seconds' * seconds $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION seconds_to_timetz(seconds int) RETURNS timetz
AS $$ SELECT timetz '00:00:00' + interval '1 seconds' * seconds $$ LANGUAGE sql;

INSERT INTO arithm_stats_test(i2, i4, i8, f4, f8, num, mon, i, dt, tm, tmtz, ts, tstz)
	SELECT i, i, i, i, i, i, i,
			interval '1 seconds' * i,
			date '2000-01-01' + i,
			time '00:00:00' + interval '1 seconds' * i,
			time with time zone '00:00:00' + interval '1 seconds' * i,
			timestamp' 2000-01-01 00:00:00' + interval '1 seconds' * i,
			timestamp with time zone '2000-01-01 00:00:00'  + interval '1 seconds' * i
	FROM generate_series(0, 10000) i;

ANALYZE arithm_stats_test;

-- create helper functions
CREATE OR REPLACE FUNCTION arithm_stats_explain_jsonb(sql_query text)
RETURNS TABLE(explain_line json) AS
$$
BEGIN
	RETURN QUERY EXECUTE 'EXPLAIN (ANALYZE, FORMAT json) ' || sql_query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION arithm_stats_get_plan_and_actual_rows(sql_query text)
RETURNS TABLE(plan integer, actual integer) AS
$$
	SELECT
		(plan->>'Plan Rows')::integer plan,
		(plan->>'Actual Rows')::integer actual
	FROM (
		SELECT arithm_stats_explain_jsonb(sql_query) #> '{0,Plan,Plans,0}'
	) p(plan)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION arithm_stats_check_estimate(sql_query text, accuracy real)
RETURNS boolean AS
$$
	SELECT plan BETWEEN actual / (1 + accuracy) AND (actual + 1) * (1 + accuracy)
	FROM (SELECT * FROM arithm_stats_get_plan_and_actual_rows(sql_query)) x
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION arithm_stats_check_estimate2(sql_query text, accuracy real)
RETURNS TABLE(min integer, max integer) AS
$$
	SELECT (actual * (1 - accuracy))::integer, ((actual + 1) * (1 + accuracy))::integer
	FROM (SELECT * FROM arithm_stats_get_plan_and_actual_rows(sql_query)) x
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION check_arithm_stats_test_estimate(sql_condition text, accuracy real)
RETURNS boolean AS
$$
	SELECT arithm_stats_check_estimate('SELECT count(*) FROM arithm_stats_test WHERE ' || sql_condition, accuracy)
$$ LANGUAGE sql;

DROP FUNCTION IF EXISTS check_arithm_stats_test_estimate2(text, real);

CREATE OR REPLACE FUNCTION check_arithm_stats_test_estimate2(sql_condition text, accuracy real)
RETURNS TABLE(plan integer, actual integer) AS
$$
	SELECT arithm_stats_get_plan_and_actual_rows('SELECT count(*) FROM arithm_stats_test WHERE ' || sql_condition)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION
	check_arithm_stats_test_estimate_num(col text, add_type text, mul_type text)
RETURNS boolean[]
AS $$
	SELECT array_agg(check_arithm_stats_test_estimate(format(expr_format, $1, $2, $3), 0.1))
	FROM (VALUES
		('%1$I + %2$I(3000) > %2$I(5000)'),
		('%1$I - %2$I(3000) > %2$I(5000)'),
		('%1$I * (+2 :: %3$I) > %2$I(8000)'),
		('%1$I * (-2 :: %3$I) < %2$I(-6000)'),
		('%1$I / (+2 :: %3$I) > %2$I(4000)'),
		('%1$I / (-2 :: %3$I) < %2$I(-3000)'),
		('%2$I(3000) + %1$I > %2$I(5000)'),
		('%2$I(3000) - %1$I > %2$I(-5000)'),
		(' 2 :: %3$I * %1$I > %2$I(8000)'),
		('-2 :: %3$I * %1$I < %2$I(-6000)')
	) exprs(expr_format)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION
	check_arithm_stats_test_estimate_datetime(
		col_name text, col_type text, interval_type text, base_value text, have_minus boolean = true)
RETURNS boolean[]
AS $$
	SELECT array_agg(check_arithm_stats_test_estimate(format(expr_format, $1, $2, $3, $4), 0.1))
	FROM (VALUES
		('%1$I + %3$I(3000) > %2$I ''%4$s'' + %3$I(5000)'),
		('%3$I(3000) + %1$I > %2$I ''%4$s'' + %3$I(5000)'),
		(CASE $5 WHEN FALSE THEN NULL ELSE '%1$I - (%2$I ''%4$s'' + %3$I(5000)) > %3$I(3000)' END),
		(CASE $5 WHEN FALSE THEN NULL ELSE '(%2$I ''%4$s'' + %3$I(5000)) - %1$I > %3$I(3000)' END)
	) exprs(expr_format) WHERE expr_format IS NOT NULL
$$ LANGUAGE sql;

-- check NULL fraction estimation
SELECT check_arithm_stats_test_estimate($$i2 IS NULL$$, 0.03);
SELECT check_arithm_stats_test_estimate($$(i2 + 1) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(i4 + 1) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(i8 + 1) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(f4 + 1) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(f8 + 1) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(num + 1) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(mon + 1::money) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(i + interval '1 day') IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(dt + 1) IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(tm + interval '1 day') IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(tmtz + interval '1 day') IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(ts + interval '1 day') IS NULL$$, 0.01);
SELECT check_arithm_stats_test_estimate($$(tstz + interval '1 day') IS NULL$$, 0.01);

-- check numeric operators estimation
SELECT check_arithm_stats_test_estimate_num('i2',  'int2',    'int2');
SELECT check_arithm_stats_test_estimate_num('i2',  'int2',    'int4');
SELECT check_arithm_stats_test_estimate_num('i4',  'int4',    'int4');
SELECT check_arithm_stats_test_estimate_num('i8',  'int8',    'int8');
SELECT check_arithm_stats_test_estimate_num('f4',  'float4',  'float4');
SELECT check_arithm_stats_test_estimate_num('f8',  'float8',  'float8');
SELECT check_arithm_stats_test_estimate_num('num', 'numeric', 'numeric');

SELECT check_arithm_stats_test_estimate_num('mon', 'money',   'int2');
SELECT check_arithm_stats_test_estimate_num('mon', 'money',   'int4');
SELECT check_arithm_stats_test_estimate_num('mon', 'money',   'int8');
SELECT check_arithm_stats_test_estimate_num('mon', 'money',   'float4');
SELECT check_arithm_stats_test_estimate_num('mon', 'money',   'float8');
SELECT check_arithm_stats_test_estimate_num('mon', 'money',   'numeric');

SELECT check_arithm_stats_test_estimate_num('i', 'seconds_interval', 'int2');
SELECT check_arithm_stats_test_estimate_num('i', 'seconds_interval', 'int4');
SELECT check_arithm_stats_test_estimate_num('i', 'seconds_interval', 'int8');
SELECT check_arithm_stats_test_estimate_num('i', 'seconds_interval', 'float4');
SELECT check_arithm_stats_test_estimate_num('i', 'seconds_interval', 'float8');
SELECT check_arithm_stats_test_estimate_num('i', 'seconds_interval', 'numeric');

-- check date/time +/- interval operators estimation
SELECT check_arithm_stats_test_estimate_datetime('dt',   'date',      'int4',             '2000-01-01');
SELECT check_arithm_stats_test_estimate_datetime('tm',   'time',      'seconds_interval', '00:00:00');
SELECT check_arithm_stats_test_estimate_datetime('tmtz', 'timetz',    'seconds_interval', '00:00:00', false /* no timez - timetz operator */);
SELECT check_arithm_stats_test_estimate_datetime('ts',   'timestamp', 'seconds_interval', '2000-01-01 00:00:00');
SELECT check_arithm_stats_test_estimate_datetime('tstz', 'timestamp', 'seconds_interval', '2000-01-01 00:00:00');

-- check date + time operator estimation
SELECT check_arithm_stats_test_estimate($$date '2000-01-01 00:00' + tm   > timestamp   '2000-01-01 02:00' $$, 0.1);
SELECT check_arithm_stats_test_estimate($$tmtz + date '2000-01-01 00:00' > timestamptz '2000-01-01 02:00' $$, 0.1);

-- check division by zero protection
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM arithm_stats_test WHERE i4 / 0 > 0;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM arithm_stats_test WHERE 1 / i4 > 0;
