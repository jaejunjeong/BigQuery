--SQL UDF
CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
  RETURNS FLOAT64
  AS ((x + 4) / y);

SELECT val, AddFourAndDivide(val, 2)
  FROM UNNEST([2,3,5,8]) AS val;

--Java Script UDF
CREATE TEMP FUNCTION multiplyInputs(x FLOAT64, y FLOAT64)
RETURNS FLOAT64
LANGUAGE js AS r"""
  return x*y;
""";

WITH numbers AS
  (SELECT 1 AS x, 5 as y
  UNION ALL
  SELECT 2 AS x, 10 as y
  UNION ALL
  SELECT 3 as x, 15 as y)
SELECT x, y, multiplyInputs(x, y) as product
FROM numbers;

--OPTIONS 문을 사용해 Google Cloud Storage 내에 보관된 js 파일 import 가능(매개변수 이름이 조회 테이블의 필드 이름과 같을 경우 오류가 발생하므로 주의)
CREATE TEMP FUNCTION myFunc(a FLOAT64, b STRING)
  RETURNS STRING
  LANGUAGE js
  OPTIONS (
    library=["gs://my-bucket/path/to/lib1.js", "gs://my-bucket/path/to/lib2.js"]
  )
  AS
r"""
    // Assumes 'doInterestingStuff' is defined in one of the library files.
    return doInterestingStuff(a, b);
""";

SELECT myFunc(3.14, 'foo');

--TVF
CREATE OR REPLACE TABLE FUNCTION mydataset.names_by_year(y INT64)
AS
  SELECT year, name, SUM(number) AS total
  FROM `bigquery-public-data.usa_names.usa_1910_current`
  WHERE year = y
  GROUP BY year, name

--EXAMPLE1
create temp function get_value(params any type, key string) as (
  (select value.string_value from unnest(params) where key = key)
);

select
  event_timestamp,
  event_params,
  (select value.string_value from unnest(event_params) where key='page_location'),
  get_value(event_params, 'page_location')
from
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
where
  event_name = 'add_to_cart'
  and _table_suffix between 
    format_date('%Y%m%d', date_sub(end_dt, interval 2 day)) 
    and format_date('%Y%m%d', end_dt)
