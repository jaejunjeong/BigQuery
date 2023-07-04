--DECLARE
DECLARE x INT64;

DECLARE d DATE DEFAULT CURRENT_DATE();

DECLARE x, y, z INT64 DEFAULT 0;

--SET
SET x = 5;

SET (a, b, c) = (1 + 3, 'foo', false);

--Example1
DECLARE target_word STRING DEFAULT 'methinks';
DECLARE corpus_count, word_count INT64;

SET (corpus_count, word_count) = (
  SELECT AS STRUCT COUNT(DISTINCT corpus), SUM(word_count)
  FROM bigquery-public-data.samples.shakespeare
  WHERE LOWER(word) = target_word
);

SELECT
  FORMAT('Found %d occurrences of "%s" across %d Shakespeare works',
         word_count, target_word, corpus_count) AS result;

-- 결과 : Found 151 occurrences of "methinks" across 38 Shakespeare works

--EXECUTE IMMEDIATE
EXECUTE IMMEDIATE """
  SELECT ? FROM TABLE
  """
[ INTO variable[, ...] ] 
[ USING identifier[, ...] ];

--LOOP
DECLARE x INT64 DEFAULT 0;
LOOP
  SET x = x + 1;
  IF x >= 10 THEN
    LEAVE;
  END IF;
END LOOP;
SELECT x;

--IF
DECLARE target_product_id INT64 DEFAULT 103;
IF EXISTS(SELECT 1 FROM schema.products
           WHERE product_id = target_product_id) THEN
  SELECT CONCAT('found product ', CAST(target_product_id AS STRING));
  ELSEIF EXISTS(SELECT 1 FROM schema.more_products
           WHERE product_id = target_product_id) THEN
  SELECT CONCAT('found product from more_products table',
  CAST(target_product_id AS STRING));
ELSE
  SELECT CONCAT('did not find product ', CAST(target_product_id AS STRING));
END IF;

--TCL
BEGIN TRANSACTION;

-- Create a temporary table of new arrivals from warehouse #1
CREATE TEMP TABLE tmp AS
SELECT * FROM myschema.NewArrivals WHERE warehouse = 'warehouse #1';

-- Delete the matching records from the original table.
DELETE myschema.NewArrivals WHERE warehouse = 'warehouse #1';

-- Merge the matching records into the Inventory table.
MERGE myschema.Inventory AS I
USING tmp AS T
ON I.product = T.product
WHEN NOT MATCHED THEN
 INSERT(product, quantity, supply_constrained)
 VALUES(product, quantity, false)
WHEN MATCHED THEN
 UPDATE SET quantity = I.quantity + T.quantity;

DROP TABLE tmp;

COMMIT TRANSACTION;

--Example2
declare fill_period int64 default 3;

begin
  declare min_date date;
  declare max_date date;

  -- 1. 현재 add_to_cart 테이블 기록 확인
  execute immediate """
    select min(date), max(date) from temp.add_to_cart
  """
  into min_date, max_date;

  -- 2. 마지막 기록일로부터 n일간 데이터 가져오기
  create temp table recent_data as 
    select 
      date(timestamp_micros(event_timestamp)) date,
      item_id,
      item_name,
      count(distinct user_pseudo_id) user_count 
    from
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`, 
      unnest(items)
    where
      _table_suffix between format_date('%Y%m%d', max_date+1) and format_date('%Y%m%d', max_date+1+fill_period)
      and event_name = 'add_to_cart'
    group by 1, 2, 3
  ;

  -- 3. 테이블 머지
  merge temp.add_to_cart as a 
  using recent_data as r 
  on a.date = r.date and a.item_id = r.item_id and a.item_name = r.item_name
  when not matched then 
    insert(date, item_id, item_name, user_count)
    values(date, item_id, item_name, user_count)
  when matched then 
    update set user_count = r.user_count;
  
  -- 4. 임시 테이블 삭제 
  drop table recent_data;

  -- 5. 업데이트 기록 확인
  select min(date), max(date) from temp.add_to_cart;
end;
