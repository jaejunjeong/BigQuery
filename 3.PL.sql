--DECLARE
DECLARE x INT64;

DECLARE d DATE DEFAULT CURRENT_DATE();

DECLARE x, y, z INT64 DEFAULT 0;

--SET
SET x = 5;

SET (a, b, c) = (1 + 3, 'foo', false);

--DECLARE, SET example
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

