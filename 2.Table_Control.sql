-- Create Table
create table temp.add_to_cart as (
  select 
    date(timestamp_micros(event_timestamp)) date,
    item_id,
    item_name,
    count(distinct user_pseudo_id) user_count 
  from
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210111`, 
    unnest(items)
  where
    event_name = 'add_to_cart'
  group by 1, 2, 3
)

--Wildcard 테이블 조회
SELECT 
    DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS date, 
    COUNT(DISTINCT user_pseudo_id) AS active_user 
FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210111` 
GROUP BY 
    date

--_Table_SUFFIX
SELECT 
    DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS date, 
    COUNT(DISTINCT user_pseudo_id) AS active_user 
FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
WHERE 
    _table_suffix between "20210104" and "20210111"
GROUP BY 
    date

----파티션 테이블 용량 차이 확인
select 
  *
from `bigquery-public-data.deps_dev_v1.Advisories`
--하단 쿼리가 조회량이 작아짐
select 
  *
from `bigquery-public-data.deps_dev_v1.Advisories` 
where 
  SnapshotAt > '2022-05-20'

----클러스터 테이블 용량 차이 확인
select
  *
from 
  `bigquery-public-data.deps_dev_v1.Dependencies`
where 
	SnapshotAt < '2022-05-09'
--하단 쿼리가 조회량이 작아짐
select
  *
from 
  `bigquery-public-data.deps_dev_v1.Dependencies`
where
  SnapshotAt < '2022-05-09'
  and system = 'PYPI'
  and name = 'urllib3'
  and version = '1.26.9'

--테이블 샘플링 [tablesample system (N percent)]
select
  *
from 
  `bigquery-public-data.deps_dev_v1.Dependencies` tablesample system (10 percent)
where
  SnapshotAt < '2022-05-09'
  and system = 'PYPI'
  and name = 'urllib3'
  and version = '1.26.9'

--시간 지정 쿼리
select 
	*
from 
	temp.dev
  for system_time as of timestamp_sub(current_timestamp, interval 1 minute)
