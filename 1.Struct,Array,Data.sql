-- Struct 확인
select
	device, 
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`

-- Struct 내 항목 선택 (추출 대상 항목을 struct 컬럼 이름에 . 을 붙여 선택 )
select
	device, 
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
  
-- Array 확인
select
	items, 
from 
	`bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` 
where 
	event_name = 'add_to_cart'

-- Array 내 객체 unnest 후 선택 a
select 
  item_id,
  item_name,
  user_pseudo_id
from
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`, 
	unnest(items)
where
	event_name = 'add_to_cart'

-- Array 내 객체 unnest 후 선택 b
select
  (select array_agg(item_id) from unnest(items)) item_id, 
  (select array_agg(item_name) from unnest(items)) item_name, 
  user_pseudo_id
from
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
where
	event_name = 'add_to_cart'

-- timestamp casting for date aggregation
select 
  date(timestamp_micros(event_timestamp)) date,
  item_id,
  item_name,
  count(distinct user_pseudo_id) user_count 
from
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`, 
	unnest(items)
where
	event_name = 'add_to_cart'
group by 1, 2, 3
