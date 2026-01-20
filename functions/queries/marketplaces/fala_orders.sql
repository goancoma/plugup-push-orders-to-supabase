-- FALA Orders Migration Query - New System
WITH raw_enriched_orders AS (
  SELECT
    *
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY marketplace_order_id ORDER BY processed_at DESC) AS rn
    FROM `plugup_data.orders_enriched`
  )
  WHERE rn = 1
), 
fala_enriched_orders AS (
  SELECT 
    JSON_EXTRACT_SCALAR(custom_fields, '$.order_number') as marketplace_order_id,
    order_status,
    order_created_at,
    JSON_EXTRACT_SCALAR(shipping_info, '$.shipment_provider') as shipment_provider,
    JSON_EXTRACT_SCALAR(shipping_info, '$.promised_shipping_time') as promised_shipping_time,
    items,
    processed_at as order_creation_timestamp
  FROM raw_enriched_orders
  WHERE marketplace = 'FALA'
),
fala_items_unnested AS (
  SELECT 
    marketplace_order_id,
    order_status,
    order_created_at,
    shipment_provider,
    promised_shipping_time,
    JSON_EXTRACT_SCALAR(item, '$.seller_sku') as seller_sku,
    JSON_EXTRACT_SCALAR(item, '$.marketplace_sku') as marketplace_sku,
    JSON_EXTRACT_SCALAR(item, '$.title') as sku_name,
    CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS INT64) as quantity,
    JSON_EXTRACT_SCALAR(item, '$.marketplace_item_id') as order_item_id,
    order_creation_timestamp
  FROM fala_enriched_orders,
  UNNEST(JSON_QUERY_ARRAY(items)) as item  -- Use JSON_QUERY_ARRAY instead
)
-- Final output matching legacy format
SELECT DISTINCT
  'fala' as marketplace,
  CAST(marketplace_order_id AS STRING) as order_id,
  CAST(marketplace_order_id AS STRING) as shipping_id,
  shipment_provider as logistic_type,
  order_status as status,
  "active" as shipping_status,
  seller_sku,
  marketplace_sku as market_place_match_id,
  sku_name,
  COALESCE(quantity, 1) as quantity,
  order_created_at,
  COALESCE(TIMESTAMP(promised_shipping_time), order_created_at) as shipping_promise_date,
  order_item_id
FROM fala_items_unnested
WHERE order_creation_timestamp >= timestamp_sub(current_timestamp, interval @lookback_minutes minute);