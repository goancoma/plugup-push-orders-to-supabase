-- ENHANCED CENC MIGRATION QUERY
-- Uses plugup_data.orders_enriched with enhanced items structure

WITH raw_enriched_orders AS (
  SELECT
    *
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY marketplace_order_id ORDER BY processed_at DESC) AS rn
    FROM `plugup_data.orders_enriched`
  )
  WHERE rn = 1
), 
cenc_orders AS (
  SELECT
    marketplace_order_id,
    order_status,
    order_created_at,
    JSON_QUERY_ARRAY(items) as items_array,
    JSON_EXTRACT_SCALAR(shipping_info, '$.carrier') as order_level_carrier,
    JSON_EXTRACT_SCALAR(custom_fields, '$.webhook_seller_id') as seller_id,
    company_id
  FROM raw_enriched_orders
  WHERE marketplace = 'CENC'
    AND processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), interval @lookback_minutes minute)
),
cenc_items_unnested AS (
  SELECT DISTINCT
    marketplace_order_id,
    order_status,
    order_created_at,
    seller_id,
    company_id,
    JSON_EXTRACT_SCALAR(item, '$.order_item_id') as order_item_id,
    JSON_EXTRACT_SCALAR(item, '$.carrier') as logistic_type,
    JSON_EXTRACT_SCALAR(item, '$.status_name') as status,
    JSON_EXTRACT_SCALAR(item, '$.arrival_date') as shipping_promise_date,
    CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS INT64) as quantity,
    -- Now populated from sub_order.items[].sellerSku
    JSON_EXTRACT_SCALAR(item, '$.seller_sku') as seller_sku,
    -- Now populated from sub_order.items[].sku
    JSON_EXTRACT_SCALAR(item, '$.marketplace_product_id') as marketplace_product_id,
    -- Now populated from sub_order.items[].name
    JSON_EXTRACT_SCALAR(item, '$.product_name') as sku_name
  FROM cenc_orders,
  UNNEST(items_array) as item
)
SELECT 
  'cenc' as marketplace,
  CAST(marketplace_order_id AS STRING) as order_id,
  CAST(marketplace_order_id AS STRING) as shipping_id,
  COALESCE(logistic_type, 'Unknown') as logistic_type,
  COALESCE(status, order_status) as status,
  "active" as shipping_status,
  seller_sku,
  marketplace_product_id as market_place_match_id,
  sku_name,
  COALESCE(quantity, 1) as quantity,
  order_created_at,
  TIMESTAMP(shipping_promise_date) as shipping_promise_date,
  order_item_id,
  company_id
FROM cenc_items_unnested
ORDER BY order_created_at DESC, order_item_id;
