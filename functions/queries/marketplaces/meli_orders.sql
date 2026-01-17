WITH raw_enriched_orders AS (
  SELECT
    *
  FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY marketplace_order_id ORDER BY processed_at DESC) AS rn
    FROM `plugup_data.orders_enriched`
  )
  WHERE rn = 1
),
meli_enriched_orders AS (
    SELECT 
        marketplace_order_id as reference,
        JSON_EXTRACT_SCALAR(shipping_info, '$.shipping_id') as shipping_id,
        JSON_EXTRACT_SCALAR(shipping_info, '$.status') as shipping_status,
        order_status,
        order_created_at,
        JSON_EXTRACT_SCALAR(shipping_info, '$.estimated_delivery') as delivery_promise,
        JSON_EXTRACT_SCALAR(custom_fields, '$.pack_id') as pack_id,
        processed_at as order_creation_timestamp,
        items,
        JSON_EXTRACT_SCALAR(shipping_info, '$.logistic_type') as raw_logistic_type
    FROM raw_enriched_orders
    WHERE marketplace = 'MELI'
),
flattened_items AS (
    SELECT 
        reference,
        pack_id,
        shipping_id,
        shipping_status,
        order_status,
        order_created_at,
        delivery_promise,
        order_creation_timestamp,
        raw_logistic_type,
        JSON_EXTRACT_SCALAR(item, '$.seller_sku') as seller_sku,
        JSON_EXTRACT_SCALAR(item, '$.marketplace_item_id') as market_place_match_id,
        JSON_EXTRACT_SCALAR(item, '$.title') as sku_name,
        CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS INT64) as quantity,
        JSON_EXTRACT_SCALAR(item, '$.variation_id') as order_item_id
    FROM meli_enriched_orders,
    UNNEST(JSON_EXTRACT_ARRAY(items)) as item
)

SELECT DISTINCT
    'meli' as marketplace,
    CAST(COALESCE(pack_id, reference) AS STRING) as order_id,
    CAST(shipping_id AS STRING) as shipping_id,
    CASE
        WHEN raw_logistic_type = "fulfillment" THEN "full"
        WHEN raw_logistic_type = "self_service" THEN "flex"
        WHEN raw_logistic_type = "cross_docking" THEN "colecta"
        WHEN raw_logistic_type = "xd_drop_off" THEN "centro envio"
        WHEN LOWER(raw_logistic_type) like "blue%" and LOWER(raw_logistic_type) like "%express" THEN "bluexpress"
        WHEN raw_logistic_type IS NULL THEN "full"
        ELSE raw_logistic_type
    END AS logistic_type,
    order_status as status,
    shipping_status,
    seller_sku,
    market_place_match_id,
    sku_name,
    quantity,
    order_created_at,
    TIMESTAMP_ADD(order_created_at, interval 2 day) as shipping_promise_date,
    COALESCE(order_item_id, '') as order_item_id
FROM flattened_items
WHERE order_creation_timestamp >= timestamp_sub(current_timestamp, interval @lookback_minutes minute);