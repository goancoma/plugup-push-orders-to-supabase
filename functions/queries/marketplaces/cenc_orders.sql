-- Cencosud Orders Query
-- Extracts and standardizes Cencosud order data
WITH cencosud_orders as (
    select * except(rn) 
    from (
        select *, row_number() over(partition by subOrderNumber order by created_at desc) rn
        from `cencosud.raw_orders`
    ) 
    where rn = 1
)

SELECT DISTINCT
    'cenc' as marketplace,
    m0.subOrderNumber as order_id,
    s0.trackingNumber as shipping_id,
    s0.deliveryOption.translate as logistic_type,
    s0.status.name as status,
    'Not found' as shipping_status,
    i0.sellerSku as seller_sku,
    i0.sku as market_place_match_id,
    i0.name as sku_name,
    count(*) over(partition by m0.suborderNumber, i0.sku) as quantity,
    TIMESTAMP_ADD(m0.createdAt, INTERVAL 3 HOUR) as order_created_at,
    s0.dispatchDate as shipping_promise_date,
    '' as order_item_id
FROM cencosud_orders m0,
unnest(subOrders) as s0,
unnest(s0.items) as i0
WHERE TIMESTAMP_ADD(m0.createdAt, INTERVAL 3 HOUR) >= timestamp_sub(current_timestamp, interval @lookback_minutes minute)