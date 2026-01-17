-- Walmart Orders Query
-- Extracts and standardizes Walmart order data
WITH walmart_orders as (
    select * except(rn) 
    from (
        select *, row_number() over(partition by purchaseOrderId order by created_at desc) rn
        from `walmart.raw_orders`
    ) 
    where rn = 1
)

SELECT DISTINCT
    'walm' as marketplace,
    cast(customerOrderId as string) as order_id,
    cast(t2.trackingInfo.shipmentNo as string) as shipping_id,
    shippingInfo.methodCode as logistic_type,
    t2.status,
    t2.trackingInfo.packageStatus as shipping_status,
    t.item.sku as seller_sku,
    t.item.sku as market_place_match_id,
    t.item.productName as sku_name,
    cast(t.orderLineQuantity.amount as int64) as quantity,
    timestamp_millis(orderDate) as order_created_at,
    timestamp_millis(shippingInfo.estimatedShipDate) as shipping_promise_date,
    '' as order_item_id
FROM walmart_orders,
UNNEST(orderLines.orderLine) AS t,
UNNEST(t.orderLineStatuses.orderLineStatus) AS t2
WHERE timestamp_millis(orderDate) >= timestamp_sub(current_timestamp, interval @lookback_minutes minute)