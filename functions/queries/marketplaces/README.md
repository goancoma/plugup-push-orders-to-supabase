# Marketplace Queries

This directory contains individual SQL query files for each marketplace. Each file is self-contained and follows the same output schema.

## Files

- `walmart_orders.sql` - Walmart marketplace orders
- `cencosud_orders.sql` - Cencosud marketplace orders  
- `fala_orders.sql` - FALA marketplace orders
- `meli_orders.sql` - MELI marketplace orders

## Output Schema

All queries output the following columns:
- `marketplace` - Marketplace identifier ('walm', 'cenc', 'fala', 'meli')
- `order_id` - Order identifier
- `shipping_id` - Shipping/tracking identifier
- `logistic_type` - Logistics method/type
- `status` - Order status
- `shipping_status` - Shipping status
- `seller_sku` - Seller SKU
- `market_place_match_id` - Marketplace SKU/match ID
- `sku_name` - Product name
- `quantity` - Item quantity
- `order_created_at` - Order creation timestamp
- `shipping_promise_date` - Promised shipping/delivery date
- `order_item_id` - Order item identifier

## Usage

These files are automatically loaded and combined by `BigQueryService._build_unified_query()`. 

To add a new marketplace:
1. Create a new `{marketplace}_orders.sql` file in this directory
2. Follow the same output schema as existing files
3. The service will automatically include it in the unified query

To remove a marketplace:
1. Delete the corresponding SQL file
2. Or pass a custom `marketplaces` list to `fetch_recent_orders()`

## Parameters

All queries support the `@lookback_minutes` parameter for filtering recent orders.