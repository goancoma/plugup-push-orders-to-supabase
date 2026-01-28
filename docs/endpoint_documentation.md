# Process Shipment Tracking Edge Function

## Overview

This Supabase Edge Function processes shipment tracking events from GCP Cloud Function and stores them in the `shipment_tracking` table. It handles marketplace order ID to internal order ID mapping, duplicate detection, and comprehensive error handling.

## Endpoint

```
POST /functions/v1/process-shipment-tracking
```

## Authentication

Requires `Authorization: Bearer {SUPABASE_SERVICE_ROLE_KEY}` header.

## Request Format

```json
{
  "events": [
    {
      "marketplace_order_id": "MELI-123456",
      "marketplace": "meli",
      "company_id": "uuid-bamo",
      "event_status": "delivered",
      "event_timestamp": "2026-01-26T14:30:00Z",
      "event_location": "Santiago, Región Metropolitana",
      "courier_name": "Mercado Envíos Flex",
      "tracking_number": "SHIP-789",
      "notes": "Entregado exitosamente"
    }
  ]
}
```

### Required Fields

- `marketplace_order_id` (string): Order ID from marketplace
- `marketplace` (enum): One of "meli", "fala", "walm", "cenc"
- `company_id` (uuid): Company UUID for multi-tenant isolation
- `event_status` (string): Tracking event status
- `event_timestamp` (ISO 8601): When the event occurred

### Optional Fields

- `event_location` (string): Location where event occurred
- `courier_name` (string): Name of courier/delivery service
- `tracking_number` (string): Tracking number from courier
- `notes` (string): Additional notes about the event

## Response Format

### Success (200)

```json
{
  "status": "success",
  "processed": 2,
  "created": 1,
  "updated": 1,
  "skipped": 0,
  "errors": 0,
  "details": {
    "created_ids": ["uuid-event-1"],
    "updated_ids": ["uuid-event-2"],
    "skipped_orders": [],
    "error_messages": []
  }
}
```

### Partial Success (200)

```json
{
  "status": "partial_success",
  "processed": 2,
  "created": 1,
  "updated": 0,
  "skipped": 1,
  "errors": 0,
  "details": {
    "created_ids": ["uuid-event-1"],
    "updated_ids": [],
    "skipped_orders": ["FALA-999 (Order not found in Supabase)"],
    "error_messages": []
  }
}
```

### Error (400/401/500)

```json
{
  "status": "error",
  "message": "Invalid payload: events array required",
  "details": {}
}
```

## Business Logic

### 1. Order Lookup

The function maps `marketplace_order_id` to internal `order_id` using:

```sql
SELECT id, company_id
FROM orders
WHERE order_id = {marketplace_order_id}
  AND company_id = {company_id}
LIMIT 1;
```

If no order is found, the event is **skipped** (not an error).

### 2. Duplicate Detection (Idempotency)

Events are considered duplicates based on:
- `order_id`
- `event_status` 
- `event_timestamp`

If a duplicate is found, the function **updates** optional fields:
- `event_location`
- `courier_name`
- `tracking_number`
- `notes`
- `received_at` (updated to NOW())

### 3. Multi-Tenant Security

- Always validates that `company_id` from event matches `company_id` from order
- Uses Row Level Security (RLS) policies on `shipment_tracking` table
- Company isolation prevents cross-tenant data access

## Error Handling

### Validation Errors (400)

- Missing required fields
- Invalid marketplace enum
- Invalid UUID format for `company_id`
- Invalid ISO 8601 timestamp
- Empty events array
- More than 100 events per request

### Authentication Errors (401)

- Missing `Authorization` header
- Invalid Bearer token format

### Processing Errors

- **Order not found**: Event is skipped, not treated as error
- **Company ID mismatch**: Event is treated as error for security
- **Database errors**: Logged and returned as error

## Logging

All logs are structured JSON format:

```json
{
  "level": "INFO|WARN|ERROR",
  "function": "process-shipment-tracking",
  "action": "event_created|order_not_found|insert_failed",
  "timestamp": "2026-01-27T22:41:25.876Z",
  "order_id": "uuid",
  "event_status": "delivered",
  "marketplace": "meli"
}
```

### Log Actions

- `processing_started`: Function execution begins
- `event_created`: New tracking event inserted
- `event_updated`: Existing event updated
- `order_not_found`: Order lookup failed (warning)
- `company_id_mismatch`: Security violation (error)
- `insert_failed`: Database insert error
- `processing_completed`: Function execution summary

## Deployment

```bash
# Deploy the function
supabase functions deploy process-shipment-tracking

# Test locally
supabase functions serve process-shipment-tracking
```

## Testing

### Test Cases

1. **Happy Path - Create Event**
```bash
curl -X POST http://localhost:54321/functions/v1/process-shipment-tracking \
  -H "Authorization: Bearer YOUR_SERVICE_ROLE_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "marketplace_order_id": "MELI-TEST-001",
      "marketplace": "meli",
      "company_id": "c12585ee-c8f4-4103-b7f0-37bd62401a65",
      "event_status": "delivered",
      "event_timestamp": "2026-01-27T15:00:00Z",
      "courier_name": "Test Courier"
    }]
  }'
```

2. **Duplicate Event - Update**
Run the same request twice. First should create, second should update.

3. **Order Not Found - Skip**
```bash
curl -X POST http://localhost:54321/functions/v1/process-shipment-tracking \
  -H "Authorization: Bearer YOUR_SERVICE_ROLE_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "marketplace_order_id": "FAKE-999",
      "marketplace": "meli",
      "company_id": "c12585ee-c8f4-4103-b7f0-37bd62401a65",
      "event_status": "pending",
      "event_timestamp": "2026-01-27T15:00:00Z"
    }]
  }'
```

4. **Invalid Payload**
```bash
curl -X POST http://localhost:54321/functions/v1/process-shipment-tracking \
  -H "Authorization: Bearer YOUR_SERVICE_ROLE_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "marketplace_order_id": "MELI-001"
    }]
  }'
```

## Database Schema

The function interacts with these tables:

### `orders` table
- Used for marketplace order ID lookup
- Columns: `id`, `order_id`, `company_id`

### `shipment_tracking` table
- Target table for tracking events
- Columns: `id`, `order_id`, `company_id`, `event_status`, `event_timestamp`, `event_location`, `courier_name`, `tracking_number`, `notes`, `received_at`, `created_at`

## Performance Considerations

- Events are processed **sequentially** to avoid database conflicts
- Maximum 100 events per request to prevent timeouts
- Uses single database queries with proper indexes
- 60-second timeout (Supabase Edge Function default)

## Security Features

- Service Role Key authentication required
- Multi-tenant company isolation
- Input validation and sanitization
- SQL injection protection via Supabase client
- CORS headers for web requests

## Integration with GCP Cloud Function

This Edge Function is designed to be called from a GCP Cloud Function that:

1. Queries BigQuery for tracking updates every 15 minutes
2. Batches events by company and marketplace
3. Calls this endpoint with batched events
4. Handles retries and error reporting

The GCP Cloud Function should use the `SUPABASE_SERVICE_ROLE_KEY` for authentication and respect the 100-event batch limit.