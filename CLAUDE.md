# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-marketplace e-commerce logistics ETL platform. Two independent GCP Cloud Functions (Python 3.11) pull order and tracking event data from BigQuery (aggregated from 4 marketplaces: MELI, FALA, WALM, CENC) and push it to Supabase PostgreSQL via Edge Functions.

- **`functions/`** — Orders sync (legacy): queries BigQuery → transforms → sends to external webhook (65-min lookback, hourly schedule)
- **`function_shipment_tracking/`** — Shipment tracking sync (active): queries BigQuery → normalizes statuses → sends to Supabase Edge Function (20-min lookback, every 15 minutes)

GCP Project: `projectbamo` | Region: `us-central1` | Supabase: `obrhnfnqrvkfbgcpjixl.supabase.co`

## Deployment

**Deploy Orders Function:**
```bash
gcloud functions deploy push-orders-to-supabase \
  --gen2 --runtime=python311 --region=us-central1 \
  --source=functions --entry-point=process_orders \
  --trigger-http --allow-unauthenticated \
  --timeout=540s --memory=512Mi --max-instances=10
```

**Deploy Shipment Tracking Function:**
```bash
gcloud functions deploy sync-shipment-tracking \
  --gen2 --runtime=python311 --region=us-central1 \
  --source=function_shipment_tracking --entry-point=sync_shipment_tracking \
  --trigger-http --no-allow-unauthenticated \
  --timeout=540s --memory=1Gi --max-instances=5
```

**Test deployed functions:**
```bash
# Orders (public)
curl -X POST https://us-central1-projectbamo.cloudfunctions.net/push-orders-to-supabase

# Shipment tracking (authenticated)
curl -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://us-central1-projectbamo.cloudfunctions.net/sync-shipment-tracking
```

## Architecture & Data Flow

Both functions follow the same ETL pattern:

```
BigQueryService → TransformerService → [WebhookService | SupabaseService]
```

Each function's `main.py` orchestrates the pipeline:
1. `BigQueryService` loads marketplace SQL queries from `queries/` folder, executes a `UNION ALL` across all 4 marketplaces, returns a DataFrame
2. `TransformerService` validates, normalizes, and shapes records into the target API payload format
3. `WebhookService` / `SupabaseService` sends records in batches with exponential backoff retry (max 3 attempts)

## Key Domain Concepts

**Status normalization** (`function_shipment_tracking/utils/status_mapping.py`): Each marketplace uses different status strings. All are normalized to PlugUp standard statuses: `pending`, `ready_to_ship`, `dispatched`, `in_transit`, `out_for_delivery`, `delivered`, `delivery_failed`, `cancelled`, `returned`. Logic: try marketplace-specific map first → fall back to general map → default to `pending`.

**Company mapping** (`function_shipment_tracking/company_mapping.py`): BigQuery stores company names as strings (e.g. `"bamo_company"`), but Supabase requires UUIDs. This file is the manual mapping that must be updated when new companies are onboarded.

**Batch processing**: The shipment tracking function chunks events into batches of 100 (configurable via `BATCH_SIZE` env var) before sending to Supabase Edge Function.

**Idempotency**: The Supabase Edge Function (`process-shipment-tracking`) handles duplicate detection — the same tracking event sent twice will be deduplicated on the Supabase side.

## Environment Variables

**`function_shipment_tracking`:**
```
GCP_PROJECT, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
LOOKBACK_MINUTES (default: 20), BATCH_SIZE (default: 100),
MAX_RETRY_ATTEMPTS (default: 3), SUPABASE_TIMEOUT (default: 30), LOG_LEVEL (default: INFO)
```

**`functions` (orders):**
```
GCP_PROJECT, WEBHOOK_URL, WEBHOOK_TOKEN,
LOOKBACK_MINUTES (default: 65), WEBHOOK_TIMEOUT (default: 30), MAX_RETRY_ATTEMPTS (default: 3)
```

## SQL Queries

Each function has a `queries/` folder with one `.sql` file per marketplace. The BigQuery service dynamically loads all `.sql` files and combines them with `UNION ALL`. When adding a new marketplace, create a new `.sql` file in the appropriate `queries/` folder — it will be picked up automatically.

See `function_shipment_tracking/queries/README.md` for detailed field mapping documentation per marketplace.

## Marketplace-Specific Quirks

- **FALA**: Items are stored as arrays; transformer uses priority logic to pick the winning status
- **MELI**: `logistic_type` field is used to determine `courier_name`
- **WALM**: Order lines can have multiple statuses; transformer picks the most recent
- **CENC**: `facilityConfigId` maps to courier identification; Spanish-language status strings

## Logging

Both functions use structured JSON logging (`utils/logging_utils.py`) compatible with GCP Cloud Logging. Use severity levels INFO/WARN/ERROR. Do not log sensitive values (auth tokens, keys).
