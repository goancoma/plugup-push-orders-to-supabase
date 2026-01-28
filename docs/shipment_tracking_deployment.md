# Shipment Tracking Cloud Function Deployment Guide

## 🎯 Overview

This document provides comprehensive deployment instructions for the `sync-shipment-tracking` Cloud Function Gen 2, which synchronizes shipment tracking events from BigQuery to Supabase every 15 minutes.

## 📁 Project Structure

```
function_shipment_tracking/
├── main.py                     # Cloud Function entry point
├── config.py                   # Configuration management
├── requirements.txt            # Python dependencies
├── services/
│   ├── __init__.py
│   ├── bigquery_service.py     # BigQuery operations
│   ├── transformer_service.py  # Data transformation
│   └── supabase_service.py     # Supabase API calls
├── utils/
│   ├── __init__.py
│   ├── logging_utils.py        # Structured logging
│   └── status_mapping.py       # Status normalization
├── queries/
│   ├── README.md
│   ├── meli_tracking_events.sql
│   ├── fala_tracking_events.sql
│   ├── walm_tracking_events.sql
│   └── cenc_tracking_events.sql
└── docs/
    ├── plan_to_push_orders_to_supabase.md
    ├── endpoint_documentation.md
    └── shipment_tracking_deployment.md
```

## 🔧 Prerequisites

### 1. GCP Setup
- **Project ID**: `projectbamo`
- **Region**: `us-central1`
- **BigQuery Dataset**: Access to marketplace data tables
- **Service Account**: Cloud Function default service account with BigQuery access

### 2. Supabase Setup
- **Project URL**: `https://obrhnfnqrvkfbgcpjixl.supabase.co`
- **Edge Function**: `process-shipment-tracking` deployed and active
- **Service Role Key**: Required for authentication
- **Tables**: `orders` and `shipment_tracking` with proper RLS policies

### 3. Required Permissions
```bash
# BigQuery permissions (via Service Account)
- bigquery.jobs.create
- bigquery.datasets.get
- bigquery.tables.get
- bigquery.tables.getData

# Cloud Functions permissions
- cloudfunctions.functions.create
- cloudfunctions.functions.update
- cloudfunctions.operations.get
```

## 🚀 Deployment Instructions

### Step 1: Environment Variables

Set the following environment variables for the Cloud Function:

```bash
# Core Configuration
GCP_PROJECT="projectbamo"
SUPABASE_URL="https://obrhnfnqrvkfbgcpjixl.supabase.co"
SUPABASE_SERVICE_ROLE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9icmhuZm5xcnZrZmJnY3BqaXhsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjE2ODc4NDMsImV4cCI6MjA3NzI2Mzg0M30.lZTZlmKBdA1t1DqXmEvkE8OtBoGyB0I5QkM_smZ0S_A"

# Function Configuration
LOOKBACK_MINUTES="20"
MAX_RETRY_ATTEMPTS="3"
SUPABASE_TIMEOUT="30"
BATCH_SIZE="100"

# Logging
LOG_LEVEL="INFO"
```

### Step 2: Deploy Cloud Function

```bash
# Navigate to function directory
cd function_shipment_tracking

# Deploy the function
gcloud functions deploy sync-shipment-tracking \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=sync_shipment_tracking \
  --trigger-http \
  --no-allow-unauthenticated \
  --timeout=540s \
  --memory=1Gi \
  --max-instances=5 \
  --min-instances=0 \
  --concurrency=1 \
  --set-env-vars="GCP_PROJECT=projectbamo,SUPABASE_URL=https://obrhnfnqrvkfbgcpjixl.supabase.co,SUPABASE_SERVICE_ROLE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9icmhuZm5xcnZrZmJnY3BqaXhsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTY4Nzg0MywiZXhwIjoyMDc3MjYzODQzfQ.7_uAVNVMGPMzEKnJL36EWdko6oeOGKfXQv36jH-mMqs,LOOKBACK_MINUTES=20,MAX_RETRY_ATTEMPTS=3,SUPABASE_TIMEOUT=30,BATCH_SIZE=100,LOG_LEVEL=INFO"
```

### Step 3: Create Cloud Scheduler Job

```bash
# Create scheduler job for every 15 minutes
gcloud scheduler jobs create http sync-shipment-tracking-job \
  --location=us-central1 \
  --schedule="*/15 * * * *" \
  --uri="https://us-central1-projectbamo.cloudfunctions.net/sync-shipment-tracking" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"trigger":"scheduler"}' \
  --oidc-service-account-email="projectbamo@appspot.gserviceaccount.com" \
  --time-zone="America/Santiago" \
  --description="Sync shipment tracking events from BigQuery to Supabase every 15 minutes"
```

## 🔐 Security Configuration

### 1. Service Account Setup

```bash
# Create dedicated service account (optional, or use default)
gcloud iam service-accounts create shipment-tracking-sa \
  --display-name="Shipment Tracking Service Account"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding projectbamo \
  --member="serviceAccount:shipment-tracking-sa@projectbamo.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding projectbamo \
  --member="serviceAccount:shipment-tracking-sa@projectbamo.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

### 2. Supabase Security

- **Service Role Key**: Store in Google Secret Manager (recommended)
- **RLS Policies**: Ensure `shipment_tracking` table has proper company-based isolation
- **Edge Function**: Validate authentication headers

### 3. Network Security

```bash
# Optional: Restrict function access to specific networks
gcloud functions deploy sync-shipment-tracking \
  --ingress-settings=internal-only \
  --vpc-connector=projects/projectbamo/locations/us-central1/connectors/vpc-connector
```

## 📊 Monitoring & Alerting

### 1. Cloud Monitoring Metrics

Key metrics to monitor:

```yaml
# Function Execution Metrics
- cloud.google.com/function/execution/count
- cloud.google.com/function/execution/duration
- cloud.google.com/function/execution/memory_usage

# Custom Metrics (via structured logging)
- events_processed_total
- events_created_count
- events_updated_count
- events_skipped_count
- bigquery_query_duration
- supabase_api_duration
```

### 2. Log-based Alerts

```bash
# Create alert for function failures
gcloud alpha monitoring policies create \
  --policy-from-file=monitoring/function-error-alert.yaml

# Create alert for high error rates
gcloud alpha monitoring policies create \
  --policy-from-file=monitoring/high-error-rate-alert.yaml
```

### 3. Dashboard Setup

Create Cloud Monitoring dashboard with:
- Function execution count and duration
- Error rate trends
- Events processed per execution
- BigQuery and Supabase latency

## 🧪 Testing & Validation

### 1. Manual Testing

```bash
# Test function directly
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"test": true}' \
  https://us-central1-projectbamo.cloudfunctions.net/sync-shipment-tracking
```

### 2. Scheduler Testing

```bash
# Trigger scheduler job manually
gcloud scheduler jobs run sync-shipment-tracking-job \
  --location=us-central1
```

### 3. Validation Queries

```sql
-- Check recent tracking events in Supabase
SELECT 
  COUNT(*) as total_events,
  COUNT(DISTINCT order_id) as unique_orders,
  event_status,
  DATE(received_at) as date_received
FROM shipment_tracking 
WHERE received_at >= NOW() - INTERVAL '1 hour'
GROUP BY event_status, DATE(received_at)
ORDER BY date_received DESC, event_status;

-- Verify company isolation
SELECT 
  company_id,
  COUNT(*) as event_count
FROM shipment_tracking 
WHERE received_at >= NOW() - INTERVAL '1 hour'
GROUP BY company_id;
```

## 🔄 Status Mapping Configuration

The function normalizes marketplace-specific statuses to PlugUp standard statuses:

```python
# Status mapping in utils/status_mapping.py
PLUGUP_STATUS_MAPPING = {
    'pending': ['pending', 'pendiente', 'created', 'acknowledged'],
    'ready_to_ship': ['ready_to_ship', 'lista para despacho', 'handling'],
    'dispatched': ['shipped', 'enviado', 'en tránsito'],
    'in_transit': ['shipped', 'en tránsito', 'transit'],
    'delivered': ['delivered', 'entregada', 'entregado'],
    'cancelled': ['cancelled', 'canceled', 'cancelada', 'cancelado'],
    'returned': ['returned', 'devuelto'],
    'delivery_failed': ['not_delivered', 'failed_delivery', 'excepción']
}
```

## 📈 Performance Optimization

### 1. BigQuery Optimization

- **Clustering**: Tables clustered by `marketplace` and `company_id`
- **Partitioning**: Time-based partitioning on `processed_at`
- **Query Optimization**: Use parameterized queries with 20-minute lookback

### 2. Function Optimization

```yaml
# Recommended settings
Memory: 1Gi
Timeout: 540s (9 minutes)
Max Instances: 5
Min Instances: 0
Concurrency: 1  # Sequential processing to avoid conflicts
```

### 3. Batch Processing

- Process events in batches of 100
- Implement exponential backoff for retries
- Use connection pooling for Supabase calls

## 🚨 Troubleshooting

### Common Issues

1. **BigQuery Permission Errors**
   ```bash
   # Check service account permissions
   gcloud projects get-iam-policy projectbamo \
     --flatten="bindings[].members" \
     --filter="bindings.members:*@projectbamo.iam.gserviceaccount.com"
   ```

2. **Supabase Authentication Errors**
   ```bash
   # Verify service role key
   curl -H "Authorization: Bearer YOUR_SERVICE_ROLE_KEY" \
     https://obrhnfnqrvkfbgcpjixl.supabase.co/rest/v1/orders?select=id&limit=1
   ```

3. **Function Timeout Issues**
   ```bash
   # Increase timeout and memory
   gcloud functions deploy sync-shipment-tracking \
     --update-env-vars="BATCH_SIZE=50" \
     --timeout=600s \
     --memory=2Gi
   ```

### Debug Mode

Enable debug logging:

```bash
gcloud functions deploy sync-shipment-tracking \
  --update-env-vars="LOG_LEVEL=DEBUG"
```

## 📋 Maintenance Tasks

### Weekly Tasks
- [ ] Review function execution logs for errors
- [ ] Check BigQuery query performance
- [ ] Validate Supabase data consistency
- [ ] Monitor function memory usage

### Monthly Tasks
- [ ] Update status mapping if new marketplace statuses appear
- [ ] Review and optimize BigQuery queries
- [ ] Check function scaling metrics
- [ ] Update dependencies in requirements.txt

### Quarterly Tasks
- [ ] Review security configurations
- [ ] Update monitoring dashboards
- [ ] Performance testing and optimization
- [ ] Documentation updates

## 🔗 Related Resources

- [BigQuery Tracking Events Queries](queries/README.md)
- [Supabase Edge Function Documentation](docs/endpoint_documentation.md)
- [Project Requirements](docs/plan_to_push_orders_to_supabase.md)
- [Cloud Functions Gen 2 Documentation](https://cloud.google.com/functions/docs/2nd-gen)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)

## 📞 Support

For issues or questions:
- **Technical Issues**: Check Cloud Logging for detailed error messages
- **Data Issues**: Validate BigQuery source data and Supabase target tables
- **Performance Issues**: Review monitoring dashboards and adjust function configuration

---

**Last Updated**: 2026-01-27  
**Version**: 1.0  
**Maintainer**: PlugUp Engineering Team