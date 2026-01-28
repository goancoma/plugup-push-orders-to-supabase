import functions_framework
from flask import Request
import json
from datetime import datetime
from typing import Dict, Any

from services.bigquery_service import BigQueryService
from services.transformer_service import ShipmentTransformerService
from services.supabase_service import SupabaseService
from utils.logging_utils import setup_logger, log_structured

logger = setup_logger(__name__)

@functions_framework.http
def sync_shipment_tracking(request: Request):
    """
    HTTP Cloud Function to sync shipment tracking events from BigQuery to Supabase.
    
    Triggered by Cloud Scheduler every 15 minutes.
    """
    try:
        log_structured(logger, "Shipment tracking sync started")
        
        # 1. Fetch tracking events from BigQuery
        bq_service = BigQueryService()
        events_df = bq_service.fetch_recent_tracking_events()
        
        if events_df.empty:
            log_structured(logger, "No tracking events found")
            return {'status': 'success', 'message': 'No tracking events to process'}, 200
        
        log_structured(logger, "Fetched tracking events from BigQuery", 
                      total_events=len(events_df))
        
        # 2. Transform to Supabase format
        transformer = ShipmentTransformerService()
        tracking_events = transformer.transform_to_tracking_events(events_df)
        
        if not tracking_events:
            log_structured(logger, "No valid tracking events after transformation")
            return {'status': 'success', 'message': 'No valid tracking events to process'}, 200
        
        log_structured(logger, "Transformed tracking events", 
                      valid_events=len(tracking_events))
        
        # 3. Test connectivity to Supabase Edge Function first
        supabase_service = SupabaseService()
        
        # Run connectivity test
        connectivity_ok, connectivity_msg = supabase_service.test_connectivity()
        log_structured(logger, "Supabase connectivity test",
                      success=connectivity_ok,
                      details=connectivity_msg)
        
        if not connectivity_ok:
            error_msg = f"Supabase connectivity test failed: {connectivity_msg}"
            log_structured(logger, error_msg, severity='ERROR')
            return {'status': 'error', 'message': error_msg}, 500
        
        # 4. Send tracking events to Supabase Edge Function
        result = supabase_service.send_tracking_events(tracking_events)
        
        # 5. Return summary
        response = {
            'status': 'completed',
            'bigquery_events': len(events_df),
            'valid_events': len(tracking_events),
            'connectivity_test': connectivity_msg,
            **result.to_dict()
        }
        
        log_structured(logger, "Shipment tracking sync completed", **response)
        return response, 200
        
    except Exception as e:
        error_msg = f"Shipment tracking sync failed: {str(e)}"
        log_structured(logger, error_msg, severity='ERROR')
        return {'status': 'error', 'message': error_msg}, 500