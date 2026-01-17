import functions_framework
from flask import Request
import json
import pandas as pd
from datetime import datetime

from services.bigquery_service import BigQueryService
from services.transformer_service import OrderTransformerService
from services.webhook_service import WebhookService
from utils.logging_utils import setup_logger, log_structured

logger = setup_logger(__name__)

@functions_framework.http
def process_orders(request: Request):
    """
    HTTP Cloud Function to process orders from BigQuery and send to webhook.
    
    Triggered by Cloud Scheduler every hour.
    """
    try:
        log_structured(logger, "Function started")
        
        # 1. Fetch orders from BigQuery
        bq_service = BigQueryService()
        df = bq_service.fetch_recent_orders()
        
        if df.empty:
            log_structured(logger, "No orders found")
            return {'status': 'success', 'message': 'No orders to process'}, 200
        
        # 2. Transform to webhook format
        transformer = OrderTransformerService()
        webhooks = transformer.transform_to_webhooks(df)
        
        # 3. Send to webhook
        webhook_service = WebhookService()
        result = webhook_service.send_batch(webhooks)
        
        # 4. Return summary
        response = {
            'status': 'completed',
            **result.to_dict()
        }
        
        log_structured(logger, "Function completed", **response)
        return response, 200
        
    except Exception as e:
        error_msg = f"Function execution failed: {str(e)}"
        log_structured(logger, error_msg, severity='ERROR')
        return {'status': 'error', 'message': error_msg}, 500