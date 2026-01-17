import os

class Config:
    """Configuration for Cloud Function"""
    
    # BigQuery
    PROJECT_ID = os.getenv('GCP_PROJECT', 'projectbamo')
    
    # Webhook
    WEBHOOK_URL = os.getenv('WEBHOOK_URL')
    WEBHOOK_TOKEN = os.getenv('WEBHOOK_TOKEN')
    
    # Timeouts and retries
    WEBHOOK_TIMEOUT = int(os.getenv('WEBHOOK_TIMEOUT', 30))
    MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', 3))
    
    # Query parameters
    LOOKBACK_MINUTES = int(os.getenv('LOOKBACK_MINUTES', 65))