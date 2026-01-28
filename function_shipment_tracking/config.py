import os

class Config:
    """Configuration for Shipment Tracking Cloud Function"""
    
    # BigQuery
    PROJECT_ID = os.getenv('GCP_PROJECT', 'projectbamo')
    
    # Supabase
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    # Timeouts and retries
    SUPABASE_TIMEOUT = int(os.getenv('SUPABASE_TIMEOUT', 30))
    MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', 3))
    
    # Query parameters
    LOOKBACK_MINUTES = int(os.getenv('LOOKBACK_MINUTES', 20))
    
    # Batch processing
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required_vars = [
            'SUPABASE_URL',
            'SUPABASE_SERVICE_ROLE_KEY'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True