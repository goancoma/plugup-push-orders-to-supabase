import logging
import json
from typing import Any, Dict

def setup_logger(name: str = __name__) -> logging.Logger:
    """Configure structured logging for Cloud Functions"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Cloud Logging expects JSON format
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    
    return logger

def log_structured(logger: logging.Logger, message: str, 
                   severity: str = 'INFO', **kwargs):
    """Log structured data for Cloud Logging"""
    log_entry = {
        'message': message,
        'severity': severity,
        **kwargs
    }
    logger.info(json.dumps(log_entry))