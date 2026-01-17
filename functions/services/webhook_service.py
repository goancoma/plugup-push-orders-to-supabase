import requests
from typing import Dict, Any, Tuple, List
from dataclasses import dataclass
import time

from config import Config
from utils.logging_utils import setup_logger, log_structured

logger = setup_logger(__name__)

@dataclass
class WebhookResult:
    """Result of webhook sending operation"""
    total: int
    successful: int
    failed: int
    errors: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_orders': self.total,
            'successful': self.successful,
            'failed': self.failed,
            'errors': self.errors[:10]  # Limit errors returned
        }

class WebhookService:
    """Handle webhook HTTP calls with retry logic"""
    
    def __init__(self):
        self.url = Config.WEBHOOK_URL
        self.headers = {
            'Authorization': f'Bearer {Config.WEBHOOK_TOKEN}',
            'Content-Type': 'application/json'
        }
        self.timeout = Config.WEBHOOK_TIMEOUT
        self.max_retries = Config.MAX_RETRY_ATTEMPTS
    
    def _send_single_order(self, order: Dict[str, Any],
                          retry_count: int = 0) -> Tuple[bool, str]:
        """
        Send single order to webhook with retry logic.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            response = requests.post(
                url=self.url,
                headers=self.headers,
                json=order,
                timeout=self.timeout
            )
            
            order_id = order.get('order', 'unknown')
            marketplace = order.get('market_place', 'unknown')
            
            logger.info(f"Webhook response: {marketplace} - {order_id}")
            
            if response.status_code in [200, 201]:
                logger.info(f"Webhook response: {marketplace} - {order_id}. Status code {response.status_code}")
                return True, f"Success: {response.status_code}"
            else:
                logger.error(f"Webhook response: {marketplace} - {order_id}. Status code {response.status_code}. {response.text[:100]}")
                error_msg = f"Failed: {response.status_code} - {response.text[:100]}"
                
                # Retry on server errors
                if response.status_code >= 500 and retry_count < self.max_retries:
                    time.sleep(2 ** retry_count)  # Exponential backoff
                    return self._send_single_order(order, retry_count + 1)
                
                return False, error_msg
                
        except requests.exceptions.Timeout:
            error_msg = f"Timeout after {self.timeout}s"
            
            if retry_count < self.max_retries:
                time.sleep(2 ** retry_count)
                return self._send_single_order(order, retry_count + 1)
            
            return False, error_msg
            
        except Exception as e:
            return False, f"Exception: {str(e)}"
    
    def send_batch(self, orders: List[Dict[str, Any]]) -> WebhookResult:
        """
        Send batch of orders to webhook.
        
        Args:
            orders: List of order payload dictionaries
            
        Returns:
            WebhookResult with summary statistics
        """
        success_count = 0
        error_count = 0
        errors = []
        
        logger.info(f"Starting webhook batch total_orders={len(orders)}")
        
        for order in orders:
            order_id = order.get('order', 'unknown')
            marketplace = order.get('market_place', 'unknown')
            
            # Log the complete order payload for debugging
            logger.info(f"""
                "message": "Processing order with payload",
                "order_id": {order_id},
                "marketplace": {marketplace},
                "order_created_at": {order.get('order_created_at')},
                "shipping_promise_date": {order.get('shipping_promise_date')},
                "payload_keys": {list(order.keys())}""")
            
            success, message = self._send_single_order(order)
            
            if success:
                success_count += 1
            else:
                error_count += 1
                errors.append(f"Order {order_id} ({marketplace}): {message}")
        
        result = WebhookResult(
            total=len(orders),
            successful=success_count,
            failed=error_count,
            errors=errors
        )
        
        logger.info(f"Webhook batch completed {result.to_dict()}")
        
        return result