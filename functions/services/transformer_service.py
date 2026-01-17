import pandas as pd
from typing import List, Dict, Any

from utils.datetime_utils import to_iso8601_clt
from utils.logging_utils import setup_logger, log_structured

logger = setup_logger(__name__)

class OrderTransformerService:
    """Transform BigQuery results to webhook format"""
    
    @staticmethod
    def _build_sku_dict(group: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Build SKU dictionary from order items"""
        sku_dict = {}
        
        for _, row in group.iterrows():
            seller_sku = str(row['seller_sku'])
            sku_dict[seller_sku] = {
                "quantity": int(row['quantity']),
                "name": str(row['sku_name']),
                "market_place_match_id": str(row['market_place_match_id']),
                "order_item_id": str(row.get('order_item_id', ''))
            }
        
        return sku_dict
    
    @staticmethod
    def _build_webhook_payload(order_id: str, first_row: pd.Series,
                               sku_dict: Dict) -> Dict[str, Any]:
        """Build complete webhook payload for an order"""
        # Log raw date values for debugging
        order_created_raw = first_row.get('order_created_at')
        shipping_promise_raw = first_row.get('shipping_promise_date')
        
        logger.debug(f"Order {order_id} - Raw dates: order_created_at={order_created_raw} (type: {type(order_created_raw)}), shipping_promise_date={shipping_promise_raw} (type: {type(shipping_promise_raw)})")
        
        # Convert dates with error handling
        order_created_iso = to_iso8601_clt(order_created_raw)
        shipping_promise_iso = to_iso8601_clt(shipping_promise_raw)
        
        logger.debug(f"Order {order_id} - Converted dates: order_created_at={order_created_iso}, shipping_promise_date={shipping_promise_iso}")
        
        # Validate required date fields
        if order_created_iso is None:
            logger.error(f"Order {order_id} - Missing or invalid order_created_at: {order_created_raw}")
            raise ValueError(f"Missing required date field: order_created_at for order {order_id}")
        
        # 🪲 DEBUG: Add detailed logging for shipping_promise_date handling
        if shipping_promise_iso is None:
            logger.warning(f"Order {order_id} - shipping_promise_date is None/NaT: {shipping_promise_raw} (type: {type(shipping_promise_raw)})")
            # 🔧 FIX: shipping_promise_date should be optional, not required
            # Some orders may legitimately not have a shipping promise date
            logger.info(f"Order {order_id} - Proceeding with null shipping_promise_date")
        
        return {
            "order": str(order_id),
            "shipping_id": str(first_row['shipping_id']),
            "status": str(first_row['status']),
            "shipping_status": str(first_row['shipping_status']),
            "logistic_type": str(first_row['logistic_type']),
            "market_place": str(first_row['marketplace']),
            "order_created_at": order_created_iso,
            "shipping_promise_date": shipping_promise_iso,  # Can be None - webhook handler will handle it
            "sku": sku_dict
        }
    
    def transform_to_webhooks(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Transform DataFrame to list of webhook payloads.
        
        Args:
            df: DataFrame from BigQuery with order data
            
        Returns:
            List of webhook payload dictionaries
        """
        if df.empty:
            logger.info("No orders to transform")
            return []
        
        webhooks = []
        
        for order_id, group in df.groupby('order_id'):
            try:
                first_row = group.iloc[0]
                sku_dict = self._build_sku_dict(group)
                webhook = self._build_webhook_payload(order_id, first_row, sku_dict)
                webhooks.append(webhook)
                
            except Exception as e:
                # Log detailed error information for debugging
                first_row = group.iloc[0] if not group.empty else None
                if first_row is not None:
                    marketplace=first_row.get('marketplace', 'unknown')
                    logger.error(f"Order transformation failed: {marketplace} {order_id}: {str(e)}")
                else:
                    logger.error(f"Error transforming order {order_id}: {str(e)}")
                continue
        
        logger.info(f"Transformed {len(webhooks)} orders")
        return webhooks