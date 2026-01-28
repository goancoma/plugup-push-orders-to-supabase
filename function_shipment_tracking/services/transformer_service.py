import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime

from utils.logging_utils import setup_logger, log_structured
from utils.status_mapping import normalize_status
from company_mapping import get_company_uuid, get_all_company_mappings

logger = setup_logger(__name__)

class ShipmentTransformerService:
    """Transform BigQuery tracking results to Supabase Edge Function format"""
    
    @staticmethod
    def _validate_required_fields(row: pd.Series) -> bool:
        """Validate that required fields are present and valid"""
        required_fields = [
            'marketplace_order_id',
            'marketplace', 
            'company_id',
            'event_status',
            'event_timestamp'
        ]
        
        for field in required_fields:
            value = row.get(field)
            if pd.isna(value) or value is None or str(value).strip() == '':
                logger.warning(f"Missing required field '{field}' for order {row.get('marketplace_order_id', 'unknown')}")
                return False
        
        return True
    
    @staticmethod
    def _format_timestamp(timestamp_value) -> Optional[str]:
        """Format timestamp to ISO 8601 format"""
        if pd.isna(timestamp_value) or timestamp_value is None:
            return None
        
        try:
            if isinstance(timestamp_value, pd.Timestamp):
                ts = timestamp_value
            elif isinstance(timestamp_value, datetime):
                ts = pd.Timestamp(timestamp_value)
            else:
                ts = pd.to_datetime(timestamp_value, errors="coerce", utc=False)
            
            if pd.isna(ts):
                logger.warning(f"Failed to parse timestamp: {timestamp_value}")
                return None
            
            # Ensure timezone awareness (assume UTC if no timezone)
            if ts.tzinfo is None:
                ts = ts.tz_localize('UTC')
            
            return ts.isoformat()
            
        except Exception as e:
            logger.error(f"Error formatting timestamp {timestamp_value}: {e}")
            return None
    
    @staticmethod
    def _build_tracking_event(row: pd.Series) -> Dict[str, Any]:
        """Build tracking event payload for Supabase Edge Function"""
        
        # Normalize status using marketplace-specific mapping
        normalized_status = normalize_status(
            str(row['event_status']), 
            str(row['marketplace'])
        )
        
        # Format timestamp
        event_timestamp = ShipmentTransformerService._format_timestamp(row['event_timestamp'])
        if not event_timestamp:
            raise ValueError(f"Invalid event_timestamp for order {row['marketplace_order_id']}")
        
        # Convert company name to UUID
        company_name = str(row['company_id'])
        try:
            company_uuid = get_company_uuid(company_name)
            print(f"COMPANY MAPPING: '{company_name}' -> '{company_uuid}'")
        except ValueError as e:
            print(f"COMPANY MAPPING ERROR: {str(e)}")
            raise ValueError(f"Company mapping error for order {row['marketplace_order_id']}: {str(e)}")
        
        # Build the event payload
        event = {
            "marketplace_order_id": str(row['marketplace_order_id']),
            "marketplace": str(row['marketplace']).lower(),
            "company_id": company_uuid,  # Use UUID instead of name
            "event_status": normalized_status,
            "event_timestamp": event_timestamp
        }
        
        # Add optional fields if present
        optional_fields = {
            'event_location': 'event_location',
            'courier_name': 'courier_name', 
            'tracking_number': 'tracking_number',
            'notes': 'notes'
        }
        
        for event_field, row_field in optional_fields.items():
            value = row.get(row_field)
            if not pd.isna(value) and value is not None and str(value).strip():
                event[event_field] = str(value).strip()
        
        return event
    
    def transform_to_tracking_events(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Transform DataFrame to list of tracking event payloads.
        
        Args:
            df: DataFrame from BigQuery with tracking event data
            
        Returns:
            List of tracking event payload dictionaries
        """
        if df.empty:
            logger.info("No tracking events to transform")
            return []
        
        # Log available company mappings for visibility
        company_mappings = get_all_company_mappings()
        print(f"COMPANY MAPPINGS LOADED: {company_mappings}")
        log_structured(logger, "Company mappings loaded",
                      mappings=company_mappings,
                      total_companies=len(company_mappings))
        
        tracking_events = []
        skipped_count = 0
        
        for index, row in df.iterrows():
            try:
                # Validate required fields
                if not self._validate_required_fields(row):
                    skipped_count += 1
                    continue
                
                # Build tracking event
                event = self._build_tracking_event(row)
                tracking_events.append(event)
                
            except Exception as e:
                marketplace = row.get('marketplace', 'unknown')
                order_id = row.get('marketplace_order_id', 'unknown')
                logger.error(f"Event transformation failed: {marketplace} {order_id}: {str(e)}")
                skipped_count += 1
                continue
        
        log_structured(logger, "Tracking events transformation completed",
                      total_rows=len(df),
                      valid_events=len(tracking_events),
                      skipped_events=skipped_count)
        
        return tracking_events