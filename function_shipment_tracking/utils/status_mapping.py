"""
Status normalization utilities for shipment tracking events.

Maps marketplace-specific statuses to PlugUp standard statuses according to:
function_shipment_tracking/docs/plan_to_push_orders_to_supabase.md
"""

from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

# PlugUp standard statuses mapping
PLUGUP_STATUS_MAPPING: Dict[str, List[str]] = {
    'pending': [
        'pending', 'pendiente', 'created', 'acknowledged'
    ],
    'ready_to_ship': [
        'ready_to_ship', 'lista para despacho', 'handling', 'ready_to_ship'
    ],
    'dispatched': [
        'shipped', 'enviado', 'en tránsito', 'shipped'
    ],
    'in_transit': [
        'shipped', 'en tránsito', 'transit', 'in_transit'
    ],
    'out_for_delivery': [
        'out_for_delivery', 'en reparto'
    ],
    'delivered': [
        'delivered', 'entregada', 'entregado', 'delivered'
    ],
    'delivery_failed': [
        'not_delivered', 'failed_delivery', 'excepción', 'delivery_failed'
    ],
    'cancelled': [
        'cancelled', 'canceled', 'cancelada', 'cancelado', 'cancelled'
    ],
    'returned': [
        'returned', 'devuelto', 'returned'
    ]
}

# Marketplace-specific status mappings
MARKETPLACE_STATUS_MAPPING: Dict[str, Dict[str, str]] = {
    'meli': {
        'pending': 'pending',
        'handling': 'ready_to_ship',
        'ready_to_ship': 'ready_to_ship',
        'shipped': 'dispatched',  # Could be dispatched or in_transit based on context
        'delivered': 'delivered',
        'not_delivered': 'delivery_failed',
        'cancelled': 'cancelled'
    },
    'fala': {
        'pending': 'pending',
        'ready_to_ship': 'ready_to_ship',
        'shipped': 'dispatched',  # Could be dispatched or in_transit based on context
        'delivered': 'delivered',
        'canceled': 'cancelled',
        'returned': 'returned'
    },
    'walm': {
        'created': 'pending',
        'acknowledged': 'ready_to_ship',
        'shipped': 'dispatched',  # Could be dispatched or in_transit based on context
        'delivered': 'delivered',
        'cancelled': 'cancelled'
    },
    'cenc': {
        'pendiente': 'pending',
        'lista para despacho': 'ready_to_ship',
        'enviado': 'dispatched',
        'en tránsito': 'in_transit',
        'entregada': 'delivered',
        'entregado': 'delivered',
        'cancelada': 'cancelled',
        'cancelado': 'cancelled',
        'devuelto': 'returned'
    }
}

def normalize_status(raw_status: str, marketplace: str) -> str:
    """
    Normalize marketplace-specific status to PlugUp standard status.
    
    Args:
        raw_status: Original status from marketplace
        marketplace: Marketplace identifier (meli, fala, walm, cenc)
        
    Returns:
        Normalized PlugUp status
    """
    if not raw_status or not marketplace:
        logger.warning(f"Missing status or marketplace: status='{raw_status}', marketplace='{marketplace}'")
        return 'pending'  # Default fallback
    
    # Clean and normalize input
    clean_status = str(raw_status).lower().strip()
    clean_marketplace = str(marketplace).lower().strip()
    
    # Try marketplace-specific mapping first
    marketplace_mapping = MARKETPLACE_STATUS_MAPPING.get(clean_marketplace, {})
    if clean_status in marketplace_mapping:
        normalized = marketplace_mapping[clean_status]
        logger.debug(f"Mapped {clean_marketplace} status '{raw_status}' -> '{normalized}'")
        return normalized
    
    # Try general PlugUp mapping
    for plugup_status, raw_statuses in PLUGUP_STATUS_MAPPING.items():
        if clean_status in [s.lower() for s in raw_statuses]:
            logger.debug(f"General mapping {clean_marketplace} status '{raw_status}' -> '{plugup_status}'")
            return plugup_status
    
    # Log unmapped status for future reference
    logger.warning(f"Unmapped status: {clean_marketplace} '{raw_status}' - using 'pending' as fallback")
    return 'pending'  # Default fallback

def get_supported_statuses() -> List[str]:
    """Get list of all supported PlugUp statuses"""
    return list(PLUGUP_STATUS_MAPPING.keys())

def get_marketplace_statuses(marketplace: str) -> List[str]:
    """Get list of supported statuses for a specific marketplace"""
    clean_marketplace = str(marketplace).lower().strip()
    marketplace_mapping = MARKETPLACE_STATUS_MAPPING.get(clean_marketplace, {})
    return list(marketplace_mapping.keys())

def validate_status_mapping():
    """Validate that all marketplace mappings point to valid PlugUp statuses"""
    valid_plugup_statuses = set(PLUGUP_STATUS_MAPPING.keys())
    
    for marketplace, mapping in MARKETPLACE_STATUS_MAPPING.items():
        for raw_status, plugup_status in mapping.items():
            if plugup_status not in valid_plugup_statuses:
                raise ValueError(f"Invalid PlugUp status '{plugup_status}' in {marketplace} mapping for '{raw_status}'")
    
    logger.info("Status mapping validation passed")
    return True

# Validate mappings on import
validate_status_mapping()