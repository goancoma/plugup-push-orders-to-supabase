import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

CLT = ZoneInfo("America/Santiago")
logger = logging.getLogger(__name__)

def to_iso8601_clt(value):
    """
    Convert various date formats to ISO 8601 with Chile timezone.
    
    Args:
        value: String, pandas.Timestamp, datetime, or None
        
    Returns:
        ISO 8601 string with CLT offset or None
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        logger.debug(f"Input value is None or NaN: {value} (type: {type(value)})")
        return None

    # 🪲 DEBUG: Enhanced logging for NaT detection
    logger.debug(f"Converting date value: {value} (type: {type(value)})")
    
    # Check for pandas NaT specifically
    if hasattr(value, '__class__') and 'NaTType' in str(value.__class__):
        logger.warning(f"Detected pandas NaT value: {value} (type: {type(value)}) - returning None")
        return None

    # Normalize to pandas.Timestamp
    if isinstance(value, pd.Timestamp):
        ts = value
    elif isinstance(value, datetime):
        ts = pd.Timestamp(value)
    else:
        ts = pd.to_datetime(value, errors="coerce", utc=False)

    if pd.isna(ts):
        logger.warning(f"Failed to parse date value: {value} (type: {type(value)}) - returning None instead of raw string")
        return None  # Return None instead of str(value) to avoid invalid date formats

    # Convert to CLT timezone
    try:
        if ts.tzinfo is not None:
            ts = ts.tz_convert(CLT)
        else:
            ts = ts.tz_localize(CLT)

        result = ts.isoformat(timespec="seconds")
        logger.debug(f"Successfully converted to: {result}")
        return result
    except Exception as e:
        logger.error(f"Error converting timezone for value {value}: {e}")
        return None