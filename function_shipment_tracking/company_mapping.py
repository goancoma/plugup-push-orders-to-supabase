"""
COMPANY MAPPING CONFIGURATION
============================

This file maps BigQuery company names to Supabase company UUIDs.
Update this mapping whenever new companies are added or company IDs change.

IMPORTANT: 
- BigQuery company names are on the LEFT
- Supabase company UUIDs are on the RIGHT
- Keep this mapping up to date for proper data synchronization
"""

# Company name (from BigQuery) -> Company UUID (for Supabase)
COMPANY_NAME_TO_UUID = {
    # BAMO Company
    "bamo_company": "c12585ee-c8f4-4103-b7f0-37bd62401a65",
    
    # Add more companies here as needed:
    # "company_example": "12345678-1234-1234-1234-123456789abc",
    # "another_company": "87654321-4321-4321-4321-cba987654321",
}

def get_company_uuid(company_name: str) -> str:
    """
    Get the Supabase UUID for a given BigQuery company name.
    
    Args:
        company_name: Company name from BigQuery (e.g., "company_bamo")
        
    Returns:
        Company UUID for Supabase (e.g., "c12585ee-c8f4-4103-b7f0-37bd62401a65")
        
    Raises:
        ValueError: If company name is not found in mapping
    """
    if company_name not in COMPANY_NAME_TO_UUID:
        available_companies = list(COMPANY_NAME_TO_UUID.keys())
        raise ValueError(
            f"Company '{company_name}' not found in mapping. "
            f"Available companies: {available_companies}. "
            f"Please update COMPANY_NAME_TO_UUID in company_mapping.py"
        )
    
    return COMPANY_NAME_TO_UUID[company_name]

def get_all_company_mappings() -> dict:
    """Get all company mappings for debugging/logging purposes."""
    return COMPANY_NAME_TO_UUID.copy()