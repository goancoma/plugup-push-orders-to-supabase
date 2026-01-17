from google.cloud import bigquery
from typing import Optional
import pandas as pd
from pathlib import Path

from utils.logging_utils import setup_logger, log_structured
from config import Config

logger = setup_logger(__name__)

class BigQueryService:
    """Handle all BigQuery operations"""
    
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or Config.PROJECT_ID
        self.client = bigquery.Client(project=self.project_id)
        
    def _load_query(self, query_name: str) -> str:
        """Load SQL query from file"""
        query_path = Path(__file__).parent.parent / 'queries' / f'{query_name}.sql'
        
        try:
            with open(query_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"Query file not found: {query_path}")
            raise
    
    def _load_marketplace_query(self, marketplace: str) -> str:
        """Load marketplace-specific query from marketplaces directory"""
        query_path = Path(__file__).parent.parent / 'queries' / 'marketplaces' / f'{marketplace}_orders.sql'
        
        try:
            with open(query_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"Marketplace query file not found: {query_path}")
            raise
    
    def _build_unified_query(self, marketplaces: list = None) -> str:
        """Build unified query by combining individual marketplace queries"""
        if marketplaces is None:
            marketplaces = ['walm', 'cenc', 'fala', 'meli']
        
        queries = []
        for marketplace in marketplaces:
            try:
                marketplace_query = self._load_marketplace_query(marketplace)
                # Remove trailing semicolon if present to avoid UNION ALL syntax errors
                marketplace_query = marketplace_query.rstrip().rstrip(';')
                # Wrap each query in parentheses for proper UNION ALL syntax
                queries.append(f"(\n{marketplace_query}\n)")
            except FileNotFoundError:
                logger.warning(f"Skipping marketplace {marketplace} - query file not found")
                continue
        
        if not queries:
            raise ValueError("No valid marketplace queries found")
        
        unified_query = "\n\nUNION ALL\n\n".join(queries)
        
        log_structured(logger, "Built unified query",
                      marketplaces=marketplaces,
                      total_queries=len(queries))
        
        return unified_query
    
    def fetch_recent_orders(self, lookback_minutes: int = None, marketplaces: list = None) -> pd.DataFrame:
        """
        Fetch orders from multiple marketplaces created recently.
        
        Args:
            lookback_minutes: How far back to look for orders
            marketplaces: List of marketplaces to include (default: all available)
            
        Returns:
            DataFrame with order data
        """
        lookback = lookback_minutes or Config.LOOKBACK_MINUTES
        
        try:
            # Build unified query dynamically from individual marketplace files
            query = self._build_unified_query(marketplaces)
            
            # Use parameterized query for security
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "lookback_minutes", "INT64", lookback
                    )
                ]
            )
            
            log_structured(logger, "Executing BigQuery",
                         lookback_minutes=lookback,
                         marketplaces=marketplaces or ['walm', 'cenc', 'fala', 'meli'])
            
            query_job = self.client.query(query, job_config=job_config)
            df = query_job.to_dataframe()
            
            log_structured(logger, "Query completed",
                         rows_returned=len(df),
                         bytes_processed=query_job.total_bytes_processed)
            
            return df
            
        except Exception as e:
            log_structured(logger, "BigQuery error",
                         severity='ERROR', error=str(e))
            raise