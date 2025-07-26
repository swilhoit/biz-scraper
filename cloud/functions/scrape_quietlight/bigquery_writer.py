"""
BigQuery writer module for scrapers
"""

import json
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging

logger = logging.getLogger(__name__)


class BigQueryWriter:
    """
    BigQuery writer for business listings data
    """
    
    def __init__(self, project_id: str = 'biz-hunter-oauth'):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = 'business_data'
        self.listings_table_id = 'listings'
        self.runs_table_id = 'scraper_runs'
        
        # Table references
        self.listings_table = self.client.dataset(self.dataset_id).table(self.listings_table_id)
        self.runs_table = self.client.dataset(self.dataset_id).table(self.runs_table_id)
    
    def write_listings(self, listings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Write business listings to BigQuery
        
        Args:
            listings: List of transformed listing dictionaries
            
        Returns:
            Dictionary with write results
        """
        if not listings:
            return {'success': True, 'rows_written': 0, 'errors': []}
        
        try:
            # Prepare rows for BigQuery
            processed_rows = []
            for listing in listings:
                processed_row = self._prepare_listing_for_bigquery(listing)
                processed_rows.append(processed_row)
            
            # Insert rows
            errors = self.client.insert_rows_json(self.listings_table, processed_rows)
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return {
                    'success': False,
                    'rows_written': 0,
                    'errors': errors
                }
            else:
                logger.info(f"Successfully wrote {len(processed_rows)} listings to BigQuery")
                return {
                    'success': True,
                    'rows_written': len(processed_rows),
                    'errors': []
                }
                
        except Exception as e:
            logger.error(f"Error writing to BigQuery: {e}")
            return {
                'success': False,
                'rows_written': 0,
                'errors': [str(e)]
            }
    
    def write_scraper_run(self, run_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Write scraper run data to BigQuery
        
        Args:
            run_data: Scraper run data dictionary
            
        Returns:
            Dictionary with write results
        """
        try:
            # Prepare run data for BigQuery
            processed_run = self._prepare_run_for_bigquery(run_data)
            
            # Insert row
            errors = self.client.insert_rows_json(self.runs_table, [processed_run])
            
            if errors:
                logger.error(f"BigQuery scraper run insert errors: {errors}")
                return {
                    'success': False,
                    'errors': errors
                }
            else:
                logger.info(f"Successfully wrote scraper run to BigQuery")
                return {
                    'success': True,
                    'errors': []
                }
                
        except Exception as e:
            logger.error(f"Error writing scraper run to BigQuery: {e}")
            return {
                'success': False,
                'errors': [str(e)]
            }
    
    def _prepare_listing_for_bigquery(self, listing: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare listing data for BigQuery insertion
        
        Args:
            listing: Raw listing data
            
        Returns:
            Processed listing data
        """
        # Convert datetime objects to ISO strings
        processed = {}
        for key, value in listing.items():
            if isinstance(value, datetime):
                processed[key] = value.isoformat()
            elif isinstance(value, date):
                processed[key] = value.isoformat()
            elif value is None:
                processed[key] = None
            else:
                processed[key] = value
        
        # Ensure required fields have defaults
        processed.setdefault('ingestion_date', date.today().isoformat())
        processed.setdefault('scraped_at', datetime.utcnow().isoformat())
        processed.setdefault('is_active', True)
        processed.setdefault('asking_price_currency', 'USD')
        processed.setdefault('revenue_currency', 'USD')
        processed.setdefault('profit_currency', 'USD')
        processed.setdefault('revenue_period', 'annual')
        processed.setdefault('profit_period', 'annual')
        
        return processed
    
    def _prepare_run_for_bigquery(self, run_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare scraper run data for BigQuery insertion
        
        Args:
            run_data: Raw scraper run data
            
        Returns:
            Processed scraper run data
        """
        # Convert datetime objects to ISO strings
        processed = {}
        for key, value in run_data.items():
            if isinstance(value, datetime):
                processed[key] = value.isoformat()
            elif isinstance(value, date):
                processed[key] = value.isoformat()
            elif value is None:
                processed[key] = None
            else:
                processed[key] = value
        
        # Ensure required fields have defaults
        processed.setdefault('ingestion_date', date.today().isoformat())
        processed.setdefault('created_at', datetime.utcnow().isoformat())
        processed.setdefault('scraper_version', '1.0')
        
        return processed
    
    def deactivate_old_listings(self, source: str, active_listing_ids: List[str]) -> Dict[str, Any]:
        """
        Deactivate old listings that are no longer active
        
        Args:
            source: Source name (e.g., 'Empire Flippers')
            active_listing_ids: List of currently active listing IDs
            
        Returns:
            Dictionary with deactivation results
        """
        try:
            # Build query to find listings to deactivate
            if active_listing_ids:
                listing_ids_str = "', '".join(active_listing_ids)
                query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.{self.listings_table_id}`
                SET is_active = FALSE, last_updated = CURRENT_TIMESTAMP()
                WHERE source = '{source}' 
                  AND is_active = TRUE
                  AND listing_id NOT IN ('{listing_ids_str}')
                """
            else:
                # If no active listings, deactivate all for this source
                query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.{self.listings_table_id}`
                SET is_active = FALSE, last_updated = CURRENT_TIMESTAMP()
                WHERE source = '{source}' AND is_active = TRUE
                """
            
            # Execute query
            query_job = self.client.query(query)
            results = query_job.result()
            
            logger.info(f"Deactivated old listings for {source}")
            return {
                'success': True,
                'rows_affected': results.num_dml_affected_rows
            }
            
        except Exception as e:
            logger.error(f"Error deactivating old listings: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_listing_stats(self, source: Optional[str] = None) -> Dict[str, Any]:
        """
        Get listing statistics from BigQuery
        
        Args:
            source: Optional source filter
            
        Returns:
            Dictionary with statistics
        """
        try:
            where_clause = f"WHERE source = '{source}'" if source else ""
            
            query = f"""
            SELECT 
                source,
                COUNT(*) as total_listings,
                COUNT(CASE WHEN is_active THEN 1 END) as active_listings,
                AVG(asking_price_numeric) as avg_price,
                APPROX_QUANTILES(asking_price_numeric, 2)[OFFSET(1)] as median_price,
                AVG(revenue_numeric) as avg_revenue,
                AVG(profit_numeric) as avg_profit,
                AVG(data_completeness_score) as avg_completeness,
                MAX(last_updated) as last_update
            FROM `{self.project_id}.{self.dataset_id}.{self.listings_table_id}`
            {where_clause}
            GROUP BY source
            ORDER BY source
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            stats = {}
            for row in results:
                stats[row.source] = {
                    'total_listings': row.total_listings,
                    'active_listings': row.active_listings,
                    'avg_price': row.avg_price,
                    'median_price': row.median_price,
                    'avg_revenue': row.avg_revenue,
                    'avg_profit': row.avg_profit,
                    'avg_completeness': row.avg_completeness,
                    'last_update': row.last_update.isoformat() if row.last_update else None
                }
            
            return {
                'success': True,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Error getting listing stats: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def query_listings(self, 
                      source: Optional[str] = None,
                      active_only: bool = True,
                      limit: int = 100,
                      min_price: Optional[float] = None,
                      max_price: Optional[float] = None) -> Dict[str, Any]:
        """
        Query listings from BigQuery
        
        Args:
            source: Optional source filter
            active_only: Only return active listings
            limit: Maximum number of results
            min_price: Minimum price filter
            max_price: Maximum price filter
            
        Returns:
            Dictionary with query results
        """
        try:
            # Build WHERE clause
            where_conditions = []
            
            if active_only:
                where_conditions.append("is_active = TRUE")
            
            if source:
                where_conditions.append(f"source = '{source}'")
            
            if min_price is not None:
                where_conditions.append(f"asking_price_numeric >= {min_price}")
            
            if max_price is not None:
                where_conditions.append(f"asking_price_numeric <= {max_price}")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT 
                listing_id,
                source,
                title,
                asking_price_raw,
                asking_price_numeric,
                revenue_raw,
                revenue_numeric,
                profit_raw,
                profit_numeric,
                location_raw,
                country,
                state,
                niches,
                price_to_profit_multiple,
                profit_margin_percent,
                source_url,
                last_updated
            FROM `{self.project_id}.{self.dataset_id}.{self.listings_table_id}`
            {where_clause}
            ORDER BY last_updated DESC
            LIMIT {limit}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            listings = []
            for row in results:
                listing = {
                    'listing_id': row.listing_id,
                    'source': row.source,
                    'title': row.title,
                    'asking_price_raw': row.asking_price_raw,
                    'asking_price_numeric': row.asking_price_numeric,
                    'revenue_raw': row.revenue_raw,
                    'revenue_numeric': row.revenue_numeric,
                    'profit_raw': row.profit_raw,
                    'profit_numeric': row.profit_numeric,
                    'location_raw': row.location_raw,
                    'country': row.country,
                    'state': row.state,
                    'niches': row.niches,
                    'price_to_profit_multiple': row.price_to_profit_multiple,
                    'profit_margin_percent': row.profit_margin_percent,
                    'source_url': row.source_url,
                    'last_updated': row.last_updated.isoformat() if row.last_updated else None
                }
                listings.append(listing)
            
            return {
                'success': True,
                'listings': listings,
                'count': len(listings)
            }
            
        except Exception as e:
            logger.error(f"Error querying listings: {e}")
            return {
                'success': False,
                'error': str(e)
            }