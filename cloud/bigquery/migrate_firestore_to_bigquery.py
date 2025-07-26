#!/usr/bin/env python3
"""
Migration script to move data from Firestore to BigQuery
"""

import os
import sys
import json
from datetime import datetime, date
from typing import Dict, List, Any
from google.cloud import firestore
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging

# Add the data_transforms module to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_transforms import transform_firestore_to_bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FirestoreToBigQueryMigrator:
    def __init__(self, project_id: str = 'biz-hunter-oauth'):
        self.project_id = project_id
        self.firestore_client = firestore.Client(project=project_id)
        self.bigquery_client = bigquery.Client(project=project_id)
        
        # BigQuery configuration
        self.dataset_id = 'business_data'
        self.listings_table_id = 'listings'
        self.runs_table_id = 'scraper_runs'
        
        # Get table references
        self.listings_table = self.bigquery_client.dataset(self.dataset_id).table(self.listings_table_id)
        self.runs_table = self.bigquery_client.dataset(self.dataset_id).table(self.runs_table_id)
        
    def migrate_business_listings(self, batch_size: int = 100) -> Dict[str, int]:
        """
        Migrate business listings from Firestore to BigQuery
        
        Args:
            batch_size: Number of documents to process in each batch
            
        Returns:
            Dictionary with migration statistics
        """
        logger.info("Starting migration of business listings from Firestore to BigQuery")
        
        stats = {
            'total_processed': 0,
            'successfully_migrated': 0,
            'errors': 0,
            'skipped': 0
        }
        
        # Get all business listings from Firestore
        collection_ref = self.firestore_client.collection('business_listings')
        
        # Process documents in batches
        batch = []
        
        try:
            for doc in collection_ref.stream():
                try:
                    # Transform Firestore document to BigQuery format
                    firestore_data = doc.to_dict()
                    firestore_data['id'] = doc.id  # Add document ID
                    
                    transformed_data = transform_firestore_to_bigquery(firestore_data)
                    
                    # Validate required fields
                    if not transformed_data.get('listing_id') or not transformed_data.get('source'):
                        logger.warning(f"Skipping document {doc.id} - missing required fields")
                        stats['skipped'] += 1
                        continue
                    
                    batch.append(transformed_data)
                    stats['total_processed'] += 1
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        success_count = self._insert_batch_to_bigquery(batch, self.listings_table)
                        stats['successfully_migrated'] += success_count
                        stats['errors'] += len(batch) - success_count
                        batch = []
                        
                        logger.info(f"Processed {stats['total_processed']} documents so far...")
                        
                except Exception as e:
                    logger.error(f"Error processing document {doc.id}: {e}")
                    stats['errors'] += 1
                    
            # Process remaining batch
            if batch:
                success_count = self._insert_batch_to_bigquery(batch, self.listings_table)
                stats['successfully_migrated'] += success_count
                stats['errors'] += len(batch) - success_count
                
        except Exception as e:
            logger.error(f"Error during migration: {e}")
            stats['errors'] += 1
            
        logger.info(f"Migration completed. Stats: {stats}")
        return stats
    
    def migrate_scraper_runs(self, batch_size: int = 100) -> Dict[str, int]:
        """
        Migrate scraper runs from Firestore to BigQuery
        
        Args:
            batch_size: Number of documents to process in each batch
            
        Returns:
            Dictionary with migration statistics
        """
        logger.info("Starting migration of scraper runs from Firestore to BigQuery")
        
        stats = {
            'total_processed': 0,
            'successfully_migrated': 0,
            'errors': 0,
            'skipped': 0
        }
        
        # Get all scraper runs from Firestore
        collection_ref = self.firestore_client.collection('scraper_runs')
        
        # Process documents in batches
        batch = []
        
        try:
            for doc in collection_ref.stream():
                try:
                    firestore_data = doc.to_dict()
                    
                    # Transform to BigQuery format
                    transformed_data = self._transform_scraper_run(firestore_data)
                    
                    # Validate required fields
                    if not transformed_data.get('run_id') or not transformed_data.get('source'):
                        logger.warning(f"Skipping scraper run {doc.id} - missing required fields")
                        stats['skipped'] += 1
                        continue
                    
                    batch.append(transformed_data)
                    stats['total_processed'] += 1
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        success_count = self._insert_batch_to_bigquery(batch, self.runs_table)
                        stats['successfully_migrated'] += success_count
                        stats['errors'] += len(batch) - success_count
                        batch = []
                        
                        logger.info(f"Processed {stats['total_processed']} scraper runs so far...")
                        
                except Exception as e:
                    logger.error(f"Error processing scraper run {doc.id}: {e}")
                    stats['errors'] += 1
                    
            # Process remaining batch
            if batch:
                success_count = self._insert_batch_to_bigquery(batch, self.runs_table)
                stats['successfully_migrated'] += success_count
                stats['errors'] += len(batch) - success_count
                
        except Exception as e:
            logger.error(f"Error during scraper runs migration: {e}")
            stats['errors'] += 1
            
        logger.info(f"Scraper runs migration completed. Stats: {stats}")
        return stats
    
    def _transform_scraper_run(self, firestore_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Firestore scraper run document to BigQuery format"""
        return {
            'run_id': firestore_data.get('run_id'),
            'source': firestore_data.get('source'),
            'start_time': firestore_data.get('start_time'),
            'end_time': firestore_data.get('end_time'),
            'status': firestore_data.get('status'),
            'total_listings_found': firestore_data.get('total_listings'),
            'new_listings': firestore_data.get('new_listings'),
            'updated_listings': firestore_data.get('updated_listings'),
            'deactivated_listings': firestore_data.get('deactivated_listings', 0),
            'error_message': firestore_data.get('error'),
            'error_count': firestore_data.get('error_count', 0),
            'pages_scraped': firestore_data.get('pages_scraped'),
            'requests_made': firestore_data.get('requests_made'),
            'avg_response_time_ms': firestore_data.get('avg_response_time_ms'),
            'trigger_type': firestore_data.get('trigger', {}).get('trigger', 'unknown'),
            'scraper_version': firestore_data.get('scraper_version', '1.0'),
            'created_at': firestore_data.get('start_time', datetime.utcnow()),
            'ingestion_date': date.today()
        }
    
    def _insert_batch_to_bigquery(self, batch: List[Dict[str, Any]], table_ref) -> int:
        """
        Insert a batch of documents to BigQuery
        
        Args:
            batch: List of transformed documents
            table_ref: BigQuery table reference
            
        Returns:
            Number of successfully inserted rows
        """
        try:
            # Convert datetime objects to strings for BigQuery
            processed_batch = []
            for row in batch:
                processed_row = self._prepare_row_for_bigquery(row)
                processed_batch.append(processed_row)
            
            # Insert rows
            errors = self.bigquery_client.insert_rows_json(table_ref, processed_batch)
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return 0
            else:
                logger.debug(f"Successfully inserted {len(processed_batch)} rows")
                return len(processed_batch)
                
        except Exception as e:
            logger.error(f"Error inserting batch to BigQuery: {e}")
            return 0
    
    def _prepare_row_for_bigquery(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a row for BigQuery insertion by handling data types
        
        Args:
            row: Row data dictionary
            
        Returns:
            Processed row for BigQuery
        """
        processed_row = {}
        
        for key, value in row.items():
            if value is None:
                processed_row[key] = None
            elif isinstance(value, datetime):
                processed_row[key] = value.isoformat()
            elif isinstance(value, date):
                processed_row[key] = value.isoformat()
            elif isinstance(value, (list, dict)):
                # Handle arrays and JSON
                processed_row[key] = value
            else:
                processed_row[key] = value
                
        return processed_row
    
    def verify_migration(self) -> Dict[str, Any]:
        """
        Verify the migration by comparing counts and sampling data
        
        Returns:
            Dictionary with verification results
        """
        logger.info("Verifying migration...")
        
        # Get Firestore counts
        firestore_listings = len(list(self.firestore_client.collection('business_listings').stream()))
        firestore_runs = len(list(self.firestore_client.collection('scraper_runs').stream()))
        
        # Get BigQuery counts
        bq_listings_query = f"""
        SELECT COUNT(*) as count 
        FROM `{self.project_id}.{self.dataset_id}.{self.listings_table_id}`
        """
        
        bq_runs_query = f"""
        SELECT COUNT(*) as count 
        FROM `{self.project_id}.{self.dataset_id}.{self.runs_table_id}`
        """
        
        try:
            bq_listings_count = list(self.bigquery_client.query(bq_listings_query))[0].count
            bq_runs_count = list(self.bigquery_client.query(bq_runs_query))[0].count
            
            verification_results = {
                'firestore_listings': firestore_listings,
                'bigquery_listings': bq_listings_count,
                'firestore_runs': firestore_runs,
                'bigquery_runs': bq_runs_count,
                'listings_match': firestore_listings == bq_listings_count,
                'runs_match': firestore_runs == bq_runs_count
            }
            
            logger.info(f"Verification results: {verification_results}")
            return verification_results
            
        except Exception as e:
            logger.error(f"Error during verification: {e}")
            return {'error': str(e)}
    
    def run_full_migration(self):
        """Run complete migration process"""
        logger.info("Starting full migration process")
        
        # Migrate business listings
        listings_stats = self.migrate_business_listings()
        
        # Migrate scraper runs
        runs_stats = self.migrate_scraper_runs()
        
        # Verify migration
        verification = self.verify_migration()
        
        # Print summary
        print("\n" + "="*60)
        print("MIGRATION SUMMARY")
        print("="*60)
        print(f"Business Listings:")
        print(f"  - Total processed: {listings_stats['total_processed']}")
        print(f"  - Successfully migrated: {listings_stats['successfully_migrated']}")
        print(f"  - Errors: {listings_stats['errors']}")
        print(f"  - Skipped: {listings_stats['skipped']}")
        
        print(f"\nScraper Runs:")
        print(f"  - Total processed: {runs_stats['total_processed']}")
        print(f"  - Successfully migrated: {runs_stats['successfully_migrated']}")
        print(f"  - Errors: {runs_stats['errors']}")
        print(f"  - Skipped: {runs_stats['skipped']}")
        
        print(f"\nVerification:")
        if 'error' not in verification:
            print(f"  - Firestore listings: {verification['firestore_listings']}")
            print(f"  - BigQuery listings: {verification['bigquery_listings']}")
            print(f"  - Listings match: {verification['listings_match']}")
            print(f"  - Firestore runs: {verification['firestore_runs']}")
            print(f"  - BigQuery runs: {verification['bigquery_runs']}")
            print(f"  - Runs match: {verification['runs_match']}")
        else:
            print(f"  - Verification error: {verification['error']}")
        
        print("="*60)


def main():
    """Main migration function"""
    migrator = FirestoreToBigQueryMigrator()
    migrator.run_full_migration()


if __name__ == "__main__":
    main()