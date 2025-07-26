#!/usr/bin/env python3
"""
Script to update all scrapers to use BigQuery instead of Firestore
"""

import os
import re

# List of scraper directories
scrapers = [
    'scrape_bizquest',
    'scrape_quietlight', 
    'scrape_bizbuysell'
]

def update_requirements(scraper_dir):
    """Update requirements.txt to use BigQuery"""
    req_file = f"/Users/samwilhoit/Documents/biz-scraper-2/cloud/functions/{scraper_dir}/requirements.txt"
    
    if os.path.exists(req_file):
        with open(req_file, 'r') as f:
            content = f.read()
        
        # Replace Firestore with BigQuery
        content = content.replace('google-cloud-firestore==2.11.*', 'google-cloud-bigquery==3.34.*')
        content = content.replace('google-cloud-tasks==2.13.*', '')
        
        # Remove empty lines
        content = '\n'.join([line for line in content.split('\n') if line.strip()])
        
        with open(req_file, 'w') as f:
            f.write(content)
        
        print(f"Updated requirements for {scraper_dir}")

def update_main_py(scraper_dir):
    """Update main.py to use BigQuery"""
    main_file = f"/Users/samwilhoit/Documents/biz-scraper-2/cloud/functions/{scraper_dir}/main.py"
    
    if os.path.exists(main_file):
        with open(main_file, 'r') as f:
            content = f.read()
        
        # Add BigQuery imports
        if 'from google.cloud import firestore' in content:
            content = content.replace(
                'from google.cloud import firestore',
                'from google.cloud import bigquery'
            )
        
        if 'from datetime import datetime' in content and 'date' not in content:
            content = content.replace(
                'from datetime import datetime',
                'from datetime import datetime, date'
            )
        
        # Add BigQuery imports after BeautifulSoup
        if 'from bs4 import BeautifulSoup' in content and 'bigquery_writer' not in content:
            content = content.replace(
                'from bs4 import BeautifulSoup',
                '''from bs4 import BeautifulSoup
from bigquery_writer import BigQueryWriter
from data_transforms import transform_firestore_to_bigquery'''
            )
        
        # Replace Firestore client with BigQuery writer
        content = content.replace(
            'db = firestore.Client()',
            'bq_writer = BigQueryWriter()'
        )
        
        # Remove COLLECTION_NAME and RUNS_COLLECTION
        content = re.sub(r"COLLECTION_NAME = '[^']*'\n", '', content)
        content = re.sub(r"RUNS_COLLECTION = '[^']*'\n", '', content)
        
        # Update store_listings function
        if 'def store_listings(listings, run_id):' in content:
            # Find the function and replace it
            pattern = r'def store_listings\(listings, run_id\):(.*?)(?=\n\n@|\n\ndef|\n\n\n|\Z)'
            new_function = '''def store_listings(listings, run_id):
    """Store listings in BigQuery"""
    if not listings:
        return 0, 0
    
    # Transform listings to BigQuery format
    transformed_listings = []
    for listing_data in listings:
        # Add metadata
        listing_data['first_seen'] = datetime.utcnow()
        listing_data['last_updated'] = datetime.utcnow()
        listing_data['is_active'] = True
        listing_data['scraped_at'] = datetime.utcnow()
        listing_data['ingestion_date'] = date.today()
        
        # Transform to BigQuery format
        transformed = transform_firestore_to_bigquery(listing_data)
        transformed_listings.append(transformed)
    
    # Write to BigQuery
    result = bq_writer.write_listings(transformed_listings)
    
    if result['success']:
        # Deactivate old listings
        active_ids = [l['listing_id'] for l in listings]
        source_name = listings[0].get('source', 'Unknown') if listings else 'Unknown'
        bq_writer.deactivate_old_listings(source_name, active_ids)
        
        # For compatibility, return new and updated counts
        return len(listings), 0  # All listings treated as new for BigQuery
    else:
        print(f"Failed to write to BigQuery: {result['errors']}")
        return 0, 0'''
            
            content = re.sub(pattern, new_function, content, flags=re.DOTALL)
        
        # Update run record creation
        content = re.sub(
            r'run_ref = db\.collection\(RUNS_COLLECTION\)\.document\(run_id\)\s*\n\s*run_ref\.set\(.*?\)',
            '''run_data = {
        'run_id': run_id,
        'source': source,
        'start_time': datetime.utcnow(),
        'status': 'running',
        'trigger_type': request.get_json(silent=True) or {},
        'ingestion_date': date.today()
    }''',
            content,
            flags=re.DOTALL
        )
        
        # Update success run record
        content = re.sub(
            r'run_ref\.update\(\s*\{[^}]*\'status\':\s*[\'"](?:success|completed)[\'"][^}]*\}\s*\)',
            '''run_data.update({
            'end_time': datetime.utcnow(),
            'status': 'success',
            'total_listings_found': len(listings),
            'new_listings': new_count,
            'updated_listings': updated_count
        })
        bq_writer.write_scraper_run(run_data)''',
            content,
            flags=re.DOTALL
        )
        
        # Update error run record
        content = re.sub(
            r'run_ref\.update\(\s*\{[^}]*\'status\':\s*[\'"](?:error|failed)[\'"][^}]*\}\s*\)',
            '''run_data.update({
            'end_time': datetime.utcnow(),
            'status': 'error',
            'error_message': str(e)
        })
        bq_writer.write_scraper_run(run_data)''',
            content,
            flags=re.DOTALL
        )
        
        with open(main_file, 'w') as f:
            f.write(content)
        
        print(f"Updated main.py for {scraper_dir}")

def main():
    """Update all scrapers"""
    print("Updating scrapers to use BigQuery...")
    
    for scraper in scrapers:
        print(f"\nUpdating {scraper}...")
        update_requirements(scraper)
        update_main_py(scraper)
    
    print("\nAll scrapers updated to use BigQuery!")

if __name__ == "__main__":
    main()