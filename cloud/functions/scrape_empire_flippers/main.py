"""
Cloud Function for scraping Empire Flippers listings
"""

import os
import re
import uuid
import functions_framework
from datetime import datetime, date
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
from bigquery_writer import BigQueryWriter
from data_transforms import transform_firestore_to_bigquery, generate_listing_id, normalize_price, calculate_metrics, calculate_data_completeness


bq_writer = BigQueryWriter()

SCRAPER_API_KEY = os.environ.get('SCRAPER_API_KEY')


def create_business_name(niches, listing_id, description):
    """Create a clean business name from available data"""
    if niches:
        niche_parts = [n.strip() for n in niches.split(',')]
        primary_niche = niche_parts[0] if niche_parts else "General"
        
        # Check for specific business types
        desc_lower = description.lower() if description else ""
        if 'fba' in desc_lower or 'amazon' in desc_lower:
            return f"{primary_niche} Amazon FBA Business"
        elif 'saas' in desc_lower:
            return f"{primary_niche} SaaS Business"
        elif 'ecommerce' in desc_lower or 'e-commerce' in desc_lower:
            return f"{primary_niche} E-commerce Business"
        elif 'content' in desc_lower or 'blog' in desc_lower:
            return f"{primary_niche} Content Site"
        elif 'service' in desc_lower:
            return f"{primary_niche} Service Business"
        else:
            return f"{primary_niche} Online Business"
    
    return f"Online Business {listing_id}"


def extract_listing_id(unlock_link):
    """Extract listing ID from unlock link"""
    if unlock_link:
        match = re.search(r'/marketplace/unlock/(\d+)', unlock_link)
        if match:
            return match.group(1)
    return str(uuid.uuid4())[:8]


def scrape_empire_flippers():
    """Scrape Empire Flippers listings"""
    url = "https://empireflippers.com/marketplace/"
    
    params = {
        'api_key': SCRAPER_API_KEY,
        'url': url,
        'render': 'true'
    }
    
    try:
        response = requests.get("http://api.scraperapi.com", params=params, timeout=60)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to fetch Empire Flippers: {str(e)}")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    listings = []
    
    listing_items = soup.select('div.listing-item')
    
    for item in listing_items:
        try:
            # Extract unlock link for ID
            unlock_link_elem = item.select_one('a[href*="/marketplace/unlock/"]')
            unlock_link = unlock_link_elem['href'] if unlock_link_elem else None
            listing_id = extract_listing_id(unlock_link)
            
            # Extract niches
            niches_elem = item.select_one('span.listing-overview-niche, div.listing-niches')
            niches = niches_elem.get_text().strip() if niches_elem else ""
            
            # Extract description
            desc_elem = item.select_one('div.description, p.listing-description')
            description = desc_elem.get_text().strip() if desc_elem else ""
            
            # Create clean name
            name = create_business_name(niches, listing_id, description)
            
            # Extract monetization
            monetization_elem = item.select_one('span.listing-overview-monetization')
            monetization = monetization_elem.get_text().strip() if monetization_elem else ""
            
            # Extract price
            price_elem = item.select_one('span.listing-overview-price')
            price = price_elem.get_text().strip() if price_elem else ""
            
            # Extract profit
            profit_elem = item.select_one('span.listing-overview-net')
            profit = profit_elem.get_text().strip() if profit_elem else ""
            
            listing_url = f"https://empireflippers.com{unlock_link}" if unlock_link else url
            
            listing = {
                'listing_id': f"ef_{listing_id}",
                'source': 'Empire Flippers',
                'name': name,
                'url': listing_url,
                'price': price,
                'profit': profit,
                'category': niches,
                'description': description[:500] if description else "",
                'raw_data': {
                    'monetization': monetization,
                    'niches': niches
                }
            }
            
            listings.append(listing)
            
        except Exception as e:
            print(f"Error parsing Empire Flippers listing: {e}")
    
    return listings


def store_listings(listings, run_id):
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
        bq_writer.deactivate_old_listings('Empire Flippers', active_ids)
        
        # For compatibility, return new and updated counts
        return len(listings), 0  # All listings treated as new for BigQuery
    else:
        print(f"Failed to write to BigQuery: {result['errors']}")
        return 0, 0


@functions_framework.http
def main(request):
    """HTTP Cloud Function entry point"""
    run_id = str(uuid.uuid4())
    source = 'Empire Flippers'
    
    # Create run record for BigQuery
    run_data = {
        'run_id': run_id,
        'source': source,
        'start_time': datetime.utcnow(),
        'status': 'running',
        'trigger_type': 'manual',
        'ingestion_date': date.today()
    }
    
    try:
        # Scrape listings
        listings = scrape_empire_flippers()
        
        # Store in database
        new_count, updated_count = store_listings(listings, run_id)
        
        # Update run record
        run_data.update({
            'end_time': datetime.utcnow(),
            'status': 'success',
            'total_listings_found': len(listings),
            'new_listings': new_count,
            'updated_listings': updated_count
        })
        bq_writer.write_scraper_run(run_data)
        
        return {
            'success': True,
            'run_id': run_id,
            'source': source,
            'listings_found': len(listings),
            'new_listings': new_count,
            'updated_listings': updated_count
        }, 200
        
    except Exception as e:
        # Update run record with error
        run_data.update({
            'end_time': datetime.utcnow(),
            'status': 'error',
            'error_message': str(e)
        })
        bq_writer.write_scraper_run(run_data)
        
        return {
            'success': False,
            'run_id': run_id,
            'source': source,
            'error': str(e)
        }, 500