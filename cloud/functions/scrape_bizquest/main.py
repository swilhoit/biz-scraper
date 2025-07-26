"""
Cloud Function for scraping BizQuest listings
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
from data_transforms import transform_firestore_to_bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bq_writer = BigQueryWriter()

SCRAPER_API_KEY = os.environ.get('SCRAPER_API_KEY')
MAX_DETAIL_PAGES = int(os.environ.get('MAX_DETAIL_PAGES', '20'))

# Create session for reuse
session = requests.Session()


def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip())


def extract_financial_value(text):
    """Extract financial value from text"""
    if not text:
        return ""
    
    text = text.strip()
    
    if re.match(r'^\$[\d,]+$', text):
        return text
    
    patterns = [
        r'\$[\d,]+(?:\.\d{2})?',
        r'\$\d+(?:\.\d+)?[KkMm]',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group()
    
    number_match = re.search(r'[\d,]+(?:\.\d{2})?', text)
    if number_match:
        return '$' + number_match.group()
        
    return ""


def scrape_detail_page(url):
    """Scrape revenue from BizQuest detail page"""
    params = {
        'api_key': SCRAPER_API_KEY,
        'url': url
    }
    
    try:
        response = session.get("http://api.scraperapi.com", params=params, timeout=60)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for financial details
        financial_containers = soup.select('app-financial-details .data-container')
        
        for container in financial_containers:
            label_elem = container.select_one('.text-info')
            value_elem = container.select_one('.price, b:not(.text-info)')
            
            if label_elem and value_elem:
                label = label_elem.get_text().strip().lower()
                if 'gross revenue' in label:
                    value = value_elem.get_text().strip()
                    if '$' in value:
                        return value
                    elif re.search(r'[\d,]+', value):
                        return '$' + re.search(r'[\d,]+', value).group()
        
        return ''
        
    except Exception as e:
        logger.error(f"Error fetching detail page {url}: {e}")
        return ''


def scrape_bizquest():
    """Scrape BizQuest listings"""
    url = "https://www.bizquest.com/amazon-business-for-sale/"
    
    params = {
        'api_key': SCRAPER_API_KEY,
        'url': url
    }
    
    try:
        response = session.get("http://api.scraperapi.com", params=params, timeout=60)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to fetch BizQuest: {str(e)}")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    listings = []
    
    listing_items = soup.select('div.listing')
    
    for item in listing_items:
        try:
            # Skip ads
            if 'ad' in item.get('class', []) or item.find('div', class_='gpt'):
                continue
            
            # Extract title
            title_elem = item.select_one('h3.title')
            if not title_elem:
                continue
                
            title = clean_text(title_elem.get_text())
            if not title or len(title) < 10:
                continue
            
            # Extract URL
            link_elem = item.select_one('a[href*="/business-for-sale/"]')
            listing_url = f"https://www.bizquest.com{link_elem['href']}" if link_elem else url
            
            # Generate listing ID from URL
            listing_id = re.search(r'/([^/]+)/$', listing_url)
            listing_id = listing_id.group(1) if listing_id else str(uuid.uuid4())[:8]
            
            # Extract location
            location_elem = item.select_one('p.location')
            location = clean_text(location_elem.get_text()) if location_elem else ""
            
            # Extract description
            desc_elem = item.select_one('p.description')
            description = clean_text(desc_elem.get_text()) if desc_elem else ""
            
            # Extract price
            price = ""
            price_elem = item.select_one('p.asking-price')
            if price_elem:
                price_text = price_elem.get_text()
                if 'not disclosed' not in price_text.lower():
                    price = extract_financial_value(price_text)
            
            # Extract cash flow
            profit = ""
            cash_flow_elem = item.select_one('p.cash-flow')
            if cash_flow_elem:
                cash_flow_text = cash_flow_elem.get_text()
                profit_match = re.search(r'Cash Flow:\s*(\$[\d,]+)', cash_flow_text)
                if profit_match:
                    profit = profit_match.group(1)
            
            listing = {
                'listing_id': f"bq_{listing_id}",
                'source': 'BizQuest',
                'name': title,
                'url': listing_url,
                'price': price,
                'profit': profit,
                'location': location,
                'description': description[:500] if description else "",
                'revenue': ''  # Will be filled from detail page
            }
            
            listings.append(listing)
            
        except Exception as e:
            logger.warning(f"Error parsing BizQuest listing: {e}")
    
    # Small delay after parsing listings
    time.sleep(1)
    
    # Fetch revenue data from detail pages (limit for performance)
    listings_to_detail = listings[:MAX_DETAIL_PAGES]
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_listing = {}
        
        for listing in listings_to_detail:
            future = executor.submit(scrape_detail_page, listing['url'])
            future_to_listing[future] = listing
        
        for future in as_completed(future_to_listing):
            listing = future_to_listing[future]
            try:
                revenue = future.result()
                if revenue:
                    listing['revenue'] = revenue
            except Exception as e:
                logger.error(f"Error getting revenue for {listing['url']}: {e}")
    
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
        source_name = listings[0].get('source', 'Unknown') if listings else 'Unknown'
        bq_writer.deactivate_old_listings(source_name, active_ids)
        
        # For compatibility, return new and updated counts
        return len(listings), 0  # All listings treated as new for BigQuery
    else:
        print(f"Failed to write to BigQuery: {result['errors']}")
        return 0, 0


@functions_framework.http
def main(request):
    """HTTP Cloud Function entry point"""
    run_id = str(uuid.uuid4())
    source = 'BizQuest'
    
    # Create run record
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
        listings = scrape_bizquest()
        
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