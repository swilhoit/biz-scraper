"""
Cloud Function for scraping Quiet Light listings
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
from urllib.parse import urljoin
import logging
import time


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bq_writer = BigQueryWriter()

SCRAPER_API_KEY = os.environ.get('SCRAPER_API_KEY')


# Create session for reuse
session = requests.Session()

def fetch_page(url):
    """Fetch page using ScraperAPI"""
    params = {
        'api_key': SCRAPER_API_KEY,
        'url': url,
        'country_code': 'us',
    }
    
    logger.info(f"Fetching {url}")
    response = session.get('http://api.scraperapi.com', params=params, timeout=90)
    response.raise_for_status()
    
    return BeautifulSoup(response.text, 'html.parser')


def is_valid_business(title):
    """Filter valid businesses"""
    if not title or len(title.strip()) < 10:
        return False
    
    # Exclude UI elements
    ui_keywords = [
        'instant listing', 'alerts', 'newsletter', 'sign up', 'subscribe'
    ]
    
    title_lower = title.lower()
    for keyword in ui_keywords:
        if keyword in title_lower:
            return False
    
    # Must have business indicators
    indicators = ['brand', 'business', 'amazon', 'fba', 'revenue', '$', 'growth']
    for indicator in indicators:
        if indicator in title_lower:
            return True
    
    return len(title.strip()) > 30


def extract_financials(card):
    """Extract financial data"""
    result = {'price': '', 'revenue': '', 'profit': ''}
    
    full_text = card.get_text()
    
    # Price patterns
    price_patterns = [
        r'Asking\s*Price[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
        r'\$[\d,]+(?:\.\d+)?[KMB]?',
        r'Accepting\s*Offers',
        r'Under\s*Offer'
    ]
    
    for pattern in price_patterns:
        match = re.search(pattern, full_text, re.IGNORECASE)
        if match:
            result['price'] = match.group().strip()
            break
    
    # Revenue patterns
    revenue_match = re.search(r'Revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?', full_text, re.IGNORECASE)
    if revenue_match:
        result['revenue'] = revenue_match.group().strip()
    
    # Profit patterns  
    profit_match = re.search(r'Income[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?', full_text, re.IGNORECASE)
    if profit_match:
        result['profit'] = profit_match.group().strip()
    
    return result


def scrape_category(category_name, category_url, max_pages):
    """Scrape a single category"""
    all_businesses = []
    
    for page in range(1, max_pages + 1):
        try:
            if page == 1:
                url = category_url
            else:
                url = f"{category_url}page/{page}/"
            
            soup = fetch_page(url)
            
            # Use proven selector
            cards = soup.select('div.listing-card.grid-item')
            logger.info(f"{category_name} Page {page}: Found {len(cards)} cards")
            
            if not cards:
                logger.info(f"No cards found on page {page}, stopping pagination")
                break
            
            for card in cards:
                try:
                    # Extract title
                    title_elem = card.select_one('h1, h2, h3, a[href*="/listings/"]')
                    if title_elem:
                        title = title_elem.get_text().strip()
                    else:
                        lines = [line.strip() for line in card.get_text().split('\n') if line.strip()]
                        title = lines[0] if lines else ""
                    
                    if not is_valid_business(title):
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/listings/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Generate listing ID from URL
                    listing_id = f"QL-{business_url.split('/')[-2]}" if '/listings/' in business_url else f"QL-{uuid.uuid4().hex[:8]}"
                    
                    # Extract financials
                    financials = extract_financials(card)
                    
                    business = {
                        'listing_id': listing_id,
                        'title': title[:200],
                        'url': business_url,
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': '',
                        'source': 'Quiet Light',
                        'category': category_name,
                        'niches': [category_name],
                        'location': '',
                        'established_date': None
                    }
                    
                    all_businesses.append(business)
                    
                except Exception as e:
                    logger.error(f"Error processing card: {e}")
                    continue
            
            # If fewer than 20 cards, likely last page
            if len(cards) < 20:
                logger.info(f"Page {page} has only {len(cards)} cards, likely last page")
                break
            
            # Small delay between pages
            time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error on page {page}: {e}")
            break
    
    return all_businesses


def scrape_all_categories():
    """Scrape all Quiet Light categories"""
    categories = [
        ('Amazon FBA', 'https://quietlight.com/amazon-fba-businesses-for-sale/', 10),
        ('Ecommerce', 'https://quietlight.com/ecommerce-businesses-for-sale/', 10),
        ('SaaS', 'https://quietlight.com/saas-businesses-for-sale/', 5),
        ('Content', 'https://quietlight.com/content-businesses-for-sale/', 5),
        ('All Listings', 'https://quietlight.com/listings/', 15)
    ]
    
    all_businesses = []
    
    # Scrape categories sequentially with delays
    for category_name, url, max_pages in categories:
        try:
            logger.info(f"Scraping {category_name} (up to {max_pages} pages)")
            businesses = scrape_category(category_name, url, max_pages)
            all_businesses.extend(businesses)
            logger.info(f"Completed {category_name}: {len(businesses)} businesses")
            
            # Delay between categories
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error scraping {category_name}: {e}")
    
    # Deduplicate by URL
    unique_businesses = []
    seen_urls = set()
    
    for business in all_businesses:
        if business['url'] not in seen_urls:
            unique_businesses.append(business)
            seen_urls.add(business['url'])
    
    logger.info(f"Total unique businesses: {len(unique_businesses)}")
    return unique_businesses


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
    source = 'Quiet Light'
    
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
        # Check for API key
        if not SCRAPER_API_KEY:
            raise ValueError("SCRAPER_API_KEY environment variable not set")
        
        # Scrape all categories
        listings = scrape_all_categories()
        
        # Store in Firestore
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
        
        response = {
            'success': True,
            'run_id': run_id,
            'source': source,
            'total_listings': len(listings),
            'new_listings': new_count,
            'updated_listings': updated_count
        }
        
        return response, 200
        
    except Exception as e:
        logger.error(f"Error in scraper: {str(e)}")
        
        # Update run record with error
        run_data.update({
            'end_time': datetime.utcnow(),
            'status': 'error',
            'error_message': str(e)
        })
        bq_writer.write_scraper_run(run_data)
        
        return {
            'success': False,
            'error': str(e),
            'run_id': run_id
        }, 500