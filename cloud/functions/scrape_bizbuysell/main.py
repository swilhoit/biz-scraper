"""
Cloud Function for scraping BizBuySell Amazon stores
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
MAX_PAGES = int(os.environ.get('MAX_PAGES', '5'))


def make_request(url, use_render=False):
    """Make request with ScraperAPI"""
    params = {
        'api_key': SCRAPER_API_KEY,
        'url': url,
    }
    
    if use_render:
        params['render'] = 'true'
    
    try:
        logger.info(f"Fetching {url} ({'render' if use_render else 'no-render'})")
        response = requests.get('http://api.scraperapi.com', params=params, timeout=60)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def extract_price_improved(text):
    """Extract price from text"""
    patterns = [
        r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'\$([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            clean_match = re.sub(r'[^\d.,KkMm]', '', match)
            if clean_match and any(c.isdigit() for c in clean_match):
                try:
                    if clean_match.upper().endswith('K'):
                        value = float(clean_match[:-1]) * 1000
                    elif clean_match.upper().endswith('M'):
                        value = float(clean_match[:-1]) * 1000000
                    else:
                        value = float(clean_match.replace(',', ''))
                    
                    if 10000 <= value <= 50000000:
                        return f"${value:,.0f}"
                except:
                    continue
    
    return ""


def extract_revenue_improved(text):
    """Extract revenue from text"""
    patterns = [
        r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'(\d{4})\s+revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'\$([0-9]{1,3}(?:,[0-9]{3})*)[KkMm]?\s*(?:revenue|sales)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[-1]
            
            clean_match = re.sub(r'[^\d.,KkMm]', '', match)
            if clean_match and any(c.isdigit() for c in clean_match):
                try:
                    if clean_match.upper().endswith('K'):
                        value = float(clean_match[:-1]) * 1000
                    elif clean_match.upper().endswith('M'):
                        value = float(clean_match[:-1]) * 1000000
                    else:
                        value = float(clean_match.replace(',', ''))
                    
                    if 1000 <= value <= 100000000:
                        return f"${value:,.0f}"
                except:
                    continue
    
    return ""


def extract_profit_improved(text):
    """Extract profit/cash flow from text"""
    patterns = [
        r'cash\s+flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        r'\$([0-9]{1,3}(?:,[0-9]{3})*)[KkMm]?\s*(?:cash flow|profit|ebitda)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            clean_match = re.sub(r'[^\d.,KkMm]', '', match)
            if clean_match and any(c.isdigit() for c in clean_match):
                try:
                    if clean_match.upper().endswith('K'):
                        value = float(clean_match[:-1]) * 1000
                    elif clean_match.upper().endswith('M'):
                        value = float(clean_match[:-1]) * 1000000
                    else:
                        value = float(clean_match.replace(',', ''))
                    
                    if 1000 <= value <= 10000000:
                        return f"${value:,.0f}"
                except:
                    continue
    
    return ""


def extract_location(text):
    """Extract location from text"""
    # Look for state abbreviations or city/state patterns
    state_pattern = r'\b([A-Z]{2})\b'
    matches = re.findall(state_pattern, text)
    
    # US state abbreviations
    us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    
    for match in matches:
        if match in us_states:
            return match
    
    return ""


def extract_comprehensive_data(link, full_url, parent):
    """Extract comprehensive business data"""
    try:
        # Get business name from link text
        name = link.get_text().strip()
        
        # Get full text from parent container
        if parent:
            full_text = parent.get_text()
        else:
            full_text = link.get_text()
        
        # Only include Amazon/FBA related businesses
        if not any(keyword in full_text.lower() for keyword in ['amazon', 'fba', 'ecommerce', 'e-commerce']):
            return None
        
        # Extract financial data
        price = extract_price_improved(full_text)
        revenue = extract_revenue_improved(full_text)
        profit = extract_profit_improved(full_text)
        location = extract_location(full_text)
        
        # Create clean description
        description = re.sub(r'\s+', ' ', full_text[:300]).strip()
        
        # Generate listing ID from URL
        listing_id = f"BBS-{full_url.split('/')[-2]}" if '/business-opportunity/' in full_url else f"BBS-{uuid.uuid4().hex[:8]}"
        
        listing = {
            'listing_id': listing_id,
            'source': 'BizBuySell',
            'title': name[:150],
            'price': price,
            'revenue': revenue,
            'profit': profit,
            'description': description,
            'url': full_url,
            'location': location,
            'niches': ['Amazon', 'Ecommerce'],
            'category': 'Amazon Store',
            'established_date': None
        }
        
        return listing
        
    except Exception as e:
        logger.debug(f"Error extracting comprehensive data: {e}")
        return None


def extract_listings(soup, page_url):
    """Extract listings from page"""
    listings = []
    
    # Use the working opportunity links strategy
    opportunity_links = soup.select('a[href*="opportunity"]')
    logger.info(f"  Found {len(opportunity_links)} opportunity links")
    
    for link in opportunity_links:
        try:
            href = link.get('href', '')
            if not href or '/business-opportunity/' not in href:
                continue
                
            full_url = urljoin(page_url, href)
            link_text = link.get_text().strip()
            
            # Skip navigation/UI links
            if any(skip in link_text.lower() for skip in ['register', 'login', 'sign up', 'learn more', 'see more', 'contact']):
                continue
            
            # Skip very short names
            if len(link_text) < 15:
                continue
            
            # Find parent container
            parent = link.find_parent(['div'])
            for _ in range(3):  # Try to go up 3 levels
                if parent and parent.parent:
                    parent = parent.parent
                else:
                    break
            
            # Get comprehensive business data
            listing = extract_comprehensive_data(link, full_url, parent)
            if listing:
                listings.append(listing)
                
        except Exception as e:
            logger.debug(f"Error processing opportunity link: {e}")
    
    return listings


def scrape_amazon_stores():
    """Scrape BizBuySell Amazon stores"""
    base_url = "https://www.bizbuysell.com/amazon-stores-for-sale/"
    all_listings = []
    
    for page in range(1, MAX_PAGES + 1):
        try:
            if page == 1:
                url = base_url
            else:
                url = f"{base_url}?page={page}"
            
            logger.info(f"Scraping page {page}...")
            
            response = make_request(url, use_render=True)
            if not response:
                continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_listings = extract_listings(soup, url)
            
            if page_listings:
                all_listings.extend(page_listings)
                logger.info(f"Page {page}: Found {len(page_listings)} listings")
            else:
                logger.warning(f"Page {page}: No listings found")
                break
            
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error scraping page {page}: {e}")
    
    # Remove duplicates
    unique_listings = []
    seen_urls = set()
    
    for listing in all_listings:
        if listing['url'] not in seen_urls:
            seen_urls.add(listing['url'])
            unique_listings.append(listing)
    
    logger.info(f"Total unique listings found: {len(unique_listings)}")
    return unique_listings


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
    source = 'BizBuySell'
    
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
        
        # Scrape Amazon stores
        listings = scrape_amazon_stores()
        
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