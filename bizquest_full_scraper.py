#!/usr/bin/env python3
"""BizQuest full scraper with detail pages"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BizQuestFullScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
    def make_request(self, url, retries=3):
        """Make a request using Scraper API"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'render': 'true'
        }
        
        for attempt in range(retries):
            try:
                response = requests.get("http://api.scraperapi.com", params=params, timeout=90)
                response.raise_for_status()
                return response
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None
    
    def clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text.strip())
        return text
    
    def extract_financial_value(self, text):
        """Extract financial value from text"""
        if not text:
            return ""
        
        # Clean the text first
        text = text.strip()
        
        # If it's already in a good format, return it
        if re.match(r'^\$[\d,]+$', text):
            return text
        
        # Look for currency patterns
        patterns = [
            r'\$[\d,]+(?:\.\d{2})?',
            r'\$\d+(?:\.\d+)?[KkMm]',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group()
        
        # If no dollar sign but has numbers, add it
        number_match = re.search(r'[\d,]+(?:\.\d{2})?', text)
        if number_match:
            return '$' + number_match.group()
            
        return ""
    
    def scrape_detail_page(self, url):
        """Scrape a single BizQuest detail page"""
        logger.info(f"Scraping detail: {url}")
        
        response = self.make_request(url)
        if not response:
            return {}
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Initialize data
        data = {
            'gross_revenue': '',
            'ebitda': '',
            'inventory': '',
            'established': '',
            'employees': '',
            'relocatable': '',
            'home_based': '',
            'financing_available': False,
            'detailed_description': ''
        }
        
        try:
            # Look for the financial details component
            financial_containers = soup.select('app-financial-details .data-container')
            
            for container in financial_containers:
                label_elem = container.select_one('.text-info')
                value_elem = container.select_one('.price, b:not(.text-info)')
                
                if label_elem and value_elem:
                    label = label_elem.get_text().strip().lower()
                    value = value_elem.get_text().strip()
                    
                    if 'gross revenue' in label:
                        data['gross_revenue'] = self.extract_financial_value(value)
                    elif 'ebitda' in label and 'not disclosed' not in value.lower():
                        data['ebitda'] = self.extract_financial_value(value)
                    elif 'inventory' in label and 'not disclosed' not in value.lower():
                        data['inventory'] = self.extract_financial_value(value)
            
            # Extract from the "About the Business" section
            dl_elements = soup.select('dl.dl-horizontal')
            for dl in dl_elements:
                dt_elements = dl.find_all('dt')
                dd_elements = dl.find_all('dd')
                
                for dt, dd in zip(dt_elements, dd_elements):
                    label = dt.get_text().strip().lower()
                    value = dd.get_text().strip()
                    
                    if 'years in operation' in label:
                        data['years_in_operation'] = value
                    elif 'employees' in label:
                        data['employees'] = value
                    elif 'relocatable' in label:
                        data['relocatable'] = value
                    elif 'home based' in label:
                        data['home_based'] = value
                    elif 'financing' in label and 'cash only' not in value.lower():
                        data['financing_available'] = True
            
            # Get detailed description
            desc_elem = soup.select_one('.f-18.bq-color-report')
            if desc_elem:
                data['detailed_description'] = self.clean_text(desc_elem.get_text())[:1000]
            
            # Get business type from title or description
            title_elem = soup.select_one('h3.title')
            if title_elem:
                data['full_title'] = self.clean_text(title_elem.get_text())
            
            return data
            
        except Exception as e:
            logger.error(f"Error parsing detail page {url}: {e}")
            return {}
    
    def scrape_listings_page(self, page_url):
        """Scrape the main listings page"""
        response = self.make_request(page_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        listing_items = soup.select('div.listing')
        
        for item in listing_items:
            try:
                # Skip ads
                if 'ad' in item.get('class', []) or item.find('div', class_='gpt'):
                    continue
                
                # Extract basic info
                title_elem = item.select_one('h3.title')
                if not title_elem:
                    continue
                    
                title = self.clean_text(title_elem.get_text())
                
                if not title or len(title) < 10:
                    continue
                
                # Extract URL
                link_elem = item.select_one('a[href*="/business-for-sale/"]')
                listing_url = f"https://www.bizquest.com{link_elem['href']}" if link_elem else page_url
                
                # Extract location
                location_elem = item.select_one('p.location')
                location = self.clean_text(location_elem.get_text()) if location_elem else ""
                
                # Extract description
                desc_elem = item.select_one('p.description')
                description = self.clean_text(desc_elem.get_text()) if desc_elem else ""
                
                # Extract price
                price = ""
                price_elem = item.select_one('p.asking-price')
                if price_elem:
                    price_text = price_elem.get_text()
                    if 'not disclosed' not in price_text.lower():
                        price = self.extract_financial_value(price_text)
                
                # Extract cash flow
                profit = ""
                cash_flow_elem = item.select_one('p.cash-flow')
                if cash_flow_elem:
                    cash_flow_text = cash_flow_elem.get_text()
                    profit_match = re.search(r'Cash Flow:\s*(\$[\d,]+)', cash_flow_text)
                    if profit_match:
                        profit = profit_match.group(1)
                
                listing = {
                    'source': 'BizQuest',
                    'name': title,
                    'price': price,
                    'revenue': '',  # Will be filled from detail page
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                    'location': location,
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.warning(f"Error parsing listing: {e}")
        
        return listings
    
    def scrape_all_with_details(self, max_detail_pages=10):
        """Scrape listings and their detail pages"""
        # First get all listings
        logger.info("Scraping main listings page...")
        listings = self.scrape_listings_page("https://www.bizquest.com/amazon-business-for-sale/")
        logger.info(f"Found {len(listings)} listings")
        
        # Limit detail scraping for testing
        listings_to_detail = listings[:max_detail_pages]
        logger.info(f"Scraping details for {len(listings_to_detail)} listings...")
        
        # Scrape details in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_listing = {}
            
            for listing in listings_to_detail:
                future = executor.submit(self.scrape_detail_page, listing['url'])
                future_to_listing[future] = listing
            
            for future in as_completed(future_to_listing):
                listing = future_to_listing[future]
                try:
                    detail_data = future.result()
                    
                    # Update listing with detail data
                    if detail_data.get('gross_revenue'):
                        listing['revenue'] = detail_data['gross_revenue']
                    
                    # Add additional fields
                    listing['years_in_operation'] = detail_data.get('years_in_operation', '')
                    listing['employees'] = detail_data.get('employees', '')
                    listing['financing_available'] = detail_data.get('financing_available', False)
                    
                    if detail_data.get('detailed_description'):
                        listing['description'] = detail_data['detailed_description'][:500]
                    
                    logger.info(f"Updated: {listing['name'][:50]}...")
                    
                except Exception as e:
                    logger.error(f"Error getting details for {listing['url']}: {e}")
        
        # Add placeholder data for listings we didn't detail scrape
        for listing in listings[max_detail_pages:]:
            listing['years_in_operation'] = ''
            listing['employees'] = ''
            listing['financing_available'] = False
        
        return listings
    
    def export_to_csv(self, listings, filename='bizquest_full_results.csv'):
        """Export listings to CSV"""
        if not listings:
            logger.warning("No listings to export")
            return
        
        df = pd.DataFrame(listings)
        
        # Reorder columns
        columns = ['source', 'name', 'location', 'price', 'revenue', 'profit', 
                   'description', 'years_in_operation', 'employees', 
                   'financing_available', 'url']
        
        # Only include columns that exist
        columns = [col for col in columns if col in df.columns]
        df = df[columns]
        
        df.to_csv(filename, index=False)
        logger.info(f"Exported {len(df)} listings to {filename}")
        
        # Print summary
        print(f"\nSummary:")
        print(f"Total listings: {len(df)}")
        print(f"Listings with price: {df['price'].astype(str).str.len().gt(0).sum()}")
        print(f"Listings with revenue: {df['revenue'].astype(str).str.len().gt(0).sum()}")
        print(f"Listings with profit: {df['profit'].astype(str).str.len().gt(0).sum()}")
        
        # Show sample with revenue
        revenue_listings = df[df['revenue'].astype(str).str.len() > 0].head(3)
        if len(revenue_listings) > 0:
            print("\nSample listings with revenue data:")
            for i, row in revenue_listings.iterrows():
                print(f"\n{row['name']}")
                print(f"  Location: {row['location']}")
                print(f"  Price: {row['price']}")
                print(f"  Revenue: {row['revenue']}")
                print(f"  Cash Flow: {row['profit']}")

def main():
    """Run the BizQuest full scraper"""
    scraper = BizQuestFullScraper()
    
    # Scrape with limited detail pages for testing
    listings = scraper.scrape_all_with_details(max_detail_pages=10)
    
    # Export results
    scraper.export_to_csv(listings)

if __name__ == "__main__":
    main()