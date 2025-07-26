#!/usr/bin/env python3
"""Fixed BizQuest scraper"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import re
from urllib.parse import urljoin
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BizQuestScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
    def make_request(self, url):
        """Make a request using Scraper API"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'render': 'true'
        }
        
        logger.info(f"Fetching {url}")
        response = requests.get("http://api.scraperapi.com", params=params, timeout=90)
        response.raise_for_status()
        return response
    
    def clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text.strip())
        return text
    
    def extract_price(self, text):
        """Extract price from text"""
        price_patterns = [
            r'\$[\d,]+(?:\.\d{2})?',
            r'\$\d+(?:\.\d+)?[KkMm]',
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group()
        return ""
    
    def scrape_bizquest(self):
        """Scrape BizQuest Amazon business listings"""
        url = "https://www.bizquest.com/amazon-business-for-sale/"
        response = self.make_request(url)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        # Find all listing divs - BizQuest uses Angular with specific structure
        listing_items = soup.select('div.listing')
        logger.info(f"Found {len(listing_items)} listings on BizQuest")
        
        for item in listing_items:
            try:
                # Skip non-business listings (ads, etc)
                if 'ad' in item.get('class', []) or item.find('div', class_='gpt'):
                    continue
                
                # Extract title
                title_elem = item.select_one('h3.title')
                if not title_elem:
                    continue
                    
                title = self.clean_text(title_elem.get_text())
                
                # Skip if title is too short or generic
                if not title or len(title) < 10:
                    continue
                
                # Extract URL
                link_elem = item.select_one('a[href*="/business-for-sale/"]')
                listing_url = f"https://www.bizquest.com{link_elem['href']}" if link_elem else url
                
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
                        price = self.extract_price(price_text)
                
                # Extract cash flow (profit)
                profit = ""
                cash_flow_elem = item.select_one('p.cash-flow')
                if cash_flow_elem:
                    cash_flow_text = cash_flow_elem.get_text()
                    # Extract the number from "Cash Flow: $XXX"
                    profit_match = re.search(r'Cash Flow:\s*(\$[\d,]+)', cash_flow_text)
                    if profit_match:
                        profit = profit_match.group(1)
                
                # Revenue is not directly shown, will be empty
                revenue = ""
                
                listing = {
                    'source': 'BizQuest',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                    'location': location,
                    'multiple': '',
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.warning(f"Error parsing BizQuest listing: {e}")
        
        return listings
    
    def export_to_csv(self, listings, filename='bizquest_results.csv'):
        """Export listings to CSV"""
        if not listings:
            logger.warning("No listings to export")
            return
        
        df = pd.DataFrame(listings)
        df.to_csv(filename, index=False)
        logger.info(f"Exported {len(listings)} listings to {filename}")
        
        # Print summary
        print(f"\nSummary:")
        print(f"Total listings: {len(listings)}")
        print(f"Listings with price: {df['price'].str.len().gt(0).sum()}")
        print(f"Listings with profit: {df['profit'].str.len().gt(0).sum()}")
        
        # Show sample
        print("\nSample listings:")
        for i, row in df.head(5).iterrows():
            print(f"\n{i+1}. {row['name']}")
            print(f"   Location: {row['location']}")
            print(f"   Price: {row['price']}")
            print(f"   Cash Flow: {row['profit']}")
            print(f"   Description: {row['description']}")
            print(f"   URL: {row['url']}")

def main():
    """Run the BizQuest scraper"""
    try:
        scraper = BizQuestScraper()
        listings = scraper.scrape_bizquest()
        scraper.export_to_csv(listings)
        
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        raise

if __name__ == "__main__":
    main()