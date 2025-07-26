#!/usr/bin/env python3
"""Fixed Empire Flippers scraper"""

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

class EmpireFlippersScraper:
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
    
    def extract_revenue(self, text):
        """Extract revenue from text"""
        revenue_patterns = [
            r'monthly revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self.clean_text(match.group())
        return ""
    
    def extract_profit(self, text):
        """Extract profit from text"""
        profit_patterns = [
            r'monthly net profit[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'net profit[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'profit[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self.clean_text(match.group())
        return ""
    
    def scrape_empire_flippers(self):
        """Scrape Empire Flippers Amazon FBA listings"""
        url = "https://empireflippers.com/marketplace/amazon-fba-businesses-for-sale/"
        response = self.make_request(url)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        # Find all listing items - this is the correct selector
        listing_items = soup.select('div.listing-item')
        logger.info(f"Found {len(listing_items)} listings on Empire Flippers")
        
        for item in listing_items:
            try:
                # Extract description/title from the description paragraph
                desc_elem = item.select_one('.description, p')
                title = ""
                description = ""
                
                if desc_elem:
                    full_desc = self.clean_text(desc_elem.get_text())
                    description = full_desc[:500]
                    # Use first sentence as title
                    first_sentence = full_desc.split('.')[0]
                    title = first_sentence[:150] if first_sentence else "Amazon FBA Business"
                
                # Extract niches as part of title
                niche_elem = item.select_one('.top-info-niches span')
                if niche_elem:
                    niches = niche_elem.get_text().strip()
                    if not title or title == "Amazon FBA Business":
                        title = f"Amazon FBA Business - {niches}"
                    else:
                        title = f"{title} ({niches})"
                
                # Skip if no meaningful title
                if not title or len(title) < 10:
                    continue
                
                # Extract URL
                link_elem = item.select_one('a[href*="/listing/"]')
                listing_url = f"https://empireflippers.com{link_elem['href']}" if link_elem else url
                
                # Extract financial metrics
                full_text = item.get_text()
                
                # Look for price
                price = self.extract_price(full_text)
                
                # Look for revenue
                revenue = self.extract_revenue(full_text)
                
                # Look for profit
                profit = self.extract_profit(full_text)
                
                # Get more specific metrics if available
                price_metric = item.select_one('.metric-item:has(.label:contains("Price")) .value')
                if price_metric and not price:
                    price = self.extract_price(price_metric.get_text())
                
                profit_metric = item.select_one('.metric-item:has(.label:contains("Monthly Net Profit")) .value')
                if profit_metric and not profit:
                    profit = f"Monthly Net Profit: {profit_metric.get_text().strip()}"
                
                revenue_metric = item.select_one('.metric-item:has(.label:contains("Monthly Revenue")) .value')
                if revenue_metric and not revenue:
                    revenue = f"Monthly Revenue: {revenue_metric.get_text().strip()}"
                
                listing = {
                    'source': 'EmpireFlippers',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                    'multiple': '',
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.warning(f"Error parsing Empire Flippers listing: {e}")
        
        return listings
    
    def export_to_csv(self, listings, filename='empire_flippers_results.csv'):
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
        print(f"Listings with revenue: {df['revenue'].str.len().gt(0).sum()}")
        print(f"Listings with profit: {df['profit'].str.len().gt(0).sum()}")
        
        # Show sample
        print("\nSample listings:")
        for i, row in df.head(3).iterrows():
            print(f"\n{i+1}. {row['name']}")
            print(f"   Price: {row['price']}")
            print(f"   Profit: {row['profit']}")
            print(f"   URL: {row['url']}")

def main():
    """Run the Empire Flippers scraper"""
    try:
        scraper = EmpireFlippersScraper()
        listings = scraper.scrape_empire_flippers()
        scraper.export_to_csv(listings)
        
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        raise

if __name__ == "__main__":
    main()