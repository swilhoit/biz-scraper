#!/usr/bin/env python3
"""Fixed Empire Flippers scraper with better naming"""

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
    
    def create_business_name(self, niches, listing_id, description):
        """Create a concise business name"""
        # First try to use niches
        if niches:
            # Clean up niches
            niche_parts = [n.strip() for n in niches.split(',')]
            primary_niche = niche_parts[0] if niche_parts else "General"
            
            # Add business type
            if 'fba' in description.lower() or 'amazon' in description.lower():
                return f"{primary_niche} Amazon FBA Business"
            else:
                return f"{primary_niche} eCommerce Business"
        
        # If no niches, try to extract key product/business type from description
        if description:
            # Look for key product mentions
            product_patterns = [
                r'specializes in ([^,\.]+)',
                r'sells ([^,\.]+)',
                r'offering ([^,\.]+)',
                r'features ([^,\.]+)',
            ]
            
            for pattern in product_patterns:
                match = re.search(pattern, description.lower())
                if match:
                    product = match.group(1).strip()
                    # Clean up the product name
                    product = product.replace('products', '').strip()
                    product = product.title()
                    return f"{product} FBA Business"
        
        # Fallback to generic name with ID
        if listing_id:
            return f"Amazon FBA Business #{listing_id}"
        
        return "Amazon FBA Business"
    
    def scrape_empire_flippers(self):
        """Scrape Empire Flippers Amazon FBA listings"""
        url = "https://empireflippers.com/marketplace/amazon-fba-businesses-for-sale/"
        response = self.make_request(url)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        # Find all listing items
        listing_items = soup.select('div.listing-item')
        logger.info(f"Found {len(listing_items)} listings on Empire Flippers")
        
        for item in listing_items:
            try:
                # Extract description
                desc_elem = item.select_one('.description, p')
                description = ""
                
                if desc_elem:
                    description = self.clean_text(desc_elem.get_text())
                
                # Extract niches
                niche_elem = item.select_one('.top-info-niches span')
                niches = niche_elem.get_text().strip() if niche_elem else ""
                
                # Extract listing ID from URL
                link_elem = item.select_one('a[href*="/listing/"]')
                listing_id = ""
                listing_url = url
                
                if link_elem:
                    listing_url = f"https://empireflippers.com{link_elem['href']}"
                    # Extract ID from URL
                    id_match = re.search(r'/listing/(\d+)', link_elem['href'])
                    if id_match:
                        listing_id = id_match.group(1)
                
                # Create a proper business name
                title = self.create_business_name(niches, listing_id, description)
                
                # Extract financial metrics
                full_text = item.get_text()
                
                # Look for price
                price = self.extract_price(full_text)
                
                # Look for revenue - try to get specific monthly revenue
                revenue = ""
                revenue_metric = item.select_one('.metric-item:has(.label:contains("Monthly Revenue")) .value')
                if revenue_metric:
                    revenue = revenue_metric.get_text().strip()
                else:
                    revenue = self.extract_revenue(full_text)
                
                # Look for profit - try to get specific monthly net profit
                profit = ""
                profit_metric = item.select_one('.metric-item:has(.label:contains("Monthly Net Profit")) .value')
                if profit_metric:
                    profit = profit_metric.get_text().strip()
                else:
                    profit = self.extract_profit(full_text)
                
                listing = {
                    'source': 'EmpireFlippers',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description[:500],
                    'url': listing_url,
                    'multiple': '',
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.warning(f"Error parsing Empire Flippers listing: {e}")
        
        return listings
    
    def export_to_csv(self, listings, filename='empire_flippers_fixed_names.csv'):
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
        for i, row in df.head(5).iterrows():
            print(f"\n{i+1}. {row['name']}")
            print(f"   Price: {row['price']}")
            print(f"   Revenue: {row['revenue']}")
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