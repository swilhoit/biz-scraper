#!/usr/bin/env python3
"""
BizBuySell Amazon Stores Only Scraper
Focused scraper for https://www.bizbuysell.com/amazon-stores-for-sale/ (4 pages only)
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import time
import re
from urllib.parse import urljoin
from typing import List, Dict, Optional
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BizBuySellAmazonScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()

    def make_request(self, url: str, use_render: bool = False, retries: int = 3) -> Optional[requests.Response]:
        """Make request with ScraperAPI"""
        params = {
            'api_key': self.api_key,
            'url': url,
        }
        
        if use_render:
            params['render'] = 'true'
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching {url} ({'render' if use_render else 'no-render'}, attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=60)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
        return None

    def scrape_amazon_stores(self) -> List[Dict]:
        """Scrape BizBuySell Amazon stores section (4 pages)"""
        logger.info("🎯 SCRAPING BIZBUYSELL AMAZON STORES (4 pages only)...")
        
        base_url = "https://www.bizbuysell.com/amazon-stores-for-sale/"
        all_listings = []
        
        # Scrape pages 1-4
        for page in range(1, 5):
            try:
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}?page={page}"
                
                logger.info(f"📄 Scraping page {page}...")
                
                # Try no-render first (faster), then render if needed
                response = None
                for use_render in [False, True]:
                    response = self.make_request(url, use_render=use_render, retries=2)
                    if response:
                        break
                
                if not response:
                    logger.error(f"Could not fetch page {page}")
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                page_listings = self.extract_listings_from_page(soup, url)
                
                if page_listings:
                    all_listings.extend(page_listings)
                    logger.info(f"✅ Page {page}: Found {len(page_listings)} listings")
                else:
                    logger.warning(f"❌ Page {page}: No listings found")
                
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error scraping page {page}: {e}")
        
        logger.info(f"🎉 Total Amazon store listings found: {len(all_listings)}")
        return all_listings

    def extract_listings_from_page(self, soup: BeautifulSoup, page_url: str) -> List[Dict]:
        """Extract listings from a page using multiple selector strategies"""
        listings = []
        
        # Try different selectors that BizBuySell might use
        selectors_to_try = [
            'div.result-item',
            'div.listing-item', 
            'div.business-item',
            'article.business',
            'div[class*="listing"]',
            'div[class*="business"]',
            'div[class*="result"]',
            '.business-card',
            '.listing-card',
            'div.search-result'
        ]
        
        for selector in selectors_to_try:
            elements = soup.select(selector)
            if elements and len(elements) >= 3:  # Good indicator of actual listings
                logger.info(f"  Using selector '{selector}' - found {len(elements)} elements")
                
                for element in elements:
                    listing = self.extract_single_listing(element, page_url)
                    if listing:
                        listings.append(listing)
                
                break  # Use first successful selector
        
        return listings

    def extract_single_listing(self, element, page_url: str) -> Optional[Dict]:
        """Extract a single listing from an element"""
        try:
            # Find the main link
            link = element.find('a', href=True)
            if not link:
                return None
            
            listing_url = urljoin(page_url, link['href'])
            
            # Extract title - try multiple approaches
            title = ""
            
            # Try specific title selectors
            for selector in ['h2', 'h3', 'h4', '.title', '.business-title', '.listing-title']:
                title_elem = element.find(selector)
                if title_elem:
                    title = title_elem.get_text().strip()
                    break
            
            # Fallback to link text
            if not title:
                title = link.get_text().strip()
            
            # Clean and validate title
            title = re.sub(r'\s+', ' ', title).strip()
            
            # Skip if title is too short or looks like navigation
            skip_terms = ['page', 'next', 'previous', 'more', 'see all', 'franchise', 'register']
            if len(title) < 10 or any(term in title.lower() for term in skip_terms):
                return None
            
            # Extract element text for financial data
            element_text = element.get_text()
            
            # Extract financial information
            price = self.extract_price(element_text)
            revenue = self.extract_revenue(element_text)
            profit = self.extract_profit(element_text)
            
            # Extract location
            location = ""
            location_elem = element.find(['span', 'div'], string=re.compile(r'[A-Z]{2}|,\s*[A-Z]{2}'))
            if location_elem:
                location = location_elem.get_text().strip()
            
            # Create description
            description_parts = [location, element_text[:200]]
            description = " ".join(filter(None, description_parts)).strip()
            description = re.sub(r'\s+', ' ', description)
            
            listing = {
                'source': 'BizBuySell',
                'name': title,
                'price': price,
                'revenue': revenue,
                'profit': profit,
                'description': description[:500],
                'url': listing_url,
                'multiple': '',
            }
            
            return listing
            
        except Exception as e:
            logger.debug(f"Error extracting listing: {e}")
            return None

    def extract_price(self, text: str) -> str:
        """Extract asking price"""
        patterns = [
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$?([\d,]+(?:\.\d+)?)[KkMm]?(?=.*(?:asking|price))',
            r'\$([\d,]+(?:\.\d+)?)[KkMm]?'
        ]
        return self._extract_by_patterns(text, patterns)

    def extract_revenue(self, text: str) -> str:
        """Extract revenue/sales"""
        patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'gross[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'annual[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?'
        ]
        return self._extract_by_patterns(text, patterns)

    def extract_profit(self, text: str) -> str:
        """Extract profit/cash flow"""
        patterns = [
            r'cash\s+flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'earnings[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'net[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?'
        ]
        return self._extract_by_patterns(text, patterns)

    def _extract_by_patterns(self, text: str, patterns: List[str]) -> str:
        """Extract financial value using regex patterns"""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1) if len(match.groups()) > 0 else match.group(0)
                clean_value = re.sub(r'[^\d.,KkMm]', '', value)
                if clean_value and any(c.isdigit() for c in clean_value):
                    return f"${clean_value}"
        return ""

    def remove_duplicates(self, listings: List[Dict]) -> List[Dict]:
        """Remove duplicates based on URL"""
        unique_listings = []
        seen_urls = set()
        
        for listing in listings:
            url = listing.get('url', '').lower().strip()
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_listings.append(listing)
        
        return unique_listings

    def export_results(self, listings: List[Dict]) -> None:
        """Export results to CSV"""
        if not listings:
            print("❌ No Amazon store listings found!")
            return
        
        # Remove duplicates
        unique_listings = self.remove_duplicates(listings)
        
        df = pd.DataFrame(unique_listings)
        column_order = ['source', 'name', 'price', 'revenue', 'profit', 'multiple', 'description', 'url']
        df = df[column_order]
        
        filename = 'bizbuysell_amazon_stores.csv'
        df.to_csv(filename, index=False, quoting=1)
        
        print(f"\n{'='*60}")
        print("🎯 BIZBUYSELL AMAZON STORES SCRAPING RESULTS")
        print(f"{'='*60}")
        print(f"✅ Total unique Amazon store listings: {len(df)}")
        print(f"📊 Listings with prices: {df['price'].str.len().gt(0).sum()}")
        print(f"📊 Listings with revenue: {df['revenue'].str.len().gt(0).sum()}")
        print(f"📊 Listings with profit: {df['profit'].str.len().gt(0).sum()}")
        print(f"💾 Data exported to: {filename}")
        
        # Show sample results
        if len(df) > 0:
            print(f"\n📋 SAMPLE AMAZON STORE LISTINGS:")
            for i, (_, row) in enumerate(df.head(5).iterrows()):
                print(f"  {i+1}. {row['name']}")
                if row['price']:
                    print(f"     💰 Price: {row['price']}")
                if row['revenue']:
                    print(f"     📈 Revenue: {row['revenue']}")
                print(f"     🔗 {row['url']}")
                print()

def main():
    """Main function"""
    scraper = BizBuySellAmazonScraper()
    listings = scraper.scrape_amazon_stores()
    scraper.export_results(listings)

if __name__ == "__main__":
    main() 