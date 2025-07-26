#!/usr/bin/env python3
"""
BizBuySell Focused Scraper
Dedicated scraper to maximize BizBuySell listing extraction
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import time
import re
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BizBuySellFocusedScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.all_listings = []
        self.lock = threading.Lock()

    def make_request(self, url: str, use_render: bool = True, retries: int = 3) -> Optional[requests.Response]:
        """Make request with flexible rendering options"""
        params = {
            'api_key': self.api_key,
            'url': url,
        }
        
        if use_render:
            params['render'] = 'true'
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching {url} ({'render' if use_render else 'no-render'}, attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=90)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        return None

    def analyze_bizbuysell_structure(self) -> None:
        """Analyze BizBuySell site structure to find best URLs and selectors"""
        logger.info("🔍 ANALYZING BIZBUYSELL SITE STRUCTURE...")
        
        # Test main category pages
        test_urls = [
            "https://www.bizbuysell.com/",
            "https://www.bizbuysell.com/businesses-for-sale/",
            "https://www.bizbuysell.com/amazon-stores-for-sale/",
            "https://www.bizbuysell.com/internet-businesses-for-sale/",
            "https://www.bizbuysell.com/retail-businesses-for-sale/",
            "https://www.bizbuysell.com/ecommerce/",
            "https://www.bizbuysell.com/search/?q=amazon",
            "https://www.bizbuysell.com/search/?q=ecommerce",
            "https://www.bizbuysell.com/search/?q=fba",
        ]
        
        for url in test_urls:
            logger.info(f"\n{'='*20} TESTING {url} {'='*20}")
            
            # Try both render and no-render
            for use_render in [False, True]:
                response = self.make_request(url, use_render=use_render, retries=1)
                if response:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Test different potential selectors
                    selectors_to_test = [
                        'div.result-item',
                        'div.listing-item', 
                        'div.business-item',
                        'article.business',
                        'div[class*="listing"]',
                        'div[class*="business"]',
                        'div[class*="result"]',
                        'div[data-*="listing"]',
                        '.business-card',
                        '.listing-card',
                        'div.search-result',
                        'div.business-listing'
                    ]
                    
                    found_selectors = []
                    for selector in selectors_to_test:
                        elements = soup.select(selector)
                        if elements:
                            found_selectors.append((selector, len(elements)))
                    
                    if found_selectors:
                        logger.info(f"  {'Render' if use_render else 'No-render'}: Found selectors:")
                        for selector, count in found_selectors:
                            logger.info(f"    {selector}: {count} elements")
                        
                        # Test first selector with most results
                        best_selector, best_count = max(found_selectors, key=lambda x: x[1])
                        sample_elements = soup.select(best_selector)[:3]
                        
                        for i, elem in enumerate(sample_elements):
                            text_preview = elem.get_text()[:100].replace('\n', ' ')
                            links = elem.find_all('a', href=True)
                            logger.info(f"    Sample {i+1}: {len(text_preview)} chars, {len(links)} links")
                            logger.info(f"      Text: {text_preview}...")
                            if links:
                                logger.info(f"      Link: {links[0].get('href', '')[:60]}...")
                        break
                    else:
                        logger.info(f"  {'Render' if use_render else 'No-render'}: No recognized selectors found")
                
                time.sleep(2)

    def discover_bizbuysell_categories(self) -> List[str]:
        """Discover all BizBuySell category URLs"""
        logger.info("🔍 DISCOVERING BIZBUYSELL CATEGORIES...")
        
        category_urls = []
        
        # Start with main page
        response = self.make_request("https://www.bizbuysell.com/businesses-for-sale/", use_render=False)
        if response:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for category links
            category_patterns = [
                r'/[^/]+-businesses-for-sale/',
                r'/[^/]+-for-sale/',
                r'/category/[^/]+/',
            ]
            
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                for pattern in category_patterns:
                    if re.match(pattern, href):
                        full_url = urljoin("https://www.bizbuysell.com", href)
                        if full_url not in category_urls:
                            category_urls.append(full_url)
        
        # Add known categories
        known_categories = [
            "https://www.bizbuysell.com/amazon-stores-for-sale/",
            "https://www.bizbuysell.com/internet-businesses-for-sale/",
            "https://www.bizbuysell.com/retail-businesses-for-sale/",
            "https://www.bizbuysell.com/manufacturing-businesses-for-sale/",
            "https://www.bizbuysell.com/distribution-businesses-for-sale/",
            "https://www.bizbuysell.com/wholesale-businesses-for-sale/",
            "https://www.bizbuysell.com/technology-businesses-for-sale/",
        ]
        
        for url in known_categories:
            if url not in category_urls:
                category_urls.append(url)
        
        logger.info(f"Found {len(category_urls)} category URLs to explore")
        
        # Limit to first 5 categories for focused results
        limited_urls = category_urls[:5]
        logger.info(f"Limiting to first 5 categories:")
        for url in limited_urls:
            logger.info(f"  {url}")
        
        return limited_urls

    def scrape_bizbuysell_comprehensive(self) -> List[Dict]:
        """Comprehensive BizBuySell scraping"""
        logger.info("🚀 STARTING TARGETED BIZBUYSELL SCRAPING (5 pages per category)...")
        
        # First analyze the structure
        self.analyze_bizbuysell_structure()
        
        # Discover all categories
        category_urls = self.discover_bizbuysell_categories()
        
        all_listings = []
        
        # Scrape each category
        for category_url in category_urls:
            try:
                category_listings = self.scrape_bizbuysell_category(category_url)
                all_listings.extend(category_listings)
                time.sleep(2)  # Rate limiting
            except Exception as e:
                logger.error(f"Error scraping category {category_url}: {e}")
        
        logger.info(f"Total BizBuySell listings found: {len(all_listings)}")
        return all_listings

    def scrape_bizbuysell_category(self, category_url: str) -> List[Dict]:
        """Scrape a specific BizBuySell category with pagination"""
        logger.info(f"📄 Scraping category: {category_url}")
        
        category_listings = []
        
        # Limit to first 5 pages per category for faster results
        for page in range(1, 6):
            try:
                if page == 1:
                    url = category_url
                else:
                    # Try different pagination patterns
                    pagination_patterns = [
                        f"{category_url}?page={page}",
                        f"{category_url}page/{page}/",
                        f"{category_url}?p={page}",
                        f"{category_url}?pageNum={page}",
                    ]
                    
                    url = pagination_patterns[0]  # Start with most common
                
                # Try both render and no-render
                response = None
                for use_render in [False, True]:
                    response = self.make_request(url, use_render=use_render, retries=2)
                    if response:
                        break
                
                if not response:
                    logger.warning(f"Could not fetch page {page} of {category_url}")
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Use the best selectors found from analysis
                best_selectors = [
                    'div[class*="listing"]',
                    'div[class*="business"]', 
                    'div[class*="result"]',
                    'div.listing-item',
                    'div.business-item',
                    'article.business',
                ]
                
                page_listings = []
                for selector in best_selectors:
                    elements = soup.select(selector)
                    if elements and len(elements) > 5:  # Good indicator
                        logger.info(f"  Page {page}: Found {len(elements)} listings with {selector}")
                        
                        for element in elements:
                            try:
                                listing = self.extract_bizbuysell_listing(element, category_url)
                                if listing:
                                    page_listings.append(listing)
                            except Exception as e:
                                logger.debug(f"Error extracting listing: {e}")
                        break
                
                if not page_listings:
                    logger.info(f"  Page {page}: No listings found - end of pagination")
                    break
                
                category_listings.extend(page_listings)
                logger.info(f"  Page {page}: Extracted {len(page_listings)} listings")
                
                time.sleep(1)  # Page-level rate limiting
                
            except Exception as e:
                logger.error(f"Error scraping page {page} of {category_url}: {e}")
                break
        
        logger.info(f"Category {category_url}: Total {len(category_listings)} listings")
        return category_listings

    def extract_bizbuysell_listing(self, element, base_url: str) -> Optional[Dict]:
        """Extract individual BizBuySell listing"""
        try:
            # Find the main link
            link = element.find('a', href=True)
            if not link:
                return None
            
            listing_url = urljoin(base_url, link['href'])
            
            # Extract title
            title_selectors = ['h2', 'h3', 'h4', '.title', '.business-title', '.listing-title']
            title = ""
            for selector in title_selectors:
                title_elem = element.find(selector)
                if title_elem:
                    title = title_elem.get_text().strip()
                    break
            
            if not title:
                title = link.get_text().strip()
            
            # Skip if title too short or looks like navigation
            if len(title) < 10 or any(skip in title.lower() for skip in ['page', 'next', 'previous', 'more', 'see all']):
                return None
            
            # Extract financial data
            element_text = element.get_text()
            price, revenue, profit = self.extract_financial_data(element_text)
            
            # Extract location if available
            location_elem = element.find(['span', 'div'], class_=re.compile(r'location|city|state'))
            location = location_elem.get_text().strip() if location_elem else ""
            
            # Create description
            description_parts = [location, element_text[:300]]
            description = " ".join(filter(None, description_parts)).strip()
            
            listing = {
                'source': 'BizBuySell',
                'name': self.clean_text(title),
                'price': price,
                'revenue': revenue,
                'profit': profit,
                'description': self.clean_text(description),
                'url': listing_url,
                'multiple': '',
            }
            
            return listing
            
        except Exception as e:
            logger.debug(f"Error extracting BizBuySell listing: {e}")
            return None

    def extract_financial_data(self, text: str) -> tuple:
        """Extract financial data from text"""
        price_patterns = [
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$?([\d,]+(?:\.\d+)?)[KkMm]?(?=.*(?:asking|price))',
        ]
        
        revenue_patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'gross[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        
        profit_patterns = [
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'cash\s+flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'earnings[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        
        price = self._extract_by_patterns(text, price_patterns)
        revenue = self._extract_by_patterns(text, revenue_patterns)
        profit = self._extract_by_patterns(text, profit_patterns)
        
        return price, revenue, profit

    def _extract_by_patterns(self, text: str, patterns: List[str]) -> str:
        """Extract using regex patterns"""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1) if len(match.groups()) > 0 else match.group(0)
                clean_value = re.sub(r'[^\d.,KkMm]', '', value)
                if clean_value:
                    return f"${clean_value}"
        return ""

    def clean_text(self, text: str) -> str:
        """Clean text for CSV safety"""
        if not text:
            return ""
        text = re.sub(r'["\n\r\t]', ' ', text)
        text = re.sub(r'\s+', ' ', text.strip())
        return text[:500]

    def remove_duplicates(self, listings: List[Dict]) -> List[Dict]:
        """Remove duplicates"""
        unique_listings = []
        seen_urls = set()
        
        for listing in listings:
            url = listing.get('url', '').lower().strip()
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_listings.append(listing)
        
        return unique_listings

    def export_results(self, listings: List[Dict], filename: str = 'bizbuysell_comprehensive.csv') -> None:
        """Export results"""
        if not listings:
            logger.warning("No BizBuySell listings to export")
            return
        
        # Remove duplicates
        unique_listings = self.remove_duplicates(listings)
        
        df = pd.DataFrame(unique_listings)
        column_order = ['source', 'name', 'price', 'revenue', 'profit', 'multiple', 'description', 'url']
        df = df[column_order]
        
        df.to_csv(filename, index=False, quoting=1)
        
        print(f"\n{'='*60}")
        print("🎯 BIZBUYSELL COMPREHENSIVE SCRAPING RESULTS")
        print(f"{'='*60}")
        print(f"Total unique listings: {len(df)}")
        print(f"Listings with prices: {df['price'].str.len().gt(0).sum()}")
        print(f"Listings with revenue: {df['revenue'].str.len().gt(0).sum()}")
        print(f"Listings with profit: {df['profit'].str.len().gt(0).sum()}")
        print(f"Data exported to: {filename}")
        
        # Show sample results
        if len(df) > 0:
            print(f"\n📋 SAMPLE LISTINGS:")
            for i, (_, row) in enumerate(df.head(5).iterrows()):
                print(f"  {i+1}. {row['name'][:60]}...")
                if row['price']:
                    print(f"     Price: {row['price']}")
                if row['revenue']:
                    print(f"     Revenue: {row['revenue']}")

def main():
    """Main function"""
    scraper = BizBuySellFocusedScraper()
    listings = scraper.scrape_bizbuysell_comprehensive()
    scraper.export_results(listings)

if __name__ == "__main__":
    main() 