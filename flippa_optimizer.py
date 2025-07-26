#!/usr/bin/env python3

import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re
from urllib.parse import urljoin
import pandas as pd
import logging
import time

load_dotenv()

class FlippaOptimizer:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def fetch_page_with_js(self, url: str) -> BeautifulSoup:
        """Fetch page WITH JavaScript rendering for Flippa"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
            'render': 'true',  # Essential for React-heavy sites like Flippa
            'wait': '3000',    # Wait 3 seconds for JS to load
        }
        
        self.logger.info(f"Fetching {url} (JS rendering + 3s wait)")
        response = self.session.get(self.base_url, params=params, timeout=120)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def fetch_page_no_js(self, url: str) -> BeautifulSoup:
        """Fetch page WITHOUT JavaScript as backup"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        self.logger.info(f"Fetching {url} (no JS)")
        response = self.session.get(self.base_url, params=params, timeout=90)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def debug_flippa_structure(self, url: str):
        """Debug Flippa page structure to find correct selectors"""
        print(f"\n🔍 DEBUGGING FLIPPA STRUCTURE: {url}")
        print("="*70)
        
        try:
            # Try both JS and no-JS approaches
            for approach in ['JS', 'No-JS']:
                print(f"\n--- {approach} APPROACH ---")
                
                if approach == 'JS':
                    soup = self.fetch_page_with_js(url)
                else:
                    soup = self.fetch_page_no_js(url)
                
                print(f"Page title: {soup.title.string if soup.title else 'No title'}")
                print(f"Page size: {len(soup.get_text())} characters")
                print(f"Total divs: {len(soup.find_all('div'))}")
                
                # Test various selectors
                selectors_to_test = [
                    'div[data-testid*="listing"]',
                    'div[data-testid*="card"]',
                    'div[class*="ListingCard"]',
                    'div[class*="auction"]',
                    'div[class*="listing"]',
                    'div[class*="card"]',
                    'article',
                    'div[class*="item"]',
                    'div[data-*]',  # Any div with data attributes
                ]
                
                print(f"\nSelector test results:")
                for selector in selectors_to_test:
                    try:
                        elements = soup.select(selector)
                        print(f"  {selector}: {len(elements)} elements")
                        
                        if elements and len(elements) > 0:
                            # Show sample content
                            sample = elements[0]
                            sample_text = sample.get_text().strip()[:100]
                            print(f"    Sample: {sample_text}...")
                    except Exception as e:
                        print(f"    Error with {selector}: {e}")
                
                # Look for business/auction keywords
                page_text = soup.get_text().lower()
                business_keywords = ['website', 'business', 'amazon', 'ecommerce', 'revenue', 'profit', 'sale']
                print(f"\nBusiness keyword analysis:")
                for keyword in business_keywords:
                    count = page_text.count(keyword)
                    print(f"  '{keyword}': {count} occurrences")
                
                # Find all divs with meaningful classes
                print(f"\nTop div classes:")
                class_counts = {}
                for div in soup.find_all('div', class_=True):
                    classes = ' '.join(div.get('class', []))
                    if classes:
                        class_counts[classes] = class_counts.get(classes, 0) + 1
                
                sorted_classes = sorted(class_counts.items(), key=lambda x: x[1], reverse=True)
                for class_name, count in sorted_classes[:10]:
                    print(f"    {class_name}: {count}")
                
                # Find all data attributes
                print(f"\nData attributes found:")
                data_attrs = set()
                for elem in soup.find_all(attrs=lambda x: x and any(attr.startswith('data-') for attr in x)):
                    for attr in elem.attrs:
                        if attr.startswith('data-'):
                            data_attrs.add(attr)
                
                for attr in sorted(data_attrs)[:10]:
                    print(f"    {attr}")
                
                print("\n" + "-"*50)
        
        except Exception as e:
            print(f"Error debugging: {e}")

    def extract_financial_data(self, element) -> dict:
        """Extract financial data from Flippa elements"""
        result = {'price': '', 'revenue': '', 'profit': ''}
        
        text = element.get_text()
        
        # Flippa price patterns
        price_patterns = [
            r'\$[\d,]+(?:\.\d+)?[KMB]?',
            r'[\d,]+(?:\.\d+)?\s*(?:USD|dollars?)',
            r'Starting\s*bid[:\s]*\$?[\d,]+',
            r'Buy\s*now[:\s]*\$?[\d,]+',
            r'Reserve[:\s]*\$?[\d,]+',
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result['price'] = match.group().strip()
                break
        
        # Revenue patterns
        revenue_patterns = [
            r'Revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Sales[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Monthly[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Annual[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result['revenue'] = match.group().strip()
                break
        
        # Profit patterns
        profit_patterns = [
            r'Profit[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Net[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Earnings[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result['profit'] = match.group().strip()
                break
        
        return result

    def is_valid_business_flippa(self, title: str) -> bool:
        """Validate if this is a real business listing on Flippa"""
        if not title or len(title.strip()) < 5:
            return False
        
        # Exclude navigation/UI elements
        ui_keywords = [
            'sign in', 'sign up', 'register', 'login', 'browse', 'search',
            'filter', 'sort', 'view all', 'see more', 'load more', 'next page'
        ]
        
        title_lower = title.lower()
        for keyword in ui_keywords:
            if keyword in title_lower:
                return False
        
        # Must have business indicators
        business_indicators = [
            'website', 'business', 'store', 'shop', 'brand', 'company',
            'amazon', 'ecommerce', 'dropship', 'affiliate', 'blog',
            'revenue', 'profit', 'income', 'traffic', 'visitors'
        ]
        
        for indicator in business_indicators:
            if indicator in title_lower:
                return True
        
        return len(title.strip()) > 20  # Longer titles likely business descriptions

    def scrape_flippa_optimized(self, url: str) -> list:
        """Optimized Flippa scraper using best practices"""
        businesses = []
        
        try:
            # Try JavaScript first (primary approach)
            try:
                soup = self.fetch_page_with_js(url)
                js_success = True
            except Exception as e:
                self.logger.warning(f"JS rendering failed: {e}, trying without JS")
                soup = self.fetch_page_no_js(url)
                js_success = False
            
            # Try multiple selector strategies
            selector_strategies = [
                # React/modern selectors
                'div[data-testid*="listing"]',
                'div[data-testid*="card"]', 
                'div[data-testid*="auction"]',
                # Class-based selectors
                'div[class*="ListingCard"]',
                'div[class*="AuctionCard"]',
                'div[class*="listing-card"]',
                'div[class*="auction-card"]',
                # Generic selectors
                'article',
                'div[class*="card"]',
                'div[class*="item"]',
            ]
            
            listing_elements = []
            successful_selector = None
            
            for selector in selector_strategies:
                elements = soup.select(selector)
                if elements and len(elements) >= 5:  # Found meaningful results
                    listing_elements = elements
                    successful_selector = selector
                    self.logger.info(f"Flippa: Using selector '{selector}' - found {len(elements)} elements")
                    break
            
            if not listing_elements:
                self.logger.warning("No listing elements found with any selector")
                return []
            
            # Process elements
            for i, element in enumerate(listing_elements[:50]):  # Limit to 50 to avoid rate limits
                try:
                    # Extract title
                    title_selectors = ['h1', 'h2', 'h3', '.title', 'a[href*="/"]']
                    title = ""
                    
                    for title_sel in title_selectors:
                        title_elem = element.select_one(title_sel)
                        if title_elem:
                            title = title_elem.get_text().strip()
                            if title and len(title) > 5:
                                break
                    
                    # Fallback to first text line
                    if not title:
                        lines = [line.strip() for line in element.get_text().split('\n') if line.strip()]
                        if lines:
                            title = lines[0]
                    
                    if not self.is_valid_business_flippa(title):
                        continue
                    
                    # Extract URL
                    url_elem = element.select_one('a[href*="/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financial data
                    financials = self.extract_financial_data(element)
                    
                    # Extract description
                    desc_elem = element.select_one('p, .description, .summary')
                    description = desc_elem.get_text().strip() if desc_elem else ""
                    
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': description[:500],
                        'source': 'Flippa',
                        'category': 'Amazon FBA'
                    }
                    
                    businesses.append(business)
                    
                    # Debug first few
                    if i < 3:
                        self.logger.info(f"Extracted: {title[:50]}...")
                        self.logger.info(f"  Price: '{financials['price']}'")
                        self.logger.info(f"  URL: {business_url}")
                    
                except Exception as e:
                    self.logger.warning(f"Error processing element {i}: {e}")
                    continue
            
            self.logger.info(f"Flippa: Successfully extracted {len(businesses)} businesses using {'JS' if js_success else 'No-JS'}")
            
        except Exception as e:
            self.logger.error(f"Error scraping Flippa {url}: {e}")
        
        return businesses

    def test_flippa_optimization(self):
        """Test Flippa optimization across multiple URLs"""
        print("🚀 TESTING FLIPPA OPTIMIZATION")
        print("="*60)
        
        # Test URLs
        test_urls = [
            'https://flippa.com/buy/monetization/amazon-fba',
            'https://flippa.com/buy/monetization/ecommerce',
            'https://flippa.com/buy/type/website',
        ]
        
        all_businesses = []
        
        for url in test_urls:
            try:
                print(f"\n🔍 First, debugging structure for: {url}")
                self.debug_flippa_structure(url)
                
                print(f"\n📊 Now scraping: {url}")
                businesses = self.scrape_flippa_optimized(url)
                all_businesses.extend(businesses)
                
                print(f"✅ {url}: {len(businesses)} businesses")
                if businesses:
                    print(f"   Sample: {businesses[0]['name'][:50]}...")
                    print(f"   Price: {businesses[0]['price']}")
                
                # Delay between requests
                time.sleep(3)
                
            except Exception as e:
                print(f"❌ {url}: {e}")
        
        # Deduplicate
        unique_businesses = []
        seen_urls = set()
        
        for business in all_businesses:
            if business['url'] not in seen_urls:
                unique_businesses.append(business)
                seen_urls.add(business['url'])
        
        # Results
        print(f"\n📊 FLIPPA OPTIMIZATION RESULTS:")
        print(f"Total unique businesses: {len(unique_businesses)}")
        print(f"With prices: {sum(1 for b in unique_businesses if b['price'])}")
        print(f"With revenue: {sum(1 for b in unique_businesses if b['revenue'])}")
        print(f"With profit: {sum(1 for b in unique_businesses if b['profit'])}")
        
        if unique_businesses:
            df = pd.DataFrame(unique_businesses)
            df.to_csv('FLIPPA_OPTIMIZED_RESULTS.csv', index=False)
            print(f"💾 Exported to FLIPPA_OPTIMIZED_RESULTS.csv")
        
        return unique_businesses

def main():
    optimizer = FlippaOptimizer()
    results = optimizer.test_flippa_optimization()
    
    if len(results) > 10:
        print(f"\n🎯 SUCCESS! Flippa optimization working with {len(results)} businesses!")
    else:
        print(f"\n⚠️  Flippa still needs work. Only {len(results)} businesses found")

if __name__ == "__main__":
    main() 