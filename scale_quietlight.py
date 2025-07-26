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
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

class QuietLightScaler:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def fetch_page(self, url: str) -> BeautifulSoup:
        """Fetch page using proven settings"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        self.logger.info(f"Fetching {url}")
        response = self.session.get(self.base_url, params=params, timeout=90)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def is_valid_business(self, title: str) -> bool:
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

    def extract_financials(self, card) -> dict:
        """Extract financial data using enhanced patterns"""
        result = {'price': '', 'revenue': '', 'profit': ''}
        
        full_text = card.get_text()
        
        # Enhanced price patterns
        price_patterns = [
            r'Asking\s*Price[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'\$[\d,]+(?:\.\d+)?[KMB]?',
            r'Accepting\s*Offers',
            r'Under\s*Offer',
            r'Price:\s*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'\d+\s*(?:Million|million|K|Thousand)',
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                result['price'] = match.group().strip()
                break
        
        # Enhanced revenue patterns
        revenue_patterns = [
            r'Revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Gross\s*Revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Annual\s*Revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
        ]
        for pattern in revenue_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                result['revenue'] = match.group().strip()
                break
        
        # Enhanced profit patterns  
        profit_patterns = [
            r'Income[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Net\s*Income[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Cash\s*Flow[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Profit[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
        ]
        for pattern in profit_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                result['profit'] = match.group().strip()
                break
        
        return result

    def scrape_quietlight_category(self, category_url: str, max_pages: int = 10) -> list:
        """Scrape QuietLight category with dynamic pagination"""
        all_businesses = []
        current_url = category_url
        page = 1
        
        while current_url and page <= max_pages:
            try:
                soup = self.fetch_page(current_url)
                
                # Use proven selector
                cards = soup.select('div.listing-card.grid-item')
                self.logger.info(f"Page {page}: Found {len(cards)} cards")
                
                if not cards:
                    self.logger.info(f"No cards found on page {page}, stopping pagination")
                    break
                
                page_businesses = []
                
                for card in cards:
                    try:
                        # Extract title
                        title_elem = card.select_one('h1, h2, h3, a[href*="/listings/"]')
                        if title_elem:
                            title = title_elem.get_text().strip()
                        else:
                            lines = [line.strip() for line in card.get_text().split('\n') if line.strip()]
                            title = lines[0] if lines else ""
                        
                        if not self.is_valid_business(title):
                            continue
                        
                        # Extract URL
                        url_elem = card.select_one('a[href*="/listings/"]')
                        business_url = urljoin(current_url, url_elem['href']) if url_elem else current_url
                        
                        # Extract financials
                        financials = self.extract_financials(card)
                        
                        business = {
                            'name': title[:200],
                            'url': business_url,
                            'price': financials['price'],
                            'revenue': financials['revenue'],
                            'profit': financials['profit'],
                            'description': desc_elem.get_text().strip() if (desc_elem := card.select_one('p, div[class*="description"]')) else "",
                            'source': 'QuietLight',
                            'category': 'Amazon FBA'
                        }
                        
                        page_businesses.append(business)
                        
                    except Exception as e:
                        continue
                
                all_businesses.extend(page_businesses)
                self.logger.info(f"Page {page}: Extracted {len(page_businesses)} businesses (total: {len(all_businesses)})")
                
                # If fewer than 20 cards, likely last page
                if len(cards) < 20:
                    self.logger.info(f"Page {page} has only {len(cards)} cards, likely last page")
                    break
                
                # Find next page
                next_page_elem = soup.select_one('a[href*="page/"]:contains("Next")')
                current_url = urljoin(current_url, next_page_elem['href']) if next_page_elem else None
                page += 1
                
                # Small delay between pages
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error on page {page}: {e}")
                break
        
        return all_businesses

    def scale_quietlight_harvest(self):
        """Scale up QuietLight harvesting across all categories in parallel"""
        print("🚀 SCALING QUIETLIGHT HARVEST")
        print("="*60)
        
        # All QuietLight categories
        categories = [
            ('Amazon FBA', 'https://quietlight.com/amazon-fba-businesses-for-sale/', 10),
            ('Ecommerce', 'https://quietlight.com/ecommerce-businesses-for-sale/', 10),
            ('SaaS', 'https://quietlight.com/saas-businesses-for-sale/', 5),
            ('Content', 'https://quietlight.com/content-businesses-for-sale/', 5),
            ('All Listings', 'https://quietlight.com/listings/', 15)
        ]
        
        all_businesses = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_category = {executor.submit(self.scrape_quietlight_category, url, max_pages): name for name, url, max_pages in categories}
            
            for future in as_completed(future_to_category):
                name = future_to_category[future]
                try:
                    businesses = future.result()
                    all_businesses.extend(businesses)
                    print(f"✅ {name}: {len(businesses)} businesses")
                    
                    if businesses:
                        print(f"   Sample: {businesses[0]['name'][:50]}...")
                        print(f"   Price: {businesses[0]['price']}")
                except Exception as e:
                    print(f"❌ {name}: {e}")
        
        # Deduplicate by URL
        unique_businesses = []
        seen_urls = set()
        
        for business in all_businesses:
            if business['url'] not in seen_urls:
                unique_businesses.append(business)
                seen_urls.add(business['url'])
        
        # Analysis
        print(f"\n📊 QUIETLIGHT HARVEST RESULTS:")
        print(f"Total raw businesses: {len(all_businesses)}")
        print(f"Unique businesses: {len(unique_businesses)}")
        print(f"Deduplication rate: {((len(all_businesses) - len(unique_businesses)) / len(all_businesses) * 100):.1f}%")
        
        # Data quality
        with_price = sum(1 for b in unique_businesses if b['price'])
        with_revenue = sum(1 for b in unique_businesses if b['revenue'])
        with_profit = sum(1 for b in unique_businesses if b['profit'])
        
        print(f"\n💰 DATA QUALITY:")
        print(f"With prices: {with_price}/{len(unique_businesses)} ({with_price/len(unique_businesses)*100:.1f}%)")
        print(f"With revenue: {with_revenue}/{len(unique_businesses)} ({with_revenue/len(unique_businesses)*100:.1f}%)")
        print(f"With profit: {with_profit}/{len(unique_businesses)} ({with_profit/len(unique_businesses)*100:.1f}%)")
        
        # Top businesses
        print(f"\n🏆 TOP 5 HIGHEST VALUE:")
        sorted_businesses = sorted(unique_businesses, key=lambda x: self.extract_value(x['price']), reverse=True)
        for i, business in enumerate(sorted_businesses[:5], 1):
            print(f"{i}. {business['name'][:60]}...")
            print(f"   Price: {business['price']}")
            print(f"   Revenue: {business['revenue']}")
            print()
        
        # Export
        if unique_businesses:
            df = pd.DataFrame(unique_businesses)
            df.to_csv('QUIETLIGHT_SCALED_HARVEST.csv', index=False)
            print(f"💾 Exported {len(unique_businesses)} businesses to QUIETLIGHT_SCALED_HARVEST.csv")
        
        return unique_businesses

    def extract_value(self, price_str: str) -> float:
        """Extract numeric value for sorting"""
        if not price_str:
            return 0
        
        # Extract numbers and convert M/K
        match = re.search(r'[\d,]+(?:\.\d+)?', price_str)
        if not match:
            return 0
        
        value = float(match.group().replace(',', ''))
        
        if 'M' in price_str.upper() or 'million' in price_str.lower():
            value *= 1000000
        elif 'K' in price_str.upper() or 'thousand' in price_str.lower():
            value *= 1000
        
        return value

def main():
    scaler = QuietLightScaler()
    results = scaler.scale_quietlight_harvest()
    
    if len(results) > 100:
        print(f"\n🎯 MASSIVE SUCCESS! Harvested {len(results)} QuietLight businesses!")
        print("QuietLight is now fully optimized. Ready to fix other sites!")
    else:
        print(f"\n⚠️  Need more optimization. Only {len(results)} businesses found")

if __name__ == "__main__":
    main() 