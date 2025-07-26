#!/usr/bin/env python3

import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re
from urllib.parse import urljoin
import pandas as pd
import logging

load_dotenv()

class QuietLightOptimizer:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def fetch_page(self, url: str) -> BeautifulSoup:
        """Fetch page using ScraperAPI with JavaScript rendering"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
            'render': 'true',  # Enable JavaScript rendering
        }
        
        self.logger.info(f"Fetching {url}")
        response = self.session.get(self.base_url, params=params, timeout=120)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def extract_price_enhanced(self, text: str) -> str:
        """Enhanced price extraction for QuietLight"""
        if not text:
            return ""
        
        # QuietLight uses specific price patterns
        price_patterns = [
            r'\$[\d,]+(?:\.\d{2})?',  # Standard $1,000,000 format
            r'[\d,]+\s*(?:million|Million|M)',  # 1.5 Million format
            r'[\d,]+\s*(?:thousand|Thousand|K)',  # 500K format
            r'Asking\s*Price:?\s*\$?[\d,]+',  # Asking Price: format
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group().strip()
        
        return ""

    def extract_revenue_enhanced(self, text: str) -> str:
        """Enhanced revenue extraction for QuietLight"""
        if not text:
            return ""
        
        revenue_patterns = [
            r'(?:Revenue|Sales)[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'\$[\d,]+(?:\.\d+)?[KMB]?\s*(?:revenue|sales)',
            r'(?:TTM|Annual)\s*Revenue[:\s]*\$?[\d,]+',
            r'[\d,]+\s*(?:million|Million|M)\s*(?:revenue|Revenue)',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group().strip()
        
        return ""

    def extract_profit_enhanced(self, text: str) -> str:
        """Enhanced profit extraction for QuietLight"""
        if not text:
            return ""
        
        profit_patterns = [
            r'(?:SDE|Profit|Earnings)[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'\$[\d,]+(?:\.\d+)?[KMB]?\s*(?:SDE|profit|earnings)',
            r'(?:Net\s*Income|Cash\s*Flow)[:\s]*\$?[\d,]+',
            r'[\d,]+\s*(?:million|Million|M)\s*(?:SDE|profit)',
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group().strip()
        
        return ""

    def scrape_quietlight_optimized(self, url: str) -> list:
        """Optimized QuietLight scraper using correct selectors"""
        try:
            soup = self.fetch_page(url)
            businesses = []
            
            # USE CORRECT SELECTORS based on analysis
            listing_cards = soup.select('div.listing-card.grid-item')
            self.logger.info(f"QuietLight: Found {len(listing_cards)} listing cards")
            
            for card in listing_cards:
                try:
                    # Skip sold or under-offer listings initially
                    card_classes = ' '.join(card.get('class', []))
                    if 'sold' in card_classes.lower():
                        continue
                    
                    # Extract business name/title
                    title_elem = card.select_one('h3, h2, .listing-card__title, a[href*="/listings/"]')
                    title = title_elem.get_text().strip() if title_elem else ""
                    
                    if not title or len(title) < 5:  # Skip invalid titles
                        continue
                    
                    # Extract URL - look for listing links
                    url_elem = card.select_one('a[href*="/listings/"]')
                    business_url = ""
                    if url_elem:
                        business_url = urljoin(url, url_elem['href'])
                    
                    # Extract price from card
                    price_elem = card.select_one('.listing-card__price, .price')
                    price_text = price_elem.get_text().strip() if price_elem else ""
                    price = self.extract_price_enhanced(price_text)
                    
                    # Extract revenue
                    revenue_elem = card.select_one('.listing-card__profit-revenue, .revenue')
                    revenue_text = revenue_elem.get_text().strip() if revenue_elem else ""
                    revenue = self.extract_revenue_enhanced(revenue_text)
                    
                    # Extract profit/SDE
                    profit_elem = card.select_one('.listing-card__profit-income, .sde, .profit')
                    profit_text = profit_elem.get_text().strip() if profit_elem else ""
                    profit = self.extract_profit_enhanced(profit_text)
                    
                    # Extract description
                    desc_elem = card.select_one('.listing-card__description, .description, p')
                    description = desc_elem.get_text().strip() if desc_elem else ""
                    
                    # Get all text for additional data mining
                    full_text = card.get_text()
                    
                    # Backup extraction from full text if primary failed
                    if not price:
                        price = self.extract_price_enhanced(full_text)
                    if not revenue:
                        revenue = self.extract_revenue_enhanced(full_text)  
                    if not profit:
                        profit = self.extract_profit_enhanced(full_text)
                    
                    business = {
                        'name': title,
                        'url': business_url or url,
                        'price': price,
                        'revenue': revenue,
                        'profit': profit,
                        'description': description[:500],  # Limit description length
                        'source': 'QuietLight',
                        'category': 'Amazon FBA'
                    }
                    
                    businesses.append(business)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing QuietLight card: {e}")
                    continue
            
            self.logger.info(f"QuietLight: Extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping QuietLight {url}: {e}")
            return []

    def test_quietlight_pages(self):
        """Test QuietLight optimization across multiple pages"""
        print("🚀 TESTING QUIETLIGHT OPTIMIZATION")
        print("="*60)
        
        # Test multiple QuietLight categories
        test_urls = [
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/ecommerce-businesses-for-sale/",
            "https://quietlight.com/listings/?category=amazon-fba",
        ]
        
        all_businesses = []
        
        for url in test_urls:
            try:
                businesses = self.scrape_quietlight_optimized(url)
                all_businesses.extend(businesses)
                print(f"✅ {url}: {len(businesses)} businesses")
            except Exception as e:
                print(f"❌ {url}: {e}")
        
        # Remove duplicates by URL
        unique_businesses = []
        seen_urls = set()
        
        for business in all_businesses:
            if business['url'] not in seen_urls:
                unique_businesses.append(business)
                seen_urls.add(business['url'])
        
        print(f"\n📊 RESULTS:")
        print(f"Total businesses found: {len(unique_businesses)}")
        print(f"With prices: {sum(1 for b in unique_businesses if b['price'])}")
        print(f"With revenue: {sum(1 for b in unique_businesses if b['revenue'])}")
        print(f"With profit: {sum(1 for b in unique_businesses if b['profit'])}")
        
        # Show top 5 examples
        print(f"\n🏆 TOP 5 EXAMPLES:")
        for i, business in enumerate(unique_businesses[:5], 1):
            print(f"{i}. {business['name'][:50]}...")
            print(f"   Price: {business['price']}")
            print(f"   Revenue: {business['revenue']}")
            print(f"   Profit: {business['profit']}")
            print()
        
        # Export to CSV
        if unique_businesses:
            df = pd.DataFrame(unique_businesses)
            df.to_csv('QUIETLIGHT_OPTIMIZED_RESULTS.csv', index=False)
            print(f"💾 Exported {len(unique_businesses)} businesses to QUIETLIGHT_OPTIMIZED_RESULTS.csv")
        
        return unique_businesses

def main():
    optimizer = QuietLightOptimizer()
    results = optimizer.test_quietlight_pages()
    
    if len(results) > 50:
        print(f"\n🎯 SUCCESS: QuietLight optimization working! Found {len(results)} businesses")
        print("Ready to integrate into main scraper!")
    else:
        print(f"\n⚠️  Need more optimization. Only found {len(results)} businesses")

if __name__ == "__main__":
    main() 