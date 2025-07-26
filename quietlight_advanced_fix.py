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

class QuietLightAdvancedFixer:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def fetch_page_simple(self, url: str) -> BeautifulSoup:
        """Fetch page WITHOUT JavaScript rendering to avoid 500 errors"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
            # NO render=true to avoid 500 errors
        }
        
        self.logger.info(f"Fetching {url} (no JS rendering)")
        response = self.session.get(self.base_url, params=params, timeout=90)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def is_valid_business_title(self, title: str) -> bool:
        """Filter out UI elements and keep only real business listings"""
        if not title or len(title.strip()) < 10:
            return False
        
        # UI elements to exclude
        ui_keywords = [
            'instant listing alerts', 'listing alerts', 'straight to your mailbox',
            'sign up', 'subscribe', 'newsletter', 'login', 'register',
            'view all', 'see more', 'load more', 'filter', 'search',
            'sort by', 'contact', 'about', 'privacy', 'terms'
        ]
        
        title_lower = title.lower()
        for keyword in ui_keywords:
            if keyword in title_lower:
                return False
        
        # Must contain business indicators
        business_indicators = [
            'brand', 'business', 'company', 'store', 'shop', 'site', 'website',
            'amazon', 'fba', 'ecommerce', 'subscription', 'saas', 'app',
            'revenue', 'profit', 'income', 'sales', '$', 'million', 'thousand'
        ]
        
        for indicator in business_indicators:
            if indicator in title_lower:
                return True
        
        # If title is long and descriptive, likely a business
        return len(title.strip()) > 30

    def extract_financial_data(self, card_soup) -> dict:
        """Extract price, revenue, profit from card using multiple strategies"""
        result = {'price': '', 'revenue': '', 'profit': ''}
        
        # Strategy 1: Look for specific CSS classes
        price_elem = card_soup.select_one('.listing-card__price')
        if price_elem:
            result['price'] = self.clean_financial_value(price_elem.get_text())
        
        revenue_elem = card_soup.select_one('.listing-card__profit-revenue')
        if revenue_elem:
            result['revenue'] = self.clean_financial_value(revenue_elem.get_text())
        
        profit_elem = card_soup.select_one('.listing-card__profit-income')
        if profit_elem:
            result['profit'] = self.clean_financial_value(profit_elem.get_text())
        
        # Strategy 2: Parse all text if specific elements failed
        full_text = card_soup.get_text()
        
        if not result['price']:
            price_match = re.search(r'Price[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?', full_text, re.IGNORECASE)
            if price_match:
                result['price'] = price_match.group()
            else:
                # Look for any dollar amount
                dollar_match = re.search(r'\$[\d,]+(?:\.\d+)?[KMB]?', full_text)
                if dollar_match:
                    result['price'] = dollar_match.group()
        
        if not result['revenue']:
            revenue_match = re.search(r'(?:Revenue|Sales)[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?', full_text, re.IGNORECASE)
            if revenue_match:
                result['revenue'] = revenue_match.group()
        
        if not result['profit']:
            profit_match = re.search(r'(?:SDE|Profit|Earnings)[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?', full_text, re.IGNORECASE)
            if profit_match:
                result['profit'] = profit_match.group()
        
        return result

    def clean_financial_value(self, text: str) -> str:
        """Clean and standardize financial values"""
        if not text:
            return ""
        
        # Remove extra whitespace and common prefixes
        text = re.sub(r'(Price|Revenue|Sales|SDE|Profit|Earnings)[:\s]*', '', text, flags=re.IGNORECASE)
        text = text.strip()
        
        # Find the financial value
        match = re.search(r'\$?[\d,]+(?:\.\d+)?[KMB]?', text, re.IGNORECASE)
        return match.group() if match else ""

    def scrape_quietlight_advanced(self, url: str) -> list:
        """Advanced QuietLight scraper with better filtering"""
        try:
            soup = self.fetch_page_simple(url)  # No JS rendering
            businesses = []
            
            # Find all potential listing containers
            all_selectors = [
                'div.listing-card.grid-item',  # Primary selector
                'div.listing-card',            # Backup selector
                'article[class*="listing"]',   # Alternative structure
                'div[class*="business-card"]'  # Another possibility
            ]
            
            listing_cards = []
            for selector in all_selectors:
                cards = soup.select(selector)
                if cards:
                    self.logger.info(f"Found {len(cards)} cards with selector: {selector}")
                    listing_cards = cards
                    break
            
            if not listing_cards:
                self.logger.warning("No listing cards found with any selector")
                return []
            
            self.logger.info(f"Processing {len(listing_cards)} listing cards")
            
            for i, card in enumerate(listing_cards):
                try:
                    # Extract title from multiple possible locations
                    title_selectors = [
                        'h1', 'h2', 'h3', '.title', '.listing-title', 
                        '.listing-card__title', 'a[href*="/listings/"]'
                    ]
                    
                    title = ""
                    for selector in title_selectors:
                        title_elem = card.select_one(selector)
                        if title_elem:
                            title = title_elem.get_text().strip()
                            if title and len(title) > 5:
                                break
                    
                    # Use card text as fallback
                    if not title:
                        card_text = card.get_text().strip()
                        # Take first line as title
                        lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                        if lines:
                            title = lines[0]
                    
                    if not self.is_valid_business_title(title):
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/listings/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financial data
                    financial_data = self.extract_financial_data(card)
                    
                    # Extract description
                    desc_elem = card.select_one('.description, .listing-description, p')
                    description = desc_elem.get_text().strip() if desc_elem else ""
                    
                    # Create business record
                    business = {
                        'name': title[:200],  # Limit title length
                        'url': business_url,
                        'price': financial_data['price'],
                        'revenue': financial_data['revenue'],
                        'profit': financial_data['profit'],
                        'description': description[:500],
                        'source': 'QuietLight',
                        'category': 'Amazon FBA'
                    }
                    
                    businesses.append(business)
                    
                    # Debug output for first few
                    if i < 3:
                        self.logger.info(f"Extracted business {i+1}: {title[:50]}...")
                        self.logger.info(f"  Price: '{financial_data['price']}'")
                        self.logger.info(f"  Revenue: '{financial_data['revenue']}'")
                        self.logger.info(f"  Profit: '{financial_data['profit']}'")
                    
                except Exception as e:
                    self.logger.warning(f"Error processing card {i+1}: {e}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping QuietLight {url}: {e}")
            return []

    def debug_page_structure(self, url: str):
        """Debug the actual page structure to understand the layout"""
        try:
            soup = self.fetch_page_simple(url)
            
            print(f"\n🔍 DEBUGGING PAGE STRUCTURE: {url}")
            print("="*70)
            
            # Find all cards and analyze their content
            cards = soup.select('div.listing-card')
            print(f"Found {len(cards)} listing cards")
            
            for i, card in enumerate(cards[:5]):  # Analyze first 5
                print(f"\n--- CARD {i+1} ---")
                print(f"Classes: {card.get('class', [])}")
                
                # Show all text content
                card_text = card.get_text()
                lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                print(f"Text lines ({len(lines)}):")
                for j, line in enumerate(lines[:10]):  # First 10 lines
                    print(f"  {j+1}: {line}")
                
                # Look for links
                links = card.find_all('a', href=True)
                print(f"Links found: {len(links)}")
                for link in links[:3]:
                    print(f"  {link.get('href')} -> {link.get_text().strip()}")
                
                # Look for financial elements
                price_elem = card.select_one('.listing-card__price')
                revenue_elem = card.select_one('.listing-card__profit-revenue')
                profit_elem = card.select_one('.listing-card__profit-income')
                
                print(f"Financial elements:")
                print(f"  Price: {price_elem.get_text().strip() if price_elem else 'NOT FOUND'}")
                print(f"  Revenue: {revenue_elem.get_text().strip() if revenue_elem else 'NOT FOUND'}")
                print(f"  Profit: {profit_elem.get_text().strip() if profit_elem else 'NOT FOUND'}")
                
        except Exception as e:
            print(f"Error debugging: {e}")

    def test_all_strategies(self):
        """Test all URLs and strategies"""
        print("🚀 TESTING ADVANCED QUIETLIGHT OPTIMIZATION")
        print("="*70)
        
        # Test the main Amazon FBA page with debugging
        main_url = "https://quietlight.com/amazon-fba-businesses-for-sale/"
        print(f"\n🔍 DEBUGGING MAIN PAGE FIRST:")
        self.debug_page_structure(main_url)
        
        # Test scraping
        test_urls = [
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/ecommerce-businesses-for-sale/",
        ]
        
        all_businesses = []
        
        for url in test_urls:
            try:
                businesses = self.scrape_quietlight_advanced(url)
                all_businesses.extend(businesses)
                print(f"✅ {url}: {len(businesses)} businesses")
                
                # Show sample
                if businesses:
                    print(f"   Sample: {businesses[0]['name'][:50]}...")
                    print(f"   Price: {businesses[0]['price']}")
                
            except Exception as e:
                print(f"❌ {url}: {e}")
        
        # Deduplicate and export
        unique_businesses = []
        seen_urls = set()
        
        for business in all_businesses:
            if business['url'] not in seen_urls:
                unique_businesses.append(business)
                seen_urls.add(business['url'])
        
        print(f"\n📊 FINAL RESULTS:")
        print(f"Total unique businesses: {len(unique_businesses)}")
        print(f"With prices: {sum(1 for b in unique_businesses if b['price'])}")
        print(f"With revenue: {sum(1 for b in unique_businesses if b['revenue'])}")
        print(f"With profit: {sum(1 for b in unique_businesses if b['profit'])}")
        
        if unique_businesses:
            df = pd.DataFrame(unique_businesses)
            df.to_csv('QUIETLIGHT_ADVANCED_RESULTS.csv', index=False)
            print(f"💾 Exported to QUIETLIGHT_ADVANCED_RESULTS.csv")
        
        return unique_businesses

def main():
    fixer = QuietLightAdvancedFixer()
    results = fixer.test_all_strategies()
    
    if len(results) > 20:
        print(f"\n🎯 SUCCESS! Found {len(results)} businesses")
    else:
        print(f"\n⚠️  Still need work. Only {len(results)} businesses found")

if __name__ == "__main__":
    main() 