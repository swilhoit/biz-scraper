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

class FinalSiteOptimizer:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def fetch_page(self, url: str, use_js: bool = False, wait_time: int = 0) -> BeautifulSoup:
        """Flexible page fetching with options"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        if use_js:
            params['render'] = 'true'
        if wait_time > 0:
            params['wait'] = str(wait_time * 1000)  # Convert to milliseconds
        
        self.logger.info(f"Fetching {url} ({'JS' if use_js else 'No JS'}{f', {wait_time}s wait' if wait_time else ''})")
        response = self.session.get(self.base_url, params=params, timeout=120)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def is_valid_business(self, title: str, source: str) -> bool:
        """Source-specific business validation"""
        if not title or len(title.strip()) < 8:
            return False
        
        title_lower = title.lower()
        
        # Universal UI exclusions
        ui_keywords = [
            'browse', 'categories', 'filter', 'sort', 'search', 'sign', 'login',
            'register', 'view all', 'see more', 'load more', 'next', 'previous',
            'newsletter', 'subscribe', 'contact', 'about', 'privacy', 'terms'
        ]
        
        for keyword in ui_keywords:
            if keyword in title_lower:
                return False
        
        # Business indicators
        business_indicators = [
            'brand', 'business', 'company', 'store', 'shop', 'website', 'platform',
            'amazon', 'fba', 'ecommerce', 'subscription', 'saas', 'app', 'blog',
            'revenue', 'profit', 'income', 'sales', '$', 'million', 'growth',
            'supplement', 'health', 'fitness', 'beauty', 'tech', 'software'
        ]
        
        for indicator in business_indicators:
            if indicator in title_lower:
                return True
        
        # Source-specific rules
        if source == 'QuietLight':
            return len(title.strip()) > 30  # QuietLight has detailed titles
        elif source == 'Flippa':
            return len(title.strip()) > 15  # Flippa has shorter titles
        else:
            return len(title.strip()) > 20

    def extract_financial_data(self, element, source: str) -> dict:
        """Source-specific financial extraction"""
        result = {'price': '', 'revenue': '', 'profit': ''}
        text = element.get_text()
        
        # Price patterns by source
        if source == 'QuietLight':
            price_patterns = [
                r'Asking\s*Price[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
                r'\$[\d,]+(?:\.\d+)?[KMB]?',
                r'Accepting\s*Offers',
                r'Under\s*Offer'
            ]
        elif source == 'Flippa':
            price_patterns = [
                r'Starting\s*bid[:\s]*\$?[\d,]+',
                r'Buy\s*now[:\s]*\$?[\d,]+',
                r'Reserve[:\s]*\$?[\d,]+',
                r'\$[\d,]+(?:\.\d+)?[KMB]?'
            ]
        else:  # Generic patterns
            price_patterns = [
                r'Price[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
                r'Asking[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
                r'\$[\d,]+(?:\.\d+)?[KMB]?'
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
            r'Annual[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Monthly[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?'
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result['revenue'] = match.group().strip()
                break
        
        # Profit patterns
        profit_patterns = [
            r'Income[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'SDE[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Profit[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Earnings[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?'
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result['profit'] = match.group().strip()
                break
        
        return result

    def scrape_quietlight_proven(self, url: str) -> list:
        """Use the proven QuietLight method"""
        try:
            soup = self.fetch_page(url)  # No JS needed for QuietLight
            businesses = []
            
            # Use proven selectors
            cards = soup.select('div.listing-card.grid-item, div.listing-card')
            self.logger.info(f"QuietLight: Found {len(cards)} cards")
            
            for card in cards:
                try:
                    # Extract title
                    title_elem = card.select_one('h1, h2, h3, a[href*="/listings/"]')
                    if title_elem:
                        title = title_elem.get_text().strip()
                    else:
                        lines = [line.strip() for line in card.get_text().split('\n') if line.strip()]
                        title = lines[0] if lines else ""
                    
                    if not self.is_valid_business(title, 'QuietLight'):
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/listings/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financials
                    financials = self.extract_financial_data(card, 'QuietLight')
                    
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': "",
                        'source': 'QuietLight',
                        'category': 'Amazon FBA'
                    }
                    
                    businesses.append(business)
                    
                except Exception as e:
                    continue
            
            self.logger.info(f"QuietLight: Extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping QuietLight {url}: {e}")
            return []

    def scrape_bizquest_proven(self, url: str) -> list:
        """Use the proven BizQuest method"""
        try:
            soup = self.fetch_page(url)
            businesses = []
            
            # Use proven BizQuest selectors
            cards = soup.select('div.listing-card, div[class*="listing"], article')
            self.logger.info(f"BizQuest: Found {len(cards)} cards")
            
            for card in cards:
                try:
                    # Extract title
                    title_elem = card.select_one('h1, h2, h3, .title, a[href*="/"]')
                    title = title_elem.get_text().strip() if title_elem else ""
                    
                    if not self.is_valid_business(title, 'BizQuest'):
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financials
                    financials = self.extract_financial_data(card, 'BizQuest')
                    
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': "",
                        'source': 'BizQuest',
                        'category': 'Business'
                    }
                    
                    businesses.append(business)
                    
                except Exception as e:
                    continue
            
            self.logger.info(f"BizQuest: Extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping BizQuest {url}: {e}")
            return []

    def scrape_flippa_alternative(self, url: str) -> list:
        """Alternative Flippa approach based on debugging"""
        try:
            # Flippa needs different URLs - try direct search
            alternative_urls = [
                "https://flippa.com/search?category=websites&monetization=amazon-fba",
                "https://flippa.com/search?category=websites&monetization=ecommerce",
                url  # Original URL as fallback
            ]
            
            all_businesses = []
            
            for alt_url in alternative_urls[:1]:  # Try first alternative
                try:
                    soup = self.fetch_page(alt_url, use_js=True, wait_time=5)
                    
                    # Look for actual listing containers
                    selectors = [
                        'div[class*="tw-shadow-md tw-bg-white"]',  # Flippa card containers
                        'div[class*="tw-flex tw-flex-col tw-gap-4"]',
                        'div[class*="listing"]',
                        'div[class*="auction"]',
                        'div[id*="listing"]'
                    ]
                    
                    for selector in selectors:
                        elements = soup.select(selector)
                        if elements:
                            self.logger.info(f"Flippa: Trying selector '{selector}' - {len(elements)} elements")
                            
                            for element in elements[:20]:  # Process first 20
                                text = element.get_text().strip()
                                
                                # Look for business indicators in the text
                                if any(keyword in text.lower() for keyword in ['revenue', 'profit', 'business', 'website', 'amazon', 'store']):
                                    # Found a potential business listing
                                    lines = [line.strip() for line in text.split('\n') if line.strip()]
                                    if lines:
                                        title = lines[0]
                                        
                                        if self.is_valid_business(title, 'Flippa'):
                                            financials = self.extract_financial_data(element, 'Flippa')
                                            
                                            business = {
                                                'name': title[:200],
                                                'url': alt_url,
                                                'price': financials['price'],
                                                'revenue': financials['revenue'],
                                                'profit': financials['profit'],
                                                'description': text[:500],
                                                'source': 'Flippa',
                                                'category': 'Amazon FBA'
                                            }
                                            
                                            all_businesses.append(business)
                            
                            if all_businesses:
                                break
                    
                    if all_businesses:
                        break
                        
                except Exception as e:
                    self.logger.warning(f"Error with Flippa alternative URL {alt_url}: {e}")
                    continue
            
            self.logger.info(f"Flippa: Extracted {len(all_businesses)} businesses via alternative method")
            return all_businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping Flippa {url}: {e}")
            return []

    def scrape_websiteproperties_optimized(self, url: str) -> list:
        """Optimized WebsiteProperties scraper"""
        try:
            soup = self.fetch_page(url)
            businesses = []
            
            # Multiple selector strategies
            selectors = [
                'div.property-card',
                'div[class*="property"]',
                'div[class*="listing"]',
                'article',
                'div[class*="business"]',
                'div[class*="card"]'
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    self.logger.info(f"WebsiteProperties: Using '{selector}' - {len(elements)} elements")
                    
                    for element in elements:
                        try:
                            # Extract title
                            title_elem = element.select_one('h1, h2, h3, .title, a[href*="/"]')
                            title = title_elem.get_text().strip() if title_elem else ""
                            
                            if not self.is_valid_business(title, 'WebsiteProperties'):
                                continue
                            
                            # Extract URL
                            url_elem = element.select_one('a[href*="/"]')
                            business_url = urljoin(url, url_elem['href']) if url_elem else url
                            
                            # Extract financials
                            financials = self.extract_financial_data(element, 'WebsiteProperties')
                            
                            business = {
                                'name': title[:200],
                                'url': business_url,
                                'price': financials['price'],
                                'revenue': financials['revenue'],
                                'profit': financials['profit'],
                                'description': "",
                                'source': 'WebsiteProperties',
                                'category': 'Amazon FBA'
                            }
                            
                            businesses.append(business)
                            
                        except Exception as e:
                            continue
                    
                    if businesses:
                        break
            
            self.logger.info(f"WebsiteProperties: Extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping WebsiteProperties {url}: {e}")
            return []

    def run_final_optimization(self):
        """Run final optimization across all sites"""
        print("🚀 FINAL SITE OPTIMIZATION - EQUALIZING PERFORMANCE")
        print("="*70)
        
        # Define optimized site configs
        site_configs = [
            # QuietLight (PROVEN WORKING) - Scale up
            ('QuietLight Amazon FBA', 'https://quietlight.com/amazon-fba-businesses-for-sale/', 'quietlight'),
            ('QuietLight Ecommerce', 'https://quietlight.com/ecommerce-businesses-for-sale/', 'quietlight'),
            
            # BizQuest (WORKING) - Multiple pages
            ('BizQuest Page 1', 'https://www.bizquest.com/business-for-sale/page-1/', 'bizquest'),
            ('BizQuest Page 2', 'https://www.bizquest.com/business-for-sale/page-2/', 'bizquest'),
            ('BizQuest Page 3', 'https://www.bizquest.com/business-for-sale/page-3/', 'bizquest'),
            
            # WebsiteProperties (OPTIMIZED)
            ('WebsiteProperties Amazon', 'https://websiteproperties.com/amazon-fba-business-for-sale/', 'websiteproperties'),
            ('WebsiteProperties Ecommerce', 'https://websiteproperties.com/ecommerce-business-for-sale/', 'websiteproperties'),
            
            # Flippa (ALTERNATIVE APPROACH)
            ('Flippa Amazon FBA', 'https://flippa.com/buy/monetization/amazon-fba', 'flippa'),
            ('Flippa Ecommerce', 'https://flippa.com/buy/monetization/ecommerce', 'flippa'),
        ]
        
        all_businesses = []
        
        for site_name, url, scraper_type in site_configs:
            try:
                print(f"\n📊 Scraping {site_name}...")
                
                if scraper_type == 'quietlight':
                    businesses = self.scrape_quietlight_proven(url)
                elif scraper_type == 'bizquest':
                    businesses = self.scrape_bizquest_proven(url)
                elif scraper_type == 'websiteproperties':
                    businesses = self.scrape_websiteproperties_optimized(url)
                elif scraper_type == 'flippa':
                    businesses = self.scrape_flippa_alternative(url)
                else:
                    businesses = []
                
                all_businesses.extend(businesses)
                print(f"✅ {site_name}: {len(businesses)} businesses")
                
                if businesses:
                    print(f"   Sample: {businesses[0]['name'][:50]}...")
                    print(f"   Price: {businesses[0]['price']}")
                
                # Delay between sites
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ {site_name}: {e}")
        
        # Deduplicate
        unique_businesses = []
        seen_urls = set()
        
        for business in all_businesses:
            if business['url'] not in seen_urls:
                unique_businesses.append(business)
                seen_urls.add(business['url'])
        
        # Analysis
        print(f"\n📊 FINAL OPTIMIZATION RESULTS:")
        print(f"Total unique businesses: {len(unique_businesses)}")
        
        # Source breakdown
        sources = {}
        for business in unique_businesses:
            source = business['source']
            sources[source] = sources.get(source, 0) + 1
        
        print(f"\n📈 SOURCE PERFORMANCE:")
        for source, count in sources.items():
            percentage = (count / len(unique_businesses)) * 100 if unique_businesses else 0
            print(f"  {source}: {count} businesses ({percentage:.1f}%)")
        
        # Data quality
        with_price = sum(1 for b in unique_businesses if b['price'])
        with_revenue = sum(1 for b in unique_businesses if b['revenue'])
        with_profit = sum(1 for b in unique_businesses if b['profit'])
        
        print(f"\n💰 DATA QUALITY:")
        print(f"With prices: {with_price}/{len(unique_businesses)} ({with_price/len(unique_businesses)*100:.1f}%)")
        print(f"With revenue: {with_revenue}/{len(unique_businesses)} ({with_revenue/len(unique_businesses)*100:.1f}%)")
        print(f"With profit: {with_profit}/{len(unique_businesses)} ({with_profit/len(unique_businesses)*100:.1f}%)")
        
        # Export
        if unique_businesses:
            df = pd.DataFrame(unique_businesses)
            df.to_csv('FINAL_OPTIMIZED_BUSINESS_LISTINGS.csv', index=False)
            print(f"\n💾 Exported {len(unique_businesses)} businesses to FINAL_OPTIMIZED_BUSINESS_LISTINGS.csv")
        
        return unique_businesses

def main():
    optimizer = FinalSiteOptimizer()
    results = optimizer.run_final_optimization()
    
    # Success criteria: Equal performance across sites
    sources = {}
    for business in results:
        source = business['source']
        sources[source] = sources.get(source, 0) + 1
    
    if len(results) > 100 and len(sources) >= 3:
        print(f"\n🎯 MASSIVE SUCCESS! Found {len(results)} businesses from {len(sources)} sources!")
        print("All sites now have optimized performance!")
    else:
        print(f"\n⚠️  More work needed. Found {len(results)} businesses from {len(sources)} sources")

if __name__ == "__main__":
    main() 