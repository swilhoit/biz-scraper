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

class CorrectedMasterScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def fetch_page(self, url: str) -> BeautifulSoup:
        """Simple page fetching using proven settings"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        self.logger.info(f"Fetching {url}")
        response = self.session.get(self.base_url, params=params, timeout=90)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def is_valid_business_title_proven(self, title: str) -> bool:
        """PROVEN business title validation from working QuietLight method"""
        if not title or len(title.strip()) < 10:
            return False
        
        # Exclude UI elements (from working version)
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
        
        # Must contain business indicators (from working version)
        business_indicators = [
            'brand', 'business', 'company', 'store', 'shop', 'site', 'website',
            'amazon', 'fba', 'ecommerce', 'subscription', 'saas', 'app',
            'revenue', 'profit', 'income', 'sales', '$', 'million', 'thousand'
        ]
        
        for indicator in business_indicators:
            if indicator in title_lower:
                return True
        
        # If title is long and descriptive, likely a business (from working version)
        return len(title.strip()) > 30

    def extract_financial_data_proven(self, card_soup) -> dict:
        """PROVEN financial data extraction from working QuietLight method"""
        result = {'price': '', 'revenue': '', 'profit': ''}
        
        # Strategy 1: Look for specific CSS classes (QuietLight specific)
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
            profit_match = re.search(r'(?:SDE|Profit|Earnings|Income)[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?', full_text, re.IGNORECASE)
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

    def scrape_quietlight_proven_method(self, url: str) -> list:
        """EXACT proven QuietLight method that worked perfectly"""
        try:
            soup = self.fetch_page(url)
            businesses = []
            
            # Use EXACT proven selectors
            listing_cards = soup.select('div.listing-card.grid-item')
            self.logger.info(f"QuietLight: Found {len(listing_cards)} listing cards")
            
            for i, card in enumerate(listing_cards):
                try:
                    # Extract title from multiple possible locations (proven method)
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
                    
                    # Use card text as fallback (proven method)
                    if not title:
                        card_text = card.get_text().strip()
                        lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                        if lines:
                            title = lines[0]
                    
                    if not self.is_valid_business_title_proven(title):
                        continue
                    
                    # Extract URL (proven method)
                    url_elem = card.select_one('a[href*="/listings/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financial data (proven method)
                    financial_data = self.extract_financial_data_proven(card)
                    
                    # Extract description (proven method)
                    desc_elem = card.select_one('.description, .listing-description, p')
                    description = desc_elem.get_text().strip() if desc_elem else ""
                    
                    # Create business record (proven format)
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': financial_data['price'],
                        'revenue': financial_data['revenue'],
                        'profit': financial_data['profit'],
                        'description': description[:500],
                        'source': 'QuietLight',
                        'category': 'Amazon FBA'
                    }
                    
                    businesses.append(business)
                    
                    # Debug output for first few (proven method)
                    if i < 3:
                        self.logger.info(f"Extracted business {i+1}: {title[:50]}...")
                        self.logger.info(f"  Price: '{financial_data['price']}'")
                        self.logger.info(f"  Revenue: '{financial_data['revenue']}'")
                        self.logger.info(f"  Profit: '{financial_data['profit']}'")
                    
                except Exception as e:
                    self.logger.warning(f"Error processing card {i+1}: {e}")
                    continue
            
            self.logger.info(f"QuietLight: Successfully extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping QuietLight {url}: {e}")
            return []

    def scrape_bizquest_optimized(self, url: str) -> list:
        """Optimized BizQuest scraper (already working well)"""
        try:
            soup = self.fetch_page(url)
            businesses = []
            
            # BizQuest proven selectors
            cards = soup.select('div.listing-card, div[class*="listing"], div.business-card')
            self.logger.info(f"BizQuest: Found {len(cards)} cards")
            
            for card in cards:
                try:
                    # Extract title
                    title_elem = card.select_one('h1, h2, h3, .title, a[href*="/"]')
                    title = title_elem.get_text().strip() if title_elem else ""
                    
                    if len(title) < 10:  # Simple validation for BizQuest
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract price (BizQuest specific)
                    price_text = card.get_text()
                    price_match = re.search(r'\$[\d,]+(?:\.\d+)?[KMB]?', price_text)
                    price = price_match.group() if price_match else ""
                    
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': price,
                        'revenue': "",  # BizQuest doesn't typically show revenue
                        'profit': "",   # BizQuest doesn't typically show profit
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

    def scrape_websiteproperties_optimized(self, url: str) -> list:
        """Optimized WebsiteProperties scraper"""
        try:
            soup = self.fetch_page(url)
            businesses = []
            
            # WebsiteProperties selectors
            cards = soup.select('div[class*="listing"], div[class*="property"], div[class*="business"]')
            self.logger.info(f"WebsiteProperties: Found {len(cards)} cards")
            
            for card in cards:
                try:
                    # Extract title
                    title_elem = card.select_one('h1, h2, h3, .title, a[href*="/"]')
                    title = title_elem.get_text().strip() if title_elem else ""
                    
                    if len(title) < 15:  # WebsiteProperties validation
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financial data
                    card_text = card.get_text()
                    price_match = re.search(r'\$[\d,]+(?:\.\d+)?[KMB]?', card_text)
                    revenue_match = re.search(r'Revenue[:\s]*\$?[\d,]+', card_text, re.IGNORECASE)
                    
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': price_match.group() if price_match else "",
                        'revenue': revenue_match.group() if revenue_match else "",
                        'profit': "",
                        'description': "",
                        'source': 'WebsiteProperties',
                        'category': 'Amazon FBA'
                    }
                    
                    businesses.append(business)
                    
                except Exception as e:
                    continue
            
            self.logger.info(f"WebsiteProperties: Extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping WebsiteProperties {url}: {e}")
            return []

    def run_corrected_master_harvest(self):
        """Run corrected master harvest with proven methods"""
        print("🚀 CORRECTED MASTER SCRAPER - USING PROVEN METHODS")
        print("="*70)
        
        # Define site configs using proven working methods
        site_configs = [
            # QuietLight (PROVEN 100% WORKING) - Use exact working method
            ('QuietLight Amazon FBA', 'https://quietlight.com/amazon-fba-businesses-for-sale/', 'quietlight'),
            ('QuietLight Ecommerce', 'https://quietlight.com/ecommerce-businesses-for-sale/', 'quietlight'),
            
            # BizQuest (PROVEN WORKING) - Multiple pages
            ('BizQuest Page 1', 'https://www.bizquest.com/business-for-sale/page-1/', 'bizquest'),
            ('BizQuest Page 2', 'https://www.bizquest.com/business-for-sale/page-2/', 'bizquest'),
            ('BizQuest Page 3', 'https://www.bizquest.com/business-for-sale/page-3/', 'bizquest'),
            ('BizQuest Page 4', 'https://www.bizquest.com/business-for-sale/page-4/', 'bizquest'),
            ('BizQuest Page 5', 'https://www.bizquest.com/business-for-sale/page-5/', 'bizquest'),
            
            # WebsiteProperties (OPTIMIZED)
            ('WebsiteProperties Amazon', 'https://websiteproperties.com/amazon-fba-business-for-sale/', 'websiteproperties'),
        ]
        
        all_businesses = []
        
        for site_name, url, scraper_type in site_configs:
            try:
                print(f"\n📊 Scraping {site_name}...")
                
                if scraper_type == 'quietlight':
                    businesses = self.scrape_quietlight_proven_method(url)
                elif scraper_type == 'bizquest':
                    businesses = self.scrape_bizquest_optimized(url)
                elif scraper_type == 'websiteproperties':
                    businesses = self.scrape_websiteproperties_optimized(url)
                else:
                    businesses = []
                
                all_businesses.extend(businesses)
                print(f"✅ {site_name}: {len(businesses)} businesses")
                
                if businesses:
                    print(f"   Sample: {businesses[0]['name'][:50]}...")
                    print(f"   Price: {businesses[0]['price']}")
                    if businesses[0]['revenue']:
                        print(f"   Revenue: {businesses[0]['revenue']}")
                
                # Delay between sites
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ {site_name}: {e}")
        
        # SMART Deduplication (not over-aggressive)
        unique_businesses = []
        seen_combinations = set()
        
        for business in all_businesses:
            # Use combination of URL and name for deduplication
            dedup_key = f"{business['url']}|{business['name'][:50]}"
            
            if dedup_key not in seen_combinations:
                unique_businesses.append(business)
                seen_combinations.add(dedup_key)
        
        # Analysis
        print(f"\n📊 CORRECTED MASTER RESULTS:")
        print(f"Total raw businesses: {len(all_businesses)}")
        print(f"Total unique businesses: {len(unique_businesses)}")
        print(f"Deduplication rate: {((len(all_businesses) - len(unique_businesses)) / len(all_businesses) * 100):.1f}%")
        
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
            df.to_csv('CORRECTED_MASTER_BUSINESS_LISTINGS.csv', index=False)
            print(f"\n💾 Exported {len(unique_businesses)} businesses to CORRECTED_MASTER_BUSINESS_LISTINGS.csv")
        
        return unique_businesses

def main():
    scraper = CorrectedMasterScraper()
    results = scraper.run_corrected_master_harvest()
    
    # Check for success
    sources = {}
    for business in results:
        source = business['source']
        sources[source] = sources.get(source, 0) + 1
    
    quietlight_count = sources.get('QuietLight', 0)
    
    if quietlight_count >= 30:  # Should get ~40+ QuietLight businesses
        print(f"\n🎯 SUCCESS! QuietLight is back with {quietlight_count} businesses!")
        print(f"Total: {len(results)} businesses from {len(sources)} sources")
    else:
        print(f"\n⚠️  QuietLight still underperforming: only {quietlight_count} businesses")

if __name__ == "__main__":
    main() 