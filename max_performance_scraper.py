#!/usr/bin/env python3

import os
import requests
import pandas as pd
import re
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MaxPerformanceBusinessScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.scraped_data = []
        self.processed_urls = set()
        self.stats = {
            'success': 0,
            'rate_limited': 0,
            'errors': 0,
            'server_errors': 0
        }
        self.lock = threading.Lock()
        
        # MAX PERFORMANCE: Focus on proven working sources
        self.urls = self.generate_max_performance_urls()

    def generate_max_performance_urls(self) -> List[str]:
        """Generate maximum performance URL list focusing on proven working sources"""
        urls = []
        
        # FOCUS: BizQuest (PROVEN 98.3% success rate, 60+ listings per page)
        # Expand BizQuest coverage massively since it works perfectly
        for page in range(1, 201):  # 200 pages (up from 50)
            urls.append(f"https://www.bizquest.com/business-for-sale/page-{page}/")
        
        # BizQuest categories (proven working - expand coverage)
        bizquest_categories = [
            "ecommerce-business-for-sale",
            "amazon-business-for-sale", 
            "online-business-for-sale",
            "technology-business-for-sale",
            "retail-business-for-sale",
            "automotive-business-for-sale",
            "restaurant-business-for-sale",
            "manufacturing-business-for-sale"
        ]
        
        for category in bizquest_categories:
            for page in range(1, 51):  # 50 pages each category
                urls.append(f"https://www.bizquest.com/{category}/page-{page}/")
        
        # PROVEN: QuietLight (working URLs only)
        proven_quietlight_urls = [
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/saas-businesses-for-sale/", 
            "https://quietlight.com/ecommerce-businesses-for-sale/",
        ]
        urls.extend(proven_quietlight_urls)
        
        # PROVEN: Other working sources (from successful tests)
        working_sources = [
            "https://websiteproperties.com/amazon-fba-business-for-sale/",
            "https://investors.club/tech-stack/amazon-fba/",
            "https://flippa.com/buy/monetization/amazon-fba",
        ]
        urls.extend(working_sources)
        
        # Additional BizQuest location-based searches (high success rate)
        major_locations = [
            "california-ca", "texas-tx", "florida-fl", "new-york-ny", 
            "illinois-il", "pennsylvania-pa", "ohio-oh", "georgia-ga",
            "north-carolina-nc", "michigan-mi"
        ]
        
        for location in major_locations:
            for page in range(1, 11):  # 10 pages per major location
                urls.append(f"https://www.bizquest.com/businesses-for-sale-in-{location}/page-{page}/")
        
        logger.info(f"Generated {len(urls)} URLs for MAXIMUM PERFORMANCE scraping")
        return urls

    def make_max_performance_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """MAX PERFORMANCE: Optimized requests using full 20-thread capacity"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
            'session_number': random.randint(1, 10),  # Session rotation
        }
        
        for attempt in range(retries):
            try:
                # OPTIMIZED: Faster delays for 20-thread capacity
                time.sleep(random.uniform(1, 3))  # Reduced delays for higher throughput
                
                response = self.session.get(self.base_url, params=params, timeout=90)
                
                if response.status_code == 200:
                    with self.lock:
                        self.stats['success'] += 1
                    return response
                elif response.status_code == 429:
                    # Smart rate limit handling
                    with self.lock:
                        self.stats['rate_limited'] += 1
                    if attempt < retries - 1:
                        backoff = min(30, (2 ** attempt) * 5)  # Cap at 30 seconds
                        logger.warning(f"Rate limited, backing off {backoff}s")
                        time.sleep(backoff)
                        continue
                elif response.status_code in [500, 502, 503]:
                    with self.lock:
                        self.stats['server_errors'] += 1
                    if attempt < retries - 1:
                        time.sleep(random.uniform(2, 5))
                        continue
                else:
                    response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.debug(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(2, 4))
                else:
                    with self.lock:
                        self.stats['errors'] += 1
        return None

    def extract_financial_fast(self, text: str, patterns: List[str]) -> str:
        """Fast financial extraction optimized for performance"""
        if not text:
            return ""
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                match = matches[0]
                if isinstance(match, tuple):
                    match = next((m for m in match if m and m.strip()), match[-1])
                
                clean_match = re.sub(r'[^\d.,KkMm]', '', str(match))
                if clean_match and any(c.isdigit() for c in clean_match):
                    try:
                        multiplier = 1
                        if clean_match.upper().endswith('K'):
                            multiplier = 1000
                            clean_match = clean_match[:-1]
                        elif clean_match.upper().endswith('M'):
                            multiplier = 1000000
                            clean_match = clean_match[:-1]
                        
                        value = float(clean_match.replace(',', '')) * multiplier
                        if 1000 <= value <= 500000000:  # Broader range
                            return f"${value:,.0f}"
                    except:
                        continue
        return ""

    def extract_price_fast(self, text: str) -> str:
        """Fast price extraction"""
        patterns = [
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
            r'save\s*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'listed\s*(?:at|for)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_fast(text, patterns)

    def extract_revenue_fast(self, text: str) -> str:
        """Fast revenue extraction"""
        patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'gross[:\s]*(?:sales|revenue)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'annual\s*(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'income[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_fast(text, patterns)

    def extract_profit_fast(self, text: str) -> str:
        """Fast profit extraction"""
        patterns = [
            r'cash\s*flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sde[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'net\s*(?:income|profit)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_fast(text, patterns)

    def scrape_bizquest_max_performance(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """MAX PERFORMANCE: BizQuest scraper (proven 98.3% success rate)"""
        listings = []
        
        # Use proven selector from successful tests
        business_cards = soup.select('div[class*="listing"]')
        
        if not business_cards:
            # Backup selectors
            for selector in ['div.result-item', 'div.listing-item', 'article.business']:
                business_cards = soup.select(selector)
                if business_cards:
                    break
        
        logger.info(f"BizQuest: Found {len(business_cards)} cards")
        
        for card in business_cards:
            try:
                # Fast title extraction
                title_elem = (
                    card.select_one('h2 a') or
                    card.select_one('h3 a') or
                    card.select_one('h2') or
                    card.select_one('h3') or
                    card.find('a', href=True)
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 5:
                    continue

                # Fast URL extraction
                if title_elem and title_elem.name == 'a':
                    listing_url = urljoin(base_url, title_elem.get('href', ''))
                else:
                    link_elem = card.find('a', href=True)
                    listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Fast financial extraction
                full_text = card.get_text()
                price = self.extract_price_fast(full_text)
                revenue = self.extract_revenue_fast(full_text)
                profit = self.extract_profit_fast(full_text)
                
                # Quick description
                description = re.sub(r'\s+', ' ', full_text[:200]).strip()
                
                listing = {
                    'source': 'Bizquest',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                }
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing BizQuest listing: {e}")
                
        return listings

    def scrape_generic_max_performance(self, soup: BeautifulSoup, base_url: str, domain: str) -> List[Dict]:
        """MAX PERFORMANCE: Generic scraper for other sources"""
        listings = []
        source_name = domain.replace('www.', '').split('.')[0].title()
        
        # Fast selector strategy
        selectors = ['div.listing', 'div.business', 'div[class*="listing"]', 'article', 'div.card']
        containers = []
        
        for selector in selectors:
            containers = soup.select(selector)
            if containers and len(containers) >= 3:
                break
        
        logger.info(f"{source_name}: Found {len(containers)} containers")
        
        for container in containers[:20]:  # Process more for max performance
            try:
                title_elem = container.find(['h1', 'h2', 'h3', 'h4'])
                title = title_elem.get_text().strip() if title_elem else ""

                if not title or len(title) < 5:
                    continue

                link_elem = container.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = container.get_text()
                price = self.extract_price_fast(full_text)
                revenue = self.extract_revenue_fast(full_text)
                profit = self.extract_profit_fast(full_text)
                
                listing = {
                    'source': source_name,
                    'name': title,
                    'price': price,
                    'revenue': revenue, 
                    'profit': profit,
                    'description': re.sub(r'\s+', ' ', full_text[:200]).strip(),
                    'url': listing_url,
                }
                
                if title and (price or revenue or len(full_text) > 30):
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing {source_name} listing: {e}")
        
        return listings

    def scrape_url_max_performance(self, url: str) -> List[Dict]:
        """MAX PERFORMANCE: URL scraping with smart routing"""
        if url in self.processed_urls:
            return []
        
        self.processed_urls.add(url)
        
        response = self.make_max_performance_request(url)
        if not response:
            return []

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            domain = urlparse(url).netloc.lower()
            
            # Fast routing based on proven performance
            if 'bizquest' in domain:
                return self.scrape_bizquest_max_performance(soup, url)
            else:
                return self.scrape_generic_max_performance(soup, url, domain)
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []

    def print_max_performance_stats(self):
        """Print comprehensive performance statistics"""
        total = sum(self.stats.values())
        if total == 0:
            return
            
        print(f"\n🚀 MAX PERFORMANCE STATS:")
        print(f"  ✅ Successful: {self.stats['success']} ({self.stats['success']/total*100:.1f}%)")
        print(f"  ⏱️  Rate limited: {self.stats['rate_limited']} ({self.stats['rate_limited']/total*100:.1f}%)")
        print(f"  🔥 Server errors: {self.stats['server_errors']} ({self.stats['server_errors']/total*100:.1f}%)")
        print(f"  ❌ Other errors: {self.stats['errors']} ({self.stats['errors']/total*100:.1f}%)")
        print(f"  📊 Listings harvested: {len(self.scraped_data)}")
        print(f"  📈 Avg listings per successful request: {len(self.scraped_data)/max(1, self.stats['success']):.1f}")

    def scrape_all_max_performance(self) -> None:
        """MAX PERFORMANCE: Use full 20-thread ScraperAPI capacity"""
        logger.info(f"Starting MAX PERFORMANCE scraping of {len(self.urls)} URLs")
        logger.info("Using FULL 20-thread ScraperAPI capacity!")
        
        # MAX PERFORMANCE: Use full capacity
        batch_size = 40    # Large batches for efficiency
        max_workers = 20   # FULL ScraperAPI capacity
        
        batches = [self.urls[i:i + batch_size] for i in range(0, len(self.urls), batch_size)]
        
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} URLs)")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {executor.submit(self.scrape_url_max_performance, url): url for url in batch}
                
                completed = 0
                for future in as_completed(future_to_url):
                    completed += 1
                    url = future_to_url[future]
                    try:
                        listings = future.result()
                        if listings:
                            with self.lock:
                                self.scraped_data.extend(listings)
                                logger.info(f"Added {len(listings)} listings from {url}")
                    except Exception as e:
                        logger.error(f"Error processing {url}: {e}")
                    
                    # Progress update every 10 completions
                    if completed % 10 == 0:
                        logger.info(f"Batch progress: {completed}/{len(batch)} URLs completed")
            
            # Performance stats every 5 batches
            if batch_num % 5 == 0:
                self.print_max_performance_stats()
            
            # Strategic delay between batches
            time.sleep(random.uniform(2, 5))
        
        logger.info(f"MAX PERFORMANCE scraping completed! Total raw listings: {len(self.scraped_data)}")

    def remove_duplicates_max_performance(self) -> List[Dict]:
        """MAX PERFORMANCE: Fast deduplication optimized for large datasets"""
        if not self.scraped_data:
            return []

        df = pd.DataFrame(self.scraped_data)
        logger.info(f"Starting MAX PERFORMANCE deduplication with {len(df)} raw listings")
        
        # Fast multi-step deduplication
        initial_count = len(df)
        
        # Remove exact URL duplicates
        df = df.drop_duplicates(subset=['url'], keep='first')
        logger.info(f"After URL dedup: {len(df)} ({initial_count - len(df)} removed)")
        
        # Remove short/invalid titles
        df = df[df['name'].str.len() >= 8]
        logger.info(f"After title filter: {len(df)} listings")
        
        # Remove name duplicates
        df['name_normalized'] = df['name'].str.lower().str[:40]
        df = df.drop_duplicates(subset=['name_normalized'], keep='first')
        df = df.drop(columns=['name_normalized'])
        logger.info(f"After name dedup: {len(df)} unique listings")
        
        return df.to_dict('records')

    def export_max_performance_results(self, filename: str = 'MAX_PERFORMANCE_BUSINESS_LISTINGS.csv') -> None:
        """Export maximum performance results"""
        if not self.scraped_data:
            logger.warning("No data to export")
            return

        clean_data = self.remove_duplicates_max_performance()
        df = pd.DataFrame(clean_data)
        df.to_csv(filename, index=False)
        
        # Print comprehensive results
        print("\n" + "="*80)
        print("🚀 MAXIMUM PERFORMANCE SCRAPING RESULTS")
        print("="*80)
        
        print(f"\n📊 MAXIMUM PERFORMANCE HARVEST:")
        print(f"Total unique businesses: {len(df):,}")
        
        # Final performance stats
        self.print_max_performance_stats()
        
        # Source breakdown
        source_counts = df['source'].value_counts()
        print(f"\n📈 SOURCE BREAKDOWN:")
        for source, count in source_counts.items():
            percentage = (count / len(df)) * 100
            print(f"  {source}: {count:,} businesses ({percentage:.1f}%)")
        
        # Financial coverage analysis
        price_coverage = df['price'].notna().sum()
        revenue_coverage = df['revenue'].notna().sum()
        profit_coverage = df['profit'].notna().sum()
        
        print(f"\n💰 FINANCIAL DATA QUALITY:")
        print(f"  Price coverage: {price_coverage:,}/{len(df):,} ({price_coverage/len(df)*100:.1f}%)")
        print(f"  Revenue coverage: {revenue_coverage:,}/{len(df):,} ({revenue_coverage/len(df)*100:.1f}%)")
        print(f"  Profit coverage: {profit_coverage:,}/{len(df):,} ({profit_coverage/len(df)*100:.1f}%)")
        
        # Investment categories - FIX: Proper type handling
        df['price_numeric'] = pd.to_numeric(df['price'].str.replace(r'[^\d.]', '', regex=True), errors='coerce')
        valid_prices = df[df['price_numeric'].notna() & (df['price_numeric'] > 0)]
        
        if len(valid_prices) > 0:
            high_value = valid_prices[valid_prices['price_numeric'] >= 1000000]
            medium_value = valid_prices[(valid_prices['price_numeric'] >= 100000) & (valid_prices['price_numeric'] < 1000000)]
            small_value = valid_prices[valid_prices['price_numeric'] < 100000]
            
            print(f"\n🎯 INVESTMENT OPPORTUNITIES:")
            print(f"  High-value (>$1M): {len(high_value):,} businesses")
            print(f"  Medium ($100K-$1M): {len(medium_value):,} businesses")  
            print(f"  Small (<$100K): {len(small_value):,} businesses")
            
            print(f"\n📈 FINANCIAL OVERVIEW:")
            print(f"  Price range: ${valid_prices['price_numeric'].min():,.0f} - ${valid_prices['price_numeric'].max():,.0f}")
            print(f"  Average price: ${valid_prices['price_numeric'].mean():,.0f}")
            print(f"  Median price: ${valid_prices['price_numeric'].median():,.0f}")
            print(f"  Total market value: ${valid_prices['price_numeric'].sum():,.0f}")
        
        logger.info(f"MAX PERFORMANCE results exported to {filename}")

def main():
    """MAX PERFORMANCE: Main execution using full ScraperAPI capacity"""
    try:
        scraper = MaxPerformanceBusinessScraper()
        
        start_time = time.time()
        scraper.scrape_all_max_performance()
        end_time = time.time()
        
        scraper.export_max_performance_results()
        
        duration = end_time - start_time
        logger.info(f"MAX PERFORMANCE scraping completed in {duration:.2f} seconds!")
        logger.info(f"Throughput: {len(scraper.scraped_data)/duration:.2f} listings per second")
        
    except Exception as e:
        logger.error(f"Error in max performance execution: {e}")
        raise

if __name__ == "__main__":
    main() 