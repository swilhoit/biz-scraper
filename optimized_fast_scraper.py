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

class OptimizedFastBusinessScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.scraped_data = []
        self.processed_urls = set()
        self.failed_domains = set()  # Track failing domains to skip
        self.lock = threading.Lock()
        
        # OPTIMIZED: Focus on working sources only
        self.urls = self.generate_optimized_urls()

    def generate_optimized_urls(self) -> List[str]:
        """Generate optimized URL list focusing on working sources"""
        urls = []
        
        # PRIORITY 1: BizQuest (Working excellently - 60+ listings per page)
        # Expand BizQuest coverage - it's our most reliable source
        for page in range(1, 101):  # 100 pages - fast and reliable
            urls.append(f"https://www.bizquest.com/business-for-sale/page-{page}/")
        
        # BizQuest categories that work well
        bizquest_categories = [
            "ecommerce-business-for-sale",
            "amazon-business-for-sale", 
            "online-business-for-sale",
            "technology-business-for-sale",
            "retail-business-for-sale"
        ]
        
        for category in bizquest_categories:
            for page in range(1, 31):  # 30 pages each category
                urls.append(f"https://www.bizquest.com/{category}/page-{page}/")
        
        # PRIORITY 2: QuietLight (Premium but working)
        quietlight_urls = [
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/saas-businesses-for-sale/",
            "https://quietlight.com/content-businesses-for-sale/",
            "https://quietlight.com/ecommerce-businesses-for-sale/",
        ]
        urls.extend(quietlight_urls)
        
        # PRIORITY 3: Working alternative sources (small test first)
        working_alternatives = [
            "https://flippa.com/buy/monetization/amazon-fba",
            "https://flippa.com/buy/monetization/ecommerce", 
            "https://websiteproperties.com/amazon-fba-business-for-sale/",
            "https://investors.club/tech-stack/amazon-fba/",
            "https://investors.club/tech-stack/ecommerce/",
        ]
        urls.extend(working_alternatives)
        
        # SKIP: BizBuySell entirely (all returning 500 errors)
        # SKIP: EmpireFlippers (all "Unlock Listing" placeholders)
        
        logger.info(f"Generated {len(urls)} optimized URLs (focusing on working sources)")
        return urls

    def make_fast_request(self, url: str, retries: int = 2) -> Optional[requests.Response]:
        """OPTIMIZED: Faster requests with smart error handling"""
        # Skip if domain is known to be failing
        domain = urlparse(url).netloc
        if domain in self.failed_domains:
            logger.debug(f"Skipping {url} - domain marked as failing")
            return None
        
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        for attempt in range(retries):
            try:
                # Faster delays for working sources
                time.sleep(random.uniform(0.5, 2.0))  # Reduced from 2-5 seconds
                
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=60)
                
                if response.status_code == 200:
                    return response
                elif response.status_code in [500, 503, 502]:
                    # Mark domain as failing and skip
                    self.failed_domains.add(domain)
                    logger.warning(f"Server error for {domain}, marking as failing")
                    return None
                else:
                    response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(1, 2))  # Faster retries
                else:
                    # Mark domain as failing after retries exhausted
                    self.failed_domains.add(domain)
                    logger.error(f"Failed to fetch {url}, marking domain as failing")
        return None

    def extract_financial_fast(self, text: str, patterns: List[str]) -> str:
        """OPTIMIZED: Faster financial extraction"""
        if not text:
            return ""
        
        # Use first successful pattern only for speed
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                match = matches[0]
                if isinstance(match, tuple):
                    match = next((m for m in match if m and m.strip()), match[-1])
                
                # Quick clean and validate
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
                        if 5000 <= value <= 200000000:
                            return f"${value:,.0f}"
                    except:
                        continue
        return ""

    def extract_price_fast(self, text: str) -> str:
        """OPTIMIZED: Fast price extraction with most common patterns"""
        patterns = [
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
            r'save\s*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_fast(text, patterns)

    def extract_revenue_fast(self, text: str) -> str:
        """OPTIMIZED: Fast revenue extraction"""
        patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'gross[:\s]*(?:sales|revenue)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'annual\s*(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_fast(text, patterns)

    def extract_profit_fast(self, text: str) -> str:
        """OPTIMIZED: Fast profit extraction"""
        patterns = [
            r'cash\s*flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sde[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_fast(text, patterns)

    def scrape_bizquest_fast(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """OPTIMIZED: Fast BizQuest scraper (our best performing source)"""
        listings = []
        
        # BizQuest proven selector (from logs: finding 65 cards consistently)
        business_cards = soup.select('div[class*="listing"]')
        
        if not business_cards:
            # Fallback selectors
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
                if not title or len(title) < 10:
                    continue

                # Fast URL extraction
                if title_elem.name == 'a':
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
                description = re.sub(r'\s+', ' ', full_text[:300]).strip()
                
                listing = {
                    'source': 'Bizquest',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing BizQuest listing: {e}")
                
        return listings

    def scrape_quietlight_fast(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """OPTIMIZED: Fast QuietLight scraper"""
        listings = []
        
        # QuietLight selectors
        business_cards = []
        for selector in ['div.listing-item', 'article.post', 'div[class*="business"]']:
            cards = soup.select(selector)
            if cards and len(cards) >= 5:
                business_cards = cards
                break
        
        logger.info(f"QuietLight: Found {len(business_cards)} cards")
        
        for card in business_cards[:20]:  # Limit to first 20 for speed
            try:
                title_elem = (
                    card.select_one('h1, h2, h3') or
                    card.select_one('a[href*="/listings/"]')
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 15:
                    continue

                link_elem = (
                    card.select_one('a[href*="/listings/"]') or
                    card.find('a', href=True)
                )
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = card.get_text()
                price = self.extract_price_fast(full_text)
                revenue = self.extract_revenue_fast(full_text)
                profit = self.extract_profit_fast(full_text)
                
                description = re.sub(r'\s+', ' ', full_text[:400]).strip()
                
                listing = {
                    'source': 'QuietLight',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                    'multiple': '',
                }
                
                if title and (price or revenue or 'amazon' in title.lower()):
                    listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing QuietLight listing: {e}")
                
        return listings

    def scrape_generic_fast(self, soup: BeautifulSoup, base_url: str, domain: str) -> List[Dict]:
        """OPTIMIZED: Fast generic scraper"""
        listings = []
        source_name = domain.replace('www.', '').split('.')[0].title()
        
        # Fast selector strategy
        for selector in ['div.listing', 'div.business', 'div[class*="listing"]', 'article']:
            containers = soup.select(selector)
            if containers and len(containers) >= 3:
                break
        
        logger.info(f"{source_name}: Found {len(containers)} containers")
        
        for container in containers[:15]:  # Limit for speed
            try:
                title_elem = container.find(['h1', 'h2', 'h3', 'h4'])
                title = title_elem.get_text().strip() if title_elem else ""

                if not title or len(title) < 8:
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
                    'description': re.sub(r'\s+', ' ', full_text[:300]).strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                
                if title and (price or revenue or len(full_text) > 50):
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing {source_name} listing: {e}")
        
        return listings

    def scrape_url_fast(self, url: str) -> List[Dict]:
        """OPTIMIZED: Fast URL scraping with smart routing"""
        if url in self.processed_urls:
            return []
        
        self.processed_urls.add(url)
        
        response = self.make_fast_request(url)
        if not response:
            return []

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            domain = urlparse(url).netloc.lower()
            
            # Fast routing to best scrapers
            if 'bizquest' in domain:
                return self.scrape_bizquest_fast(soup, url)
            elif 'quietlight' in domain:
                return self.scrape_quietlight_fast(soup, url)
            else:
                return self.scrape_generic_fast(soup, url, domain)
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []

    def scrape_all_optimized(self) -> None:
        """OPTIMIZED: High-speed parallel scraping"""
        logger.info(f"Starting OPTIMIZED fast scraping of {len(self.urls)} URLs")
        
        # OPTIMIZED: Larger batches, more workers for working sources
        batch_size = 25  # Increased batch size
        max_workers = 15  # Increased workers
        
        batches = [self.urls[i:i + batch_size] for i in range(0, len(self.urls), batch_size)]
        
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} URLs)")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {executor.submit(self.scrape_url_fast, url): url for url in batch}
                
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        listings = future.result()
                        if listings:
                            with self.lock:
                                self.scraped_data.extend(listings)
                                logger.info(f"Added {len(listings)} listings from {url}")
                    except Exception as e:
                        logger.error(f"Error processing {url}: {e}")
            
            # Shorter delays between batches
            time.sleep(random.uniform(1, 3))
        
        logger.info(f"OPTIMIZED scraping completed! Total raw listings: {len(self.scraped_data)}")

    def remove_duplicates_fast(self) -> List[Dict]:
        """OPTIMIZED: Fast deduplication"""
        if not self.scraped_data:
            return []

        df = pd.DataFrame(self.scraped_data)
        logger.info(f"Starting fast deduplication with {len(df)} raw listings")
        
        # Fast deduplication
        df = df.drop_duplicates(subset=['url'], keep='first')
        df = df[df['name'].str.len() >= 10]
        df['name_normalized'] = df['name'].str.lower().str[:50]
        df = df.drop_duplicates(subset=['name_normalized'], keep='first')
        df = df.drop(columns=['name_normalized'])
        
        logger.info(f"Fast deduplication complete: {len(df)} unique listings")
        return df.to_dict('records')

    def export_optimized_results(self, filename: str = 'OPTIMIZED_FAST_BUSINESS_LISTINGS.csv') -> None:
        """Export optimized results"""
        if not self.scraped_data:
            logger.warning("No data to export")
            return

        clean_data = self.remove_duplicates_fast()
        df = pd.DataFrame(clean_data)
        df.to_csv(filename, index=False)
        
        # Print results
        print("\n" + "="*80)
        print("⚡ OPTIMIZED FAST SCRAPING RESULTS")
        print("="*80)
        
        print(f"\n📊 SPEED OPTIMIZED HARVEST:")
        print(f"Total unique businesses: {len(df):,}")
        
        source_counts = df['source'].value_counts()
        print(f"\n📈 SOURCE BREAKDOWN:")
        for source, count in source_counts.items():
            percentage = (count / len(df)) * 100
            print(f"  {source}: {count:,} businesses ({percentage:.1f}%)")
        
        # Financial analysis
        price_coverage = df['price'].notna().sum()
        revenue_coverage = df['revenue'].notna().sum()
        
        print(f"\n💰 DATA QUALITY:")
        print(f"  Price coverage: {price_coverage:,}/{len(df):,} ({price_coverage/len(df)*100:.1f}%)")
        print(f"  Revenue coverage: {revenue_coverage:,}/{len(df):,} ({revenue_coverage/len(df)*100:.1f}%)")
        
        logger.info(f"Exported {len(df)} businesses to {filename}")

def main():
    """OPTIMIZED: Main execution"""
    try:
        scraper = OptimizedFastBusinessScraper()
        
        start_time = time.time()
        scraper.scrape_all_optimized()
        end_time = time.time()
        
        scraper.export_optimized_results()
        
        logger.info(f"OPTIMIZED scraping completed in {end_time - start_time:.2f} seconds!")
        
    except Exception as e:
        logger.error(f"Error in optimized execution: {e}")
        raise

if __name__ == "__main__":
    main() 