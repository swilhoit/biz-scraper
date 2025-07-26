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

class UltraOptimizedScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.scraped_data = []
        self.processed_urls = set()
        self.rate_limited_count = 0
        self.success_count = 0
        self.error_count = 0
        self.lock = threading.Lock()
        
        # ULTRA-OPTIMIZED: Focused on working sources with sustainable limits
        self.urls = self.generate_sustainable_urls()

    def generate_sustainable_urls(self) -> List[str]:
        """Generate focused URL list for sustainable scraping"""
        urls = []
        
        # PRIORITY 1: BizQuest (PROVEN TO WORK - 62 listings per page)
        # Start with fewer pages to avoid rate limits
        for page in range(1, 51):  # 50 pages instead of 100
            urls.append(f"https://www.bizquest.com/business-for-sale/page-{page}/")
        
        # BizQuest categories (proven working - reduced pages)
        bizquest_categories = [
            "ecommerce-business-for-sale",
            "amazon-business-for-sale", 
            "online-business-for-sale",
            "technology-business-for-sale"  # Removed retail to reduce load
        ]
        
        for category in bizquest_categories:
            for page in range(1, 16):  # Reduced from 30 to 15 pages each
                urls.append(f"https://www.bizquest.com/{category}/page-{page}/")
        
        # PRIORITY 2: QuietLight (4 URLs only - high value)
        quietlight_urls = [
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/saas-businesses-for-sale/",
            "https://quietlight.com/ecommerce-businesses-for-sale/",
        ]
        urls.extend(quietlight_urls)
        
        # PRIORITY 3: Minimal other sources for testing
        other_sources = [
            "https://flippa.com/buy/monetization/amazon-fba",
            "https://investors.club/tech-stack/amazon-fba/",
        ]
        urls.extend(other_sources)
        
        logger.info(f"Generated {len(urls)} sustainable URLs for rate-limit-aware scraping")
        return urls

    def make_sustainable_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """ULTRA-OPTIMIZED: Sustainable requests with proper rate limit handling"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        for attempt in range(retries):
            try:
                # ULTRA-OPTIMIZED: Longer delays to respect rate limits
                time.sleep(random.uniform(3, 6))  # Increased from 0.5-2s to 3-6s
                
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=90)
                
                if response.status_code == 200:
                    with self.lock:
                        self.success_count += 1
                    return response
                elif response.status_code == 429:
                    # FIXED: Proper 429 handling with exponential backoff
                    with self.lock:
                        self.rate_limited_count += 1
                    backoff_time = (2 ** attempt) * 10  # 10s, 20s, 40s backoff
                    logger.warning(f"Rate limited on {url}, backing off for {backoff_time}s")
                    time.sleep(backoff_time)
                    continue
                elif response.status_code in [500, 503, 502]:
                    logger.warning(f"Server error {response.status_code} for {url}")
                    time.sleep(5)
                    continue
                else:
                    response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(5, 10))  # Longer retry delays
                else:
                    with self.lock:
                        self.error_count += 1
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
        return None

    def extract_financial_optimized(self, text: str, patterns: List[str]) -> str:
        """OPTIMIZED: Fast financial extraction"""
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
                        if 5000 <= value <= 200000000:
                            return f"${value:,.0f}"
                    except:
                        continue
        return ""

    def extract_price_optimized(self, text: str) -> str:
        """Optimized price extraction"""
        patterns = [
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
            r'save\s*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_optimized(text, patterns)

    def extract_revenue_optimized(self, text: str) -> str:
        """Optimized revenue extraction"""
        patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'gross[:\s]*(?:sales|revenue)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'annual\s*(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_optimized(text, patterns)

    def extract_profit_optimized(self, text: str) -> str:
        """Optimized profit extraction"""
        patterns = [
            r'cash\s*flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sde[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_optimized(text, patterns)

    def scrape_bizquest_optimized(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """PROVEN: BizQuest scraper (working perfectly from logs)"""
        listings = []
        
        # Use proven selector from logs
        business_cards = soup.select('div[class*="listing"]')
        logger.info(f"BizQuest: Found {len(business_cards)} cards")
        
        for card in business_cards:
            try:
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

                if title_elem.name == 'a':
                    listing_url = urljoin(base_url, title_elem.get('href', ''))
                else:
                    link_elem = card.find('a', href=True)
                    listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = card.get_text()
                price = self.extract_price_optimized(full_text)
                revenue = self.extract_revenue_optimized(full_text)
                profit = self.extract_profit_optimized(full_text)
                
                description = re.sub(r'\s+', ' ', full_text[:300]).strip()
                
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

    def scrape_quietlight_optimized(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Optimized QuietLight scraper"""
        listings = []
        
        business_cards = []
        for selector in ['div.listing-item', 'article.post', 'div[class*="business"]']:
            cards = soup.select(selector)
            if cards and len(cards) >= 5:
                business_cards = cards
                break
        
        logger.info(f"QuietLight: Found {len(business_cards)} cards")
        
        for card in business_cards[:15]:  # Limit for efficiency
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
                price = self.extract_price_optimized(full_text)
                revenue = self.extract_revenue_optimized(full_text)
                profit = self.extract_profit_optimized(full_text)
                
                description = re.sub(r'\s+', ' ', full_text[:400]).strip()
                
                listing = {
                    'source': 'QuietLight',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                }
                
                if title and (price or revenue or 'amazon' in title.lower()):
                    listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing QuietLight listing: {e}")
                
        return listings

    def scrape_generic_optimized(self, soup: BeautifulSoup, base_url: str, domain: str) -> List[Dict]:
        """Optimized generic scraper"""
        listings = []
        source_name = domain.replace('www.', '').split('.')[0].title()
        
        for selector in ['div.listing', 'div.business', 'div[class*="listing"]', 'article']:
            containers = soup.select(selector)
            if containers and len(containers) >= 3:
                break
        
        logger.info(f"{source_name}: Found {len(containers)} containers")
        
        for container in containers[:10]:  # Reduced for efficiency
            try:
                title_elem = container.find(['h1', 'h2', 'h3', 'h4'])
                title = title_elem.get_text().strip() if title_elem else ""

                if not title or len(title) < 8:
                    continue

                link_elem = container.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = container.get_text()
                price = self.extract_price_optimized(full_text)
                revenue = self.extract_revenue_optimized(full_text)
                profit = self.extract_profit_optimized(full_text)
                
                listing = {
                    'source': source_name,
                    'name': title,
                    'price': price,
                    'revenue': revenue, 
                    'profit': profit,
                    'description': re.sub(r'\s+', ' ', full_text[:300]).strip(),
                    'url': listing_url,
                }
                
                if title and (price or revenue or len(full_text) > 50):
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing {source_name} listing: {e}")
        
        return listings

    def scrape_url_sustainable(self, url: str) -> List[Dict]:
        """ULTRA-OPTIMIZED: Sustainable URL scraping"""
        if url in self.processed_urls:
            return []
        
        self.processed_urls.add(url)
        
        response = self.make_sustainable_request(url)
        if not response:
            return []

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            domain = urlparse(url).netloc.lower()
            
            if 'bizquest' in domain:
                return self.scrape_bizquest_optimized(soup, url)
            elif 'quietlight' in domain:
                return self.scrape_quietlight_optimized(soup, url)
            else:
                return self.scrape_generic_optimized(soup, url, domain)
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []

    def print_progress_stats(self):
        """Print current progress statistics"""
        total_processed = self.success_count + self.error_count + self.rate_limited_count
        success_rate = (self.success_count / total_processed * 100) if total_processed > 0 else 0
        
        print(f"\n📊 PROGRESS STATS:")
        print(f"  Successful requests: {self.success_count}")
        print(f"  Rate limited (429): {self.rate_limited_count}")
        print(f"  Errors: {self.error_count}")
        print(f"  Success rate: {success_rate:.1f}%")
        print(f"  Listings harvested: {len(self.scraped_data)}")

    def scrape_all_sustainable(self) -> None:
        """ULTRA-OPTIMIZED: Sustainable scraping with rate limit awareness"""
        logger.info(f"Starting ULTRA-OPTIMIZED sustainable scraping of {len(self.urls)} URLs")
        
        # ULTRA-OPTIMIZED: Conservative settings to avoid rate limits
        batch_size = 10  # Reduced from 25 to 10
        max_workers = 3   # Reduced from 15 to 3 workers
        
        batches = [self.urls[i:i + batch_size] for i in range(0, len(self.urls), batch_size)]
        
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} URLs)")
            
            # ULTRA-OPTIMIZED: Sequential processing to avoid rate limits
            if batch_num <= 3:  # First 3 batches use threading
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_url = {executor.submit(self.scrape_url_sustainable, url): url for url in batch}
                    
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
            else:
                # After batch 3, go sequential to be extra safe
                for url in batch:
                    listings = self.scrape_url_sustainable(url)
                    if listings:
                        self.scraped_data.extend(listings)
                        logger.info(f"Added {len(listings)} listings from {url}")
            
            # Progress update every 3 batches
            if batch_num % 3 == 0:
                self.print_progress_stats()
            
            # Longer delays between batches
            time.sleep(random.uniform(5, 10))
        
        logger.info(f"ULTRA-OPTIMIZED scraping completed! Total raw listings: {len(self.scraped_data)}")

    def remove_duplicates_sustainable(self) -> List[Dict]:
        """Optimized deduplication"""
        if not self.scraped_data:
            return []

        df = pd.DataFrame(self.scraped_data)
        logger.info(f"Starting deduplication with {len(df)} raw listings")
        
        # Fast deduplication
        df = df.drop_duplicates(subset=['url'], keep='first')
        df = df[df['name'].str.len() >= 10]
        df['name_normalized'] = df['name'].str.lower().str[:50]
        df = df.drop_duplicates(subset=['name_normalized'], keep='first')
        df = df.drop(columns=['name_normalized'])
        
        logger.info(f"Deduplication complete: {len(df)} unique listings")
        return df.to_dict('records')

    def export_sustainable_results(self, filename: str = 'ULTRA_OPTIMIZED_BUSINESS_LISTINGS.csv') -> None:
        """Export sustainable results"""
        if not self.scraped_data:
            logger.warning("No data to export")
            return

        clean_data = self.remove_duplicates_sustainable()
        df = pd.DataFrame(clean_data)
        df.to_csv(filename, index=False)
        
        # Print results
        print("\n" + "="*80)
        print("🚀 ULTRA-OPTIMIZED SUSTAINABLE SCRAPING RESULTS")
        print("="*80)
        
        print(f"\n📊 SUSTAINABLE HARVEST:")
        print(f"Total unique businesses: {len(df):,}")
        
        # Final progress stats
        self.print_progress_stats()
        
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

    def fetch_page_optimized(self, url: str, use_js: bool = False) -> BeautifulSoup:
        """Fetch page with optimized settings"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        if use_js:
            params['render'] = 'true'
        
        self.logger.info(f"Fetching {url} ({'JS' if use_js else 'No JS'})")
        response = self.session.get(self.base_url, params=params, timeout=120)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def is_valid_business_title(self, title: str) -> bool:
        """Enhanced business title validation"""
        if not title or len(title.strip()) < 8:
            return False
        
        # Exclude UI elements
        ui_keywords = [
            'instant listing', 'alerts', 'newsletter', 'sign up', 'subscribe',
            'view all', 'see more', 'load more', 'filter', 'search', 'sort by',
            'contact', 'about', 'privacy', 'terms', 'login', 'register'
        ]
        
        title_lower = title.lower()
        for keyword in ui_keywords:
            if keyword in title_lower:
                return False
        
        # Must have business indicators
        business_indicators = [
            'brand', 'business', 'company', 'store', 'shop', 'website', 'platform',
            'amazon', 'fba', 'ecommerce', 'subscription', 'saas', 'app', 'tech',
            'revenue', 'profit', 'income', 'sales', '$', 'million', 'thousand',
            'yoy', 'growth', 'margin', 'supplement', 'health', 'fitness'
        ]
        
        for indicator in business_indicators:
            if indicator in title_lower:
                return True
        
        return len(title.strip()) > 40  # Long descriptive titles likely businesses

    def extract_financial_data_enhanced(self, card_soup) -> dict:
        """Enhanced financial data extraction"""
        result = {'price': '', 'revenue': '', 'profit': ''}
        
        # Get all text for comprehensive parsing
        full_text = card_soup.get_text()
        
        # Enhanced price extraction
        price_patterns = [
            r'Asking\s*Price[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Price[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'\$[\d,]+(?:\.\d+)?[KMB]?(?:\s*(?:Million|M|Thousand|K))?',
            r'[\d,]+(?:\.\d+)?\s*(?:Million|M|Thousand|K)',
            r'Accepting\s*Offers',
            r'Under\s*Offer'
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                result['price'] = match.group().strip()
                break
        
        # Enhanced revenue extraction
        revenue_patterns = [
            r'Revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Sales[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'TTM[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Annual[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'\$[\d,]+(?:\.\d+)?[KMB]?\s*(?:revenue|sales|ttm)',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                result['revenue'] = match.group().strip()
                break
        
        # Enhanced profit extraction
        profit_patterns = [
            r'Income[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'SDE[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Profit[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Earnings[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'Net[:\s]*\$?[\d,]+(?:\.\d+)?[KMB]?',
            r'\$[\d,]+(?:\.\d+)?[KMB]?\s*(?:sde|profit|income|earnings)',
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                result['profit'] = match.group().strip()
                break
        
        return result

    def scrape_quietlight_ultimate(self, base_url: str, max_pages: int = 5) -> list:
        """Ultimate QuietLight scraper with pagination"""
        all_businesses = []
        
        for page in range(1, max_pages + 1):
            try:
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}page/{page}/"
                
                soup = self.fetch_page_optimized(url)
                
                # Use proven selectors
                listing_cards = soup.select('div.listing-card.grid-item, div.listing-card')
                self.logger.info(f"QuietLight page {page}: Found {len(listing_cards)} cards")
                
                if not listing_cards:
                    break  # No more pages
                
                page_businesses = []
                
                for card in listing_cards:
                    try:
                        # Extract title
                        title_elem = card.select_one('h1, h2, h3, .title, a[href*="/listings/"]')
                        if not title_elem:
                            # Get first line of text as title
                            lines = [line.strip() for line in card.get_text().split('\n') if line.strip()]
                            title = lines[0] if lines else ""
                        else:
                            title = title_elem.get_text().strip()
                        
                        if not self.is_valid_business_title(title):
                            continue
                        
                        # Extract URL
                        url_elem = card.select_one('a[href*="/listings/"]')
                        business_url = urljoin(url, url_elem['href']) if url_elem else url
                        
                        # Extract financial data
                        financial_data = self.extract_financial_data_enhanced(card)
                        
                        # Create business record
                        business = {
                            'name': title[:200],
                            'url': business_url,
                            'price': financial_data['price'],
                            'revenue': financial_data['revenue'],
                            'profit': financial_data['profit'],
                            'description': "",
                            'source': 'QuietLight',
                            'category': 'Amazon FBA'
                        }
                        
                        page_businesses.append(business)
                        
                    except Exception as e:
                        continue
                
                all_businesses.extend(page_businesses)
                self.logger.info(f"Page {page}: Extracted {len(page_businesses)} businesses")
                
                if len(listing_cards) < 20:  # Likely last page
                    break
                
            except Exception as e:
                self.logger.error(f"Error on QuietLight page {page}: {e}")
                break
        
        return all_businesses

    def scrape_flippa_optimized(self, url: str) -> list:
        """Optimized Flippa scraper with JavaScript rendering"""
        try:
            # Flippa needs JS rendering
            soup = self.fetch_page_optimized(url, use_js=True)
            businesses = []
            
            # Try multiple selectors for Flippa
            selectors = [
                'div[data-testid*="listing"]',
                'div[class*="ListingCard"]', 
                'div[class*="auction-card"]',
                'div[class*="listing-item"]',
                'article',
                'div[class*="card"]'
            ]
            
            listing_cards = []
            for selector in selectors:
                cards = soup.select(selector)
                if cards and len(cards) > 5:  # Found meaningful results
                    listing_cards = cards
                    self.logger.info(f"Flippa: Using selector '{selector}' - found {len(cards)} cards")
                    break
            
            for card in listing_cards[:50]:  # Process first 50
                try:
                    # Extract title
                    title_elem = card.select_one('h1, h2, h3, .title, a[href*="/"]')
                    title = title_elem.get_text().strip() if title_elem else ""
                    
                    if not self.is_valid_business_title(title):
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financial data
                    financial_data = self.extract_financial_data_enhanced(card)
                    
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': financial_data['price'],
                        'revenue': financial_data['revenue'],
                        'profit': financial_data['profit'],
                        'description': "",
                        'source': 'Flippa',
                        'category': 'Amazon FBA'
                    }
                    
                    businesses.append(business)
                    
                except Exception as e:
                    continue
            
            self.logger.info(f"Flippa: Extracted {len(businesses)} businesses")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error scraping Flippa {url}: {e}")
            return []

    def scrape_websiteproperties_optimized(self, url: str) -> list:
        """Optimized WebsiteProperties scraper"""
        try:
            soup = self.fetch_page_optimized(url)
            businesses = []
            
            # Try multiple selectors
            selectors = [
                'div.listing',
                'div[class*="business"]',
                'article',
                'div.property',
                'div[class*="card"]',
                'div[class*="item"]'
            ]
            
            listing_cards = []
            for selector in selectors:
                cards = soup.select(selector)
                if cards:
                    listing_cards = cards
                    self.logger.info(f"WebsiteProperties: Using '{selector}' - found {len(cards)} cards")
                    break
            
            for card in listing_cards:
                try:
                    # Extract title
                    title_elem = card.select_one('h1, h2, h3, .title, a[href*="/"]')
                    title = title_elem.get_text().strip() if title_elem else ""
                    
                    if not self.is_valid_business_title(title):
                        continue
                    
                    # Extract URL
                    url_elem = card.select_one('a[href*="/"]')
                    business_url = urljoin(url, url_elem['href']) if url_elem else url
                    
                    # Extract financial data
                    financial_data = self.extract_financial_data_enhanced(card)
                    
                    business = {
                        'name': title[:200],
                        'url': business_url,
                        'price': financial_data['price'],
                        'revenue': financial_data['revenue'],
                        'profit': financial_data['profit'],
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

    def run_ultra_optimized_scraping(self):
        """Run ultra-optimized scraping for all sites"""
        print("🚀 STARTING ULTRA-OPTIMIZED SCRAPING")
        print("="*70)
        
        # Define optimized URLs with pagination
        url_configs = [
            # QuietLight with pagination (PROVEN WORKING)
            {
                'base_url': 'https://quietlight.com/amazon-fba-businesses-for-sale/',
                'scraper': 'quietlight',
                'pages': 5
            },
            {
                'base_url': 'https://quietlight.com/ecommerce-businesses-for-sale/',
                'scraper': 'quietlight', 
                'pages': 5
            },
            {
                'base_url': 'https://quietlight.com/saas-businesses-for-sale/',
                'scraper': 'quietlight',
                'pages': 3
            },
            # Flippa optimization
            {
                'url': 'https://flippa.com/buy/monetization/amazon-fba',
                'scraper': 'flippa'
            },
            {
                'url': 'https://flippa.com/buy/monetization/ecommerce',
                'scraper': 'flippa'
            },
            # WebsiteProperties optimization
            {
                'url': 'https://websiteproperties.com/amazon-fba-business-for-sale/',
                'scraper': 'websiteproperties'
            },
            {
                'url': 'https://websiteproperties.com/ecommerce-business-for-sale/',
                'scraper': 'websiteproperties'
            },
            # BizQuest (already working well)
            {
                'url': 'https://www.bizquest.com/business-for-sale/page-1/',
                'scraper': 'bizquest'
            },
            {
                'url': 'https://www.bizquest.com/business-for-sale/page-2/',
                'scraper': 'bizquest'
            }
        ]
        
        all_businesses = []
        
        for config in url_configs:
            try:
                if config['scraper'] == 'quietlight':
                    businesses = self.scrape_quietlight_ultimate(config['base_url'], config['pages'])
                elif config['scraper'] == 'flippa':
                    businesses = self.scrape_flippa_optimized(config['url'])
                elif config['scraper'] == 'websiteproperties':
                    businesses = self.scrape_websiteproperties_optimized(config['url'])
                elif config['scraper'] == 'bizquest':
                    businesses = self.scrape_bizquest_optimized(self.fetch_page_optimized(config['url']), config['url'])
                
                all_businesses.extend(businesses)
                print(f"✅ {config.get('base_url', config.get('url'))}: {len(businesses)} businesses")
                
                # Random delay to be nice to servers
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                print(f"❌ {config.get('base_url', config.get('url'))}: {e}")
        
        # Deduplicate
        unique_businesses = []
        seen_urls = set()
        
        for business in all_businesses:
            if business['url'] not in seen_urls:
                unique_businesses.append(business)
                seen_urls.add(business['url'])
        
        # Export results
        print(f"\n📊 ULTRA-OPTIMIZED RESULTS:")
        print(f"Total unique businesses: {len(unique_businesses)}")
        print(f"With prices: {sum(1 for b in unique_businesses if b['price'])}")
        print(f"With revenue: {sum(1 for b in unique_businesses if b['revenue'])}")
        print(f"With profit: {sum(1 for b in unique_businesses if b['profit'])}")
        
        # Source breakdown
        sources = {}
        for business in unique_businesses:
            source = business['source']
            sources[source] = sources.get(source, 0) + 1
        
        print(f"\n📈 SOURCE BREAKDOWN:")
        for source, count in sources.items():
            percentage = (count / len(unique_businesses)) * 100
            print(f"  {source}: {count} businesses ({percentage:.1f}%)")
        
        if unique_businesses:
            df = pd.DataFrame(unique_businesses)
            df.to_csv('ULTRA_OPTIMIZED_BUSINESS_LISTINGS.csv', index=False)
            print(f"\n💾 Exported {len(unique_businesses)} businesses to ULTRA_OPTIMIZED_BUSINESS_LISTINGS.csv")
        
        return unique_businesses

def main():
    scraper = UltraOptimizedScraper()
    results = scraper.run_ultra_optimized_scraping()
    
    if len(results) > 100:
        print(f"\n🎯 MASSIVE SUCCESS! Found {len(results)} businesses!")
        print("All sites optimized and working!")
    else:
        print(f"\n⚠️  Need more optimization. Found {len(results)} businesses")

if __name__ == "__main__":
    main() 