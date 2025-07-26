#!/usr/bin/env python3
"""
Cached HTML Business Scraper
Scrapes HTML once, stores locally, then works offline for parsing optimization
SAVES THOUSANDS OF API CALLS!
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
import hashlib
import json

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CachedHTMLScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        # Create cache directory
        self.cache_dir = "html_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache metadata
        self.cache_metadata_file = os.path.join(self.cache_dir, "cache_metadata.json")
        self.cache_metadata = self.load_cache_metadata()
        
        # URLs to cache (focus on proven working ones)
        self.urls_to_cache = [
            # BizQuest - proven working
            "https://www.bizquest.com/business-for-sale/page-1/",
            "https://www.bizquest.com/business-for-sale/page-2/", 
            "https://www.bizquest.com/business-for-sale/page-3/",
            "https://www.bizquest.com/business-for-sale/page-4/",
            "https://www.bizquest.com/business-for-sale/page-5/",
            
            # QuietLight - working URLs
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/ecommerce-businesses-for-sale/",
            "https://quietlight.com/saas-businesses-for-sale/",
            
            # Other working sources
            "https://websiteproperties.com/amazon-fba-business-for-sale/",
            "https://investors.club/tech-stack/amazon-fba/",
            
            # BizBuySell Amazon specific
            "https://www.bizbuysell.com/amazon-stores-for-sale/",
            "https://www.bizbuysell.com/internet-businesses-for-sale/",
        ]

    def load_cache_metadata(self) -> Dict:
        """Load cache metadata from file"""
        if os.path.exists(self.cache_metadata_file):
            try:
                with open(self.cache_metadata_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save_cache_metadata(self):
        """Save cache metadata to file"""
        with open(self.cache_metadata_file, 'w') as f:
            json.dump(self.cache_metadata, f, indent=2)

    def get_cache_filename(self, url: str) -> str:
        """Generate cache filename from URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        domain = urlparse(url).netloc.replace('www.', '').replace('.', '_')
        return f"{domain}_{url_hash}.html"

    def is_cached(self, url: str) -> bool:
        """Check if URL is already cached"""
        cache_file = os.path.join(self.cache_dir, self.get_cache_filename(url))
        return os.path.exists(cache_file)

    def get_cached_html(self, url: str) -> Optional[str]:
        """Get cached HTML for URL"""
        if not self.is_cached(url):
            return None
        
        cache_file = os.path.join(self.cache_dir, self.get_cache_filename(url))
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                logger.info(f"📁 Using cached HTML for {url}")
                return f.read()
        except Exception as e:
            logger.warning(f"Error reading cache for {url}: {e}")
            return None

    def cache_html(self, url: str, html: str):
        """Cache HTML content for URL"""
        cache_file = os.path.join(self.cache_dir, self.get_cache_filename(url))
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(html)
            
            # Update metadata
            self.cache_metadata[url] = {
                'filename': self.get_cache_filename(url),
                'cached_at': time.time(),
                'size_bytes': len(html.encode('utf-8'))
            }
            self.save_cache_metadata()
            
            logger.info(f"💾 Cached HTML for {url} ({len(html):,} chars)")
            
        except Exception as e:
            logger.error(f"Error caching {url}: {e}")

    def make_api_request(self, url: str, retries: int = 3) -> Optional[str]:
        """Make API request ONLY if not cached"""
        # Check cache first
        cached_html = self.get_cached_html(url)
        if cached_html:
            return cached_html
        
        # Make API request
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        for attempt in range(retries):
            try:
                logger.info(f"🌐 API REQUEST {attempt + 1}: {url}")
                response = self.session.get(self.base_url, params=params, timeout=90)
                response.raise_for_status()
                
                html = response.text
                
                # Cache the HTML
                self.cache_html(url, html)
                
                return html
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        
        return None

    def build_html_cache(self):
        """Build cache of HTML from all target URLs"""
        logger.info(f"🏗️  BUILDING HTML CACHE for {len(self.urls_to_cache)} URLs")
        
        # Check what's already cached
        cached_count = sum(1 for url in self.urls_to_cache if self.is_cached(url))
        uncached_count = len(self.urls_to_cache) - cached_count
        
        logger.info(f"📊 Cache status: {cached_count} cached, {uncached_count} need fetching")
        
        if uncached_count == 0:
            logger.info("🎉 All URLs already cached! No API calls needed.")
            return
        
        # Fetch uncached URLs
        api_calls_made = 0
        for url in self.urls_to_cache:
            if not self.is_cached(url):
                html = self.make_api_request(url)
                if html:
                    api_calls_made += 1
                    logger.info(f"✅ Cached {url}")
                else:
                    logger.warning(f"❌ Failed to cache {url}")
                
                # Rate limiting
                time.sleep(2)
        
        logger.info(f"🎯 Cache build complete! Made {api_calls_made} API calls (saved {cached_count})")
        self.print_cache_stats()

    def print_cache_stats(self):
        """Print cache statistics"""
        total_files = len([f for f in os.listdir(self.cache_dir) if f.endswith('.html')])
        total_size = sum(
            os.path.getsize(os.path.join(self.cache_dir, f)) 
            for f in os.listdir(self.cache_dir) 
            if f.endswith('.html')
        )
        
        print(f"\n📊 CACHE STATISTICS:")
        print(f"  Cached files: {total_files}")
        print(f"  Total size: {total_size/1024/1024:.2f} MB")
        print(f"  URLs in metadata: {len(self.cache_metadata)}")

    def extract_financial_safe(self, text: str, patterns: List[str]) -> str:
        """Extract financial values safely"""
        if not text:
            return ""
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = next((m for m in match if m and m.strip()), match[-1])
                
                clean_match = re.sub(r'[^\d.,KkMm]', '', str(match))
                if not clean_match or not any(c.isdigit() for c in clean_match):
                    continue
                
                try:
                    multiplier = 1
                    if clean_match.upper().endswith('K'):
                        multiplier = 1000
                        clean_match = clean_match[:-1]
                    elif clean_match.upper().endswith('M'):
                        multiplier = 1000000
                        clean_match = clean_match[:-1]
                    
                    value = float(clean_match.replace(',', '')) * multiplier
                    if 10000 <= value <= 100000000:  # Reasonable range
                        return f"${value:,.0f}"
                        
                except (ValueError, TypeError):
                    continue
        return ""

    def parse_cached_html(self, url: str) -> List[Dict]:
        """Parse cached HTML without making API calls"""
        html = self.get_cached_html(url)
        if not html:
            logger.warning(f"No cached HTML for {url}")
            return []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            domain = urlparse(url).netloc.lower()
            
            if 'bizquest' in domain:
                return self.parse_bizquest_cached(soup, url)
            elif 'quietlight' in domain:
                return self.parse_quietlight_cached(soup, url)
            elif 'bizbuysell' in domain:
                return self.parse_bizbuysell_cached(soup, url)
            else:
                return self.parse_generic_cached(soup, url, domain)
                
        except Exception as e:
            logger.error(f"Error parsing cached HTML for {url}: {e}")
            return []

    def parse_bizquest_cached(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Parse BizQuest from cached HTML"""
        listings = []
        
        business_cards = soup.select('div[class*="listing"]')
        if not business_cards:
            business_cards = soup.select('div.result-item')
        
        logger.info(f"BizQuest (cached): Found {len(business_cards)} cards")
        
        for card in business_cards:
            try:
                title_elem = (
                    card.select_one('h2 a') or
                    card.select_one('h3 a') or
                    card.find('a', href=True)
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 10:
                    continue

                if title_elem and title_elem.name == 'a':
                    listing_url = urljoin(base_url, title_elem.get('href', ''))
                else:
                    link_elem = card.find('a', href=True)
                    listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = card.get_text()
                
                # Financial extraction
                price_patterns = [
                    r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
                    r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
                ]
                
                revenue_patterns = [
                    r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
                    r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
                ]
                
                profit_patterns = [
                    r'cash\s*flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
                    r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
                ]
                
                price = self.extract_financial_safe(full_text, price_patterns)
                revenue = self.extract_financial_safe(full_text, revenue_patterns)
                profit = self.extract_financial_safe(full_text, profit_patterns)
                
                if price or revenue or profit:
                    listing = {
                        'source': 'BizQuest',
                        'name': title,
                        'price': price,
                        'revenue': revenue,
                        'profit': profit,
                        'description': re.sub(r'\s+', ' ', full_text[:300]).strip(),
                        'url': listing_url,
                    }
                    listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing BizQuest listing: {e}")
                
        return listings

    def parse_quietlight_cached(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Parse QuietLight from cached HTML"""
        listings = []
        
        # Try multiple selectors
        containers = []
        selectors = ['div.listing', 'div[class*="listing"]', 'article', 'div.business']
        
        for selector in selectors:
            containers = soup.select(selector)
            if containers and len(containers) >= 3:
                break
        
        logger.info(f"QuietLight (cached): Found {len(containers)} containers")
        
        for container in containers[:20]:  # Limit for quality
            try:
                title_elem = container.find(['h1', 'h2', 'h3', 'h4'])
                title = title_elem.get_text().strip() if title_elem else ""

                if not title or len(title) < 15:
                    continue

                link_elem = container.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = container.get_text()
                
                # Look for financial data
                price_match = re.search(r'\$[\d,]+(?:\.\d+)?[KkMm]?', full_text)
                price = price_match.group(0) if price_match else ""
                
                revenue_match = re.search(r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?', full_text, re.IGNORECASE)
                revenue = f"${revenue_match.group(1)}" if revenue_match else ""
                
                profit_match = re.search(r'(?:profit|sde|cash flow)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?', full_text, re.IGNORECASE)
                profit = f"${profit_match.group(1)}" if profit_match else ""
                
                if price or revenue or profit:
                    listing = {
                        'source': 'QuietLight',
                        'name': title,
                        'price': price,
                        'revenue': revenue, 
                        'profit': profit,
                        'description': re.sub(r'\s+', ' ', full_text[:300]).strip(),
                        'url': listing_url,
                    }
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing QuietLight listing: {e}")
        
        return listings

    def parse_bizbuysell_cached(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Parse BizBuySell from cached HTML with NO placeholders"""
        listings = []
        
        opportunity_links = soup.select('a[href*="opportunity"]')
        logger.info(f"BizBuySell (cached): Found {len(opportunity_links)} opportunity links")
        
        for link in opportunity_links:
            try:
                href = link.get('href', '')
                if not href or '/business-opportunity/' not in href:
                    continue
                    
                full_url = urljoin(base_url, href)
                link_text = link.get_text().strip()
                
                if len(link_text) < 15:
                    continue
                
                # Get immediate parent only to avoid template data
                parent = link.find_parent(['div', 'article'])
                if not parent:
                    continue
                
                container_text = parent.get_text()
                
                # Extract financial data carefully
                price_match = re.search(r'\$[\d,]+(?:\.\d+)?', container_text)
                price = price_match.group(0) if price_match else ""
                
                # Skip known placeholder values
                if price in ["$250,000", "$500,000", "$2,022", "$500,004"]:
                    continue
                
                if price:
                    listing = {
                        'source': 'BizBuySell',
                        'name': link_text[:150],
                        'price': price,
                        'revenue': "",
                        'profit': "",
                        'description': re.sub(r'\s+', ' ', container_text[:300]).strip(),
                        'url': full_url,
                    }
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing BizBuySell listing: {e}")
        
        return listings

    def parse_generic_cached(self, soup: BeautifulSoup, base_url: str, domain: str) -> List[Dict]:
        """Parse generic site from cached HTML"""
        listings = []
        source_name = domain.replace('www.', '').split('.')[0].title()
        
        containers = soup.select('div[class*="listing"]')
        if not containers:
            containers = soup.select('div.business')
        
        logger.info(f"{source_name} (cached): Found {len(containers)} containers")
        
        for container in containers[:10]:
            try:
                title_elem = container.find(['h1', 'h2', 'h3', 'h4'])
                title = title_elem.get_text().strip() if title_elem else ""

                if not title or len(title) < 10:
                    continue

                link_elem = container.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = container.get_text()
                
                price_match = re.search(r'\$[\d,]+(?:\.\d+)?', full_text)
                price = price_match.group(0) if price_match else ""
                
                if price:
                    listing = {
                        'source': source_name,
                        'name': title,
                        'price': price,
                        'revenue': "",
                        'profit': "",
                        'description': re.sub(r'\s+', ' ', full_text[:200]).strip(),
                        'url': listing_url,
                    }
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing {source_name} listing: {e}")
        
        return listings

    def parse_all_cached(self) -> List[Dict]:
        """Parse all cached HTML files"""
        logger.info("🔄 PARSING ALL CACHED HTML (NO API CALLS)")
        
        all_listings = []
        
        for url in self.urls_to_cache:
            if self.is_cached(url):
                listings = self.parse_cached_html(url)
                if listings:
                    all_listings.extend(listings)
                    logger.info(f"✅ Parsed {len(listings)} listings from cached {url}")
                else:
                    logger.info(f"❌ No listings found in cached {url}")
            else:
                logger.warning(f"⚠️  No cache for {url}")
        
        logger.info(f"🎯 Total listings parsed from cache: {len(all_listings)}")
        return all_listings

    def export_results(self, listings: List[Dict], filename: str = 'CACHED_PARSED_BUSINESSES.csv'):
        """Export results from cached parsing"""
        if not listings:
            logger.warning("No listings to export")
            return

        # Remove duplicates
        df = pd.DataFrame(listings)
        df = df.drop_duplicates(subset=['url'], keep='first')
        
        df.to_csv(filename, index=False)
        
        print("\n" + "="*60)
        print("💾 CACHED HTML PARSING RESULTS")
        print("="*60)
        print(f"Total businesses: {len(df):,}")
        
        # Financial coverage
        price_coverage = df['price'].notna() & (df['price'] != '')
        revenue_coverage = df['revenue'].notna() & (df['revenue'] != '')
        profit_coverage = df['profit'].notna() & (df['profit'] != '')
        
        print(f"\n💰 FINANCIAL DATA:")
        print(f"  Prices: {price_coverage.sum():,}/{len(df):,} ({price_coverage.sum()/len(df)*100:.1f}%)")
        print(f"  Revenue: {revenue_coverage.sum():,}/{len(df):,} ({revenue_coverage.sum()/len(df)*100:.1f}%)")
        print(f"  Profit: {profit_coverage.sum():,}/{len(df):,} ({profit_coverage.sum()/len(df)*100:.1f}%)")
        
        # Source breakdown
        source_counts = df['source'].value_counts()
        print(f"\n📈 SOURCES:")
        for source, count in source_counts.items():
            print(f"  {source}: {count:,} businesses")
        
        logger.info(f"Results exported to {filename}")

def main():
    """Main function"""
    try:
        scraper = CachedHTMLScraper()
        
        print("🚀 CACHED HTML BUSINESS SCRAPER")
        print("="*50)
        print("This approach saves thousands of API calls!")
        print("1. Build HTML cache once")
        print("2. Parse offline repeatedly")
        print("="*50)
        
        # Step 1: Build cache (only makes API calls for uncached URLs)
        scraper.build_html_cache()
        
        # Step 2: Parse all cached HTML (NO API calls)
        listings = scraper.parse_all_cached()
        
        # Step 3: Export results
        scraper.export_results(listings)
        
        print(f"\n🎉 SUCCESS: Parsed {len(listings)} businesses from cached HTML!")
        print("💡 Now you can modify parsing logic and re-run without API costs!")
        
    except Exception as e:
        logger.error(f"Error in cached scraping: {e}")
        raise

if __name__ == "__main__":
    main() 