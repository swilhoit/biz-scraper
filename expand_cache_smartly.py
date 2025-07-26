#!/usr/bin/env python3
"""
Smart Cache Expansion for More Business Listings
Efficiently cache more URLs from proven working sources
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

class SmartCacheExpander:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        
        self.cache_dir = "html_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # More URLs from proven working sources
        self.expansion_urls = [
            # More BizQuest pages (proven working with 60+ listings per page)
            "https://www.bizquest.com/business-for-sale/page-6/",
            "https://www.bizquest.com/business-for-sale/page-7/", 
            "https://www.bizquest.com/business-for-sale/page-8/",
            "https://www.bizquest.com/business-for-sale/page-9/",
            "https://www.bizquest.com/business-for-sale/page-10/",
            
            # BizQuest specific categories (high conversion)
            "https://www.bizquest.com/internet-businesses-for-sale/",
            "https://www.bizquest.com/retail-businesses-for-sale/",
            "https://www.bizquest.com/manufacturing-businesses-for-sale/",
            "https://www.bizquest.com/technology-businesses-for-sale/",
            "https://www.bizquest.com/amazon-businesses-for-sale/",
            
            # More BizBuySell focused URLs  
            "https://www.bizbuysell.com/ecommerce/",
            "https://www.bizbuysell.com/technology-businesses-for-sale/",
            "https://www.bizbuysell.com/retail-businesses-for-sale/",
            
            # Flippa specific sections
            "https://flippa.com/search?filter_category=&filter_price_min=&filter_price_max=&filter_revenue_multiple_min=&filter_revenue_multiple_max=&filter_auction_type=&filter_monetization=&filter_monetization=ecommerce",
            "https://flippa.com/search?filter_monetization=amazon-fba",
            
            # More QuietLight categories
            "https://quietlight.com/content-businesses-for-sale/",
            "https://quietlight.com/agency-businesses-for-sale/",
            
            # Empire Flippers sections  
            "https://empireflippers.com/marketplace/",
            "https://empireflippers.com/marketplace/content-sites/",
            "https://empireflippers.com/marketplace/ecommerce/",
            "https://empireflippers.com/marketplace/amazon-fba/",
            
            # WebsiteProperties expansions
            "https://websiteproperties.com/ecommerce-business-for-sale/",
            "https://websiteproperties.com/content-business-for-sale/",
            "https://websiteproperties.com/saas-business-for-sale/",
            
            # FE International
            "https://feinternational.com/buy-a-website/",
            "https://feinternational.com/buy-a-website/ecommerce/",
            "https://feinternational.com/buy-a-website/amazon-fba/",
            
            # BusinessForSale.com
            "https://www.businessforsale.com/uk/businesses-for-sale/",
            "https://www.businessforsale.com/us/businesses-for-sale/",
        ]

    def get_cache_filename(self, url: str) -> str:
        """Generate cache filename from URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        domain = urlparse(url).netloc.replace('www.', '').replace('.', '_')
        return f"{domain}_{url_hash}.html"

    def is_cached(self, url: str) -> bool:
        """Check if URL is already cached"""
        cache_file = os.path.join(self.cache_dir, self.get_cache_filename(url))
        return os.path.exists(cache_file)

    def cache_html(self, url: str, html: str):
        """Cache HTML content for URL"""
        cache_file = os.path.join(self.cache_dir, self.get_cache_filename(url))
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"💾 Cached {url} ({len(html):,} chars)")
        except Exception as e:
            logger.error(f"Error caching {url}: {e}")

    def make_api_request(self, url: str, retries: int = 3) -> Optional[str]:
        """Make API request efficiently"""
        if self.is_cached(url):
            logger.info(f"📁 Already cached: {url}")
            return None
        
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        for attempt in range(retries):
            try:
                logger.info(f"🌐 API REQUEST: {url}")
                response = self.session.get(self.base_url, params=params, timeout=90)
                response.raise_for_status()
                
                html = response.text
                self.cache_html(url, html)
                return html
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        
        return None

    def expand_cache_smart(self):
        """Smart cache expansion focusing on proven sources"""
        logger.info(f"🚀 SMART CACHE EXPANSION: {len(self.expansion_urls)} URLs")
        
        # Check what's already cached
        cached_count = sum(1 for url in self.expansion_urls if self.is_cached(url))
        uncached_count = len(self.expansion_urls) - cached_count
        
        print(f"📊 Expansion status: {cached_count} already cached, {uncached_count} new URLs")
        
        if uncached_count == 0:
            print("🎉 All expansion URLs already cached!")
            return
        
        # Fetch uncached URLs efficiently
        api_calls_made = 0
        successful_caches = 0
        
        for i, url in enumerate(self.expansion_urls, 1):
            if not self.is_cached(url):
                print(f"📥 Caching {i}/{len(self.expansion_urls)}: {url}")
                
                html = self.make_api_request(url)
                if html:
                    api_calls_made += 1
                    successful_caches += 1
                    
                    # Quick preview of content
                    soup = BeautifulSoup(html, 'html.parser')
                    potential_listings = len(soup.find_all('a', href=True))
                    print(f"   ✅ Cached! Found {potential_listings} links")
                else:
                    print(f"   ❌ Failed to cache")
                
                # Rate limiting between requests
                time.sleep(2)
        
        print(f"\n🎯 Cache expansion complete!")
        print(f"   API calls made: {api_calls_made}")
        print(f"   Successfully cached: {successful_caches}")
        print(f"   API calls saved: {cached_count}")

    def quick_extract_preview(self, url: str) -> int:
        """Quick preview of how many listings we might extract"""
        cache_file = os.path.join(self.cache_dir, self.get_cache_filename(url))
        
        if not os.path.exists(cache_file):
            return 0
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                html = f.read()
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Count potential business indicators
            business_indicators = [
                len(soup.select('div[class*="listing"]')),
                len(soup.select('div[class*="business"]')),
                len(soup.select('a[href*="business"]')),
                len(soup.select('a[href*="listing"]')),
                len(re.findall(r'\$[\d,]+', html)),
            ]
            
            return max(business_indicators)
            
        except Exception as e:
            return 0

    def analyze_cache_potential(self):
        """Analyze potential of current cache"""
        print(f"\n📊 ANALYZING CACHE POTENTIAL...")
        
        cache_files = [f for f in os.listdir(self.cache_dir) if f.endswith('.html')]
        
        total_potential = 0
        
        for cache_file in cache_files:
            # Reconstruct URL from filename (approximate)
            domain_part = cache_file.split('_')[0].replace('_', '.')
            
            with open(os.path.join(self.cache_dir, cache_file), 'r', encoding='utf-8') as f:
                html = f.read()
            
            soup = BeautifulSoup(html, 'html.parser')
            potential = max([
                len(soup.select('div[class*="listing"]')),
                len(soup.select('div[class*="business"]')),
                len(soup.select('a[href*="business"]')),
                len(re.findall(r'\$[\d,]+', html)),
            ])
            
            total_potential += potential
            
            if potential > 10:  # Only show promising sources
                print(f"   📈 {domain_part}: ~{potential} potential listings")
        
        print(f"\n🎯 Total potential across all cached files: ~{total_potential:,} listings")

def main():
    """Main cache expansion function"""
    print("🚀 SMART CACHE EXPANSION FOR MORE BUSINESS LISTINGS")
    print("="*70)
    print("Efficiently caching more URLs from proven working sources")
    print("="*70)
    
    expander = SmartCacheExpander()
    
    # Current cache analysis
    expander.analyze_cache_potential()
    
    # Expand cache with new URLs
    expander.expand_cache_smart()
    
    # Final analysis
    expander.analyze_cache_potential()
    
    print(f"\n🎉 SMART EXPANSION COMPLETE!")
    print("💡 Now run maximize_listings.py to extract from expanded cache!")
    print("💰 All future parsing is FREE - no more API costs!")

if __name__ == "__main__":
    main() 