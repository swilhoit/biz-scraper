from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re
import json

class BizQuestScraper(BaseScraper):
    def __init__(self, site_config: Dict, max_workers: int = 10):
        super().__init__(site_config, max_workers)
        self.js_rendering = True # Enable JS rendering for this scraper

    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs and extract data from search pages"""
        listing_urls = []
        page = 1
        
        # Initialize cache for search data
        if not hasattr(self, '_search_data_cache'):
            self._search_data_cache = {}
        
        while True:
            if max_pages and page > max_pages:
                break
            
            # BizQuest uses a 'page' query parameter
            url = f"{search_url}?page={page}"
            soup = self.get_page(url, render=self.js_rendering)
            
            if not soup:
                self.logger.info(f"No content for {url}, stopping.")
                break
            
            # Find all links that go to business listings
            listings = soup.select('a[href*="/business-for-sale/"]')
            
            # Filter to only unique business listing URLs and extract data
            new_listings = []
            for listing in listings:
                href = listing.get('href')
                if href and '/business-for-sale/' in href and href.endswith('/'):
                    if href.startswith('/'):
                        full_url = self.base_url + href
                    else:
                        full_url = href
                        
                    if full_url not in listing_urls:
                        new_listings.append(full_url)
                        listing_urls.append(full_url)
                        
                        # Extract data from the search result
                        self._extract_search_data(listing, full_url)
            
            if not new_listings:
                self.logger.warning(f"No new listings found on page {page}")
                break
                
            self.logger.info(f"Found {len(new_listings)} new listings on page {page}")
            page += 1
            
        return listing_urls
    
    def _extract_search_data(self, link_element, url: str):
        """Extract listing data from search result container"""
        # Find the specific listing container
        parent = link_element
        # Go up until we find a container that likely contains just this listing
        for _ in range(5):  # Max 5 levels up
            parent = parent.parent
            if not parent:
                return
            # Check if this looks like a listing container
            if parent.name in ['div', 'article', 'section']:
                # Check if it contains only one listing link
                links = parent.select('a[href*="/business-for-sale/"]')
                if len(links) == 1:
                    break
        
        if not parent:
            return
            
        container_text = parent.get_text(separator=' ', strip=True)
        
        data = {}
        
        # Extract title from URL path
        url_parts = url.split('/')
        if len(url_parts) > 4:
            data['title'] = url_parts[4].replace('-', ' ').title()
        
        # Extract price - look for the first price in this container
        prices = re.findall(r'\$([\d,]+(?:\.\d+)?[KkMm]?)', container_text)
        if prices:
            # Usually the first price is the asking price
            data['price'] = self.parse_price(prices[0])
        
        # Extract cash flow
        cash_flow_match = re.search(r'(?:cash flow|profit|net income)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', container_text, re.I)
        if cash_flow_match:
            data['cash_flow'] = self.parse_price(cash_flow_match.group(1))
        
        # Extract location - look for city, state pattern
        location_patterns = [
            r'([A-Za-z\s]+\s*,\s*[A-Z]{2})(?=\s|$)',  # City, ST
            r'([A-Za-z\s]+ County\s*,\s*[A-Z]{2})',     # County, ST
        ]
        
        for pattern in location_patterns:
            location_match = re.search(pattern, container_text)
            if location_match:
                data['location'] = location_match.group(1).strip()
                break
        
        # Extract business type/industry from container
        industry_keywords = ['E-commerce', 'Amazon', 'FBA', 'Wholesaler', 'Retailer', 'SaaS', 'Content']
        for keyword in industry_keywords:
            if keyword.lower() in container_text.lower():
                data['industry'] = keyword
                break
        
        # Extract description - clean it up
        desc_text = container_text
        # Remove common UI elements
        desc_text = re.sub(r'Save|Contact|\d+\s*/\s*\d+', '', desc_text).strip()
        data['description'] = desc_text[:500]
        
        # Store in cache
        self._search_data_cache[url] = data
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing - extract from search results cache if possible"""
        data = {'listing_url': url}
        
        # First check if we have data from search results
        if hasattr(self, '_search_data_cache') and url in self._search_data_cache:
            cached_data = self._search_data_cache[url]
            data.update(cached_data)
            self.logger.info(f"Using cached data for {url}")
            return data
        
        # If not in cache, try to scrape the page (though it might fail)
        soup = self.get_page(url, render=self.js_rendering)
        if not soup:
            # Return basic data with URL
            data['title'] = 'Unable to fetch details'
            data['description'] = 'Please visit the listing URL for full details'
            return data
        
        # Try to extract what we can
        # Title from meta or h1
        title_tag = soup.find('h1') or soup.find('meta', property='og:title')
        if title_tag:
            data['title'] = title_tag.text.strip() if hasattr(title_tag, 'text') else title_tag.get('content', '')
        else:
            # Extract from URL
            url_parts = url.split('/')
            if len(url_parts) > 4:
                data['title'] = url_parts[4].replace('-', ' ').title()
        
        # Description from meta
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc:
            data['description'] = meta_desc.get('content', '').strip()
        
        return data