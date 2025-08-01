from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class BizBuySellScraper(BaseScraper):
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages"""
        listing_urls = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            url = f"{self.site_config['search_url']}{page}/"
            soup = self.get_page(url)
            
            if not soup:
                break
            
            # Find listing links - BizBuySell uses Angular
            # Look for links with pattern /business-opportunity/*/ID/
            listings = soup.find_all('a', href=re.compile(r'/business-opportunity/[^/]+/\d+/', re.I))
            
            if not listings:
                # Try alternative pattern
                listings = soup.find_all('a', href=re.compile(r'/Business-Opportunity/[^/]+/\d+/', re.I))
            
            if not listings:
                self.logger.warning(f"No listings found on page {page}")
                break
            
            for listing in listings:
                href = listing.get('href')
                if href:
                    if href.startswith('/'):
                        href = self.base_url + href
                    # Only add unique URLs
                    if href not in listing_urls:
                        listing_urls.append(href)
            
            self.logger.info(f"Found {len(listings)} listings on page {page}")
            page += 1
            
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {
            'listing_url': url,
            'title': None,
            'price': None,
            'revenue': None,
            'cash_flow': None,
            'location': None,
            'industry': None,
            'description': None
        }
        
        # Since BizBuySell uses Angular, we need to look for data in different ways
        # Try to find JSON-LD data first
        json_ld = soup.find('script', type='application/ld+json')
        if json_ld:
            try:
                import json
                ld_data = json.loads(json_ld.string)
                if isinstance(ld_data, dict):
                    data['title'] = ld_data.get('name')
                    data['description'] = ld_data.get('description')
            except:
                pass
        
        # Look for title in various places
        if not data['title']:
            # Try h1 tags
            h1 = soup.find('h1')
            if h1:
                data['title'] = h1.text.strip()
            else:
                # Try meta title
                title_meta = soup.find('meta', property='og:title')
                if title_meta:
                    data['title'] = title_meta.get('content', '').strip()
        
        # Look for price
        price_patterns = [
            r'\$\s*([0-9,]+)',
            r'asking\s*price[:\s]*\$\s*([0-9,]+)',
            r'price[:\s]*\$\s*([0-9,]+)'
        ]
        
        page_text = soup.get_text()
        for pattern in price_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                data['price'] = self.parse_price(match.group(1))
                break
        
        # Look for revenue
        revenue_match = re.search(r'(?:revenue|sales)[:\s]*\$\s*([0-9,]+)', page_text, re.I)
        if revenue_match:
            data['revenue'] = self.parse_price(revenue_match.group(1))
        
        # Look for cash flow
        cash_flow_match = re.search(r'cash\s*flow[:\s]*\$\s*([0-9,]+)', page_text, re.I)
        if cash_flow_match:
            data['cash_flow'] = self.parse_price(cash_flow_match.group(1))
        
        # Look for location
        location_match = re.search(r'(?:location|located)[:\s]*([^,\n]+(?:,\s*[A-Z]{2})?)', page_text, re.I)
        if location_match:
            data['location'] = location_match.group(1).strip()
        
        # Extract from meta tags as fallback
        if not data['description']:
            desc_meta = soup.find('meta', {'name': 'description'})
            if desc_meta:
                data['description'] = desc_meta.get('content', '').strip()
        
        return data