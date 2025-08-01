"""
WebsiteClosers Scraper
Scrapes business listings from WebsiteClosers.com
"""
from typing import Dict, List, Optional
import re
from .base_scraper import BaseScraper

class WebsiteClosersScraper(BaseScraper):
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from WebsiteClosers"""
        listing_urls = []
        
        search_url = self.site_config.get('search_url')
        if not search_url:
            self.logger.error("No search URL configured for WebsiteClosers")
            return listing_urls

        self.logger.info(f"Scraping WebsiteClosers listings from {search_url}")
        
        soup = self.get_page(search_url)
        if not soup:
            return listing_urls
        
        links = soup.select('div.post_item a.post_title')
        for link in links:
            href = link.get('href')
            if href:
                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                if full_url not in listing_urls:
                    listing_urls.append(full_url)
        
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single WebsiteClosers listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        listing_data = {'listing_url': url}
        
        title_elem = soup.select_one('h1')
        if title_elem:
            listing_data['title'] = title_elem.get_text().strip()
            
        desc_elem = soup.select_one('div.wysiwyg.cfx')
        if desc_elem:
            listing_data['description'] = desc_elem.get_text().strip()[:2000]

        financials_container = soup.select_one('div.sb-table')
        if financials_container:
            lines = financials_container.select('div.line')
            for line in lines:
                left = line.select_one('div.left')
                right = line.select_one('div.right')
                if left and right:
                    label = left.text.strip().lower()
                    value = right.text.strip()
                    if 'asking price' in label:
                        listing_data['price'] = self.parse_price(value)
                    elif 'cash flow' in label:
                        listing_data['cash_flow'] = self.parse_price(value)
                    elif 'gross income' in label:
                        listing_data['revenue'] = self.parse_price(value)
                    elif 'year established' in label:
                        try:
                            listing_data['established_year'] = int(re.search(r'\d{4}', value).group())
                        except (ValueError, AttributeError):
                            pass
        
        return listing_data if listing_data.get('title') else None