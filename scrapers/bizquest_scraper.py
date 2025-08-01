from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class BizQuestScraper(BaseScraper):
    def __init__(self, site_config: Dict, max_workers: int = 10):
        super().__init__(site_config, max_workers)
        self.js_rendering = True # Enable JS rendering for this scraper

    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages"""
        listing_urls = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            # BizQuest uses a 'page' query parameter
            url = f"{search_url}?page={page}"
            soup = self.get_page(url, render=self.js_rendering)
            
            if not soup:
                self.logger.info(f"No content for {url}, stopping.")
                break
            
            listings = soup.select('div.search-result-card-container a')
            if not listings:
                self.logger.warning(f"No listings found on page {page} of {url}")
                break
            
            for listing in listings:
                href = listing.get('href')
                if href:
                    if href.startswith('/'):
                        href = self.base_url + href
                    if href not in listing_urls:
                        listing_urls.append(href)
            
            self.logger.info(f"Found {len(listing_urls)} total listings after page {page}")

            if max_pages:
                break
                
            page += 1
            
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        soup = self.get_page(url, render=self.js_rendering)
        if not soup:
            return None
        
        data = {'listing_url': url}
        
        # Title
        title_tag = soup.select_one('h1.font-h1-new')
        data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
        
        # Description
        desc_tag = soup.select_one('div.business-description')
        data['description'] = desc_tag.text.strip() if desc_tag else 'Description not found'
        
        # Financials and details from a table
        details_table = soup.select_one('table.table-striped')
        if details_table:
            for row in details_table.find_all('tr'):
                cells = row.find_all(['th', 'td'])
                if len(cells) == 2:
                    label = cells[0].text.strip().lower()
                    value = cells[1].text.strip()
                    
                    if 'asking price' in label:
                        data['price'] = self.parse_price(value)
                    elif 'gross revenue' in label:
                        data['revenue'] = self.parse_price(value)
                    elif 'cash flow' in label:
                        data['cash_flow'] = self.parse_price(value)
                    elif 'location' in label:
                        data['location'] = value
                    elif 'industry' in label:
                        data['industry'] = value
                    elif 'year established' in label:
                        try:
                            data['established_year'] = int(re.search(r'\d{4}', value).group())
                        except (ValueError, AttributeError):
                            data['established_year'] = None
        
        # Fallbacks using meta tags
        if not data.get('title'):
            meta_title = soup.find('meta', property='og:title')
            if meta_title:
                data['title'] = meta_title.get('content', '').strip()

        if not data.get('description'):
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc:
                data['description'] = meta_desc.get('content', '').strip()
        
        # Additional boolean fields
        page_text_lower = soup.get_text().lower()
        data['seller_financing_available'] = 'seller financing' in page_text_lower
        data['real_estate_included'] = 'real estate included' in page_text_lower
        
        return data