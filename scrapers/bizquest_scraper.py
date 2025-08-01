from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class BizQuestScraper(BaseScraper):
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages"""
        listing_urls = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            # BizQuest uses page parameter
            url = f"{self.site_config['search_url']}?page={page}"
            soup = self.get_page(url)
            
            if not soup:
                break
            
            # Find listing links
            # BizQuest uses pattern like /city-state-business-for-sale/ID.html
            listings = soup.find_all('a', href=re.compile(r'/[^/]+-business-for-sale/\d+\.html', re.I))
            
            if not listings:
                # Try alternative selectors
                listings = soup.find_all('h3', class_='listing-title')
                listings = [l.find('a') for l in listings if l.find('a')]
                listings = [l for l in listings if l]  # Remove None values
            
            if not listings:
                self.logger.warning(f"No listings found on page {page}")
                break
            
            for listing in listings:
                href = listing.get('href')
                if href:
                    if href.startswith('/'):
                        href = self.base_url + href
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
        
        # Title
        title_elem = soup.find('h1', class_='listing-title')
        if not title_elem:
            title_elem = soup.find('h1')
        if title_elem:
            data['title'] = title_elem.text.strip()
        
        # Look for structured data table
        info_table = soup.find('table', class_='listing-info')
        if not info_table:
            info_table = soup.find('table', id='listing-info')
        
        if info_table:
            rows = info_table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].text.strip().lower()
                    value = cells[1].text.strip()
                    
                    if 'asking price' in label:
                        data['price'] = self.parse_price(value)
                    elif 'gross revenue' in label or 'annual revenue' in label:
                        data['revenue'] = self.parse_price(value)
                    elif 'cash flow' in label:
                        data['cash_flow'] = self.parse_price(value)
                    elif 'location' in label:
                        data['location'] = value
                    elif 'business type' in label or 'industry' in label:
                        data['industry'] = value
                    elif 'established' in label:
                        year_match = re.search(r'(\d{4})', value)
                        if year_match:
                            data['established_year'] = int(year_match.group(1))
        
        # Description
        desc_elem = soup.find('div', class_='business-description')
        if not desc_elem:
            desc_elem = soup.find('div', id='description')
        if desc_elem:
            data['description'] = desc_elem.text.strip()
        
        # Fallback to meta tags
        if not data['title']:
            title_meta = soup.find('meta', property='og:title')
            if title_meta:
                data['title'] = title_meta.get('content', '').strip()
        
        if not data['description']:
            desc_meta = soup.find('meta', {'name': 'description'})
            if desc_meta:
                data['description'] = desc_meta.get('content', '').strip()
        
        # Additional details
        details_section = soup.find('div', class_='listing-details')
        if details_section:
            text = details_section.text.lower()
            data['seller_financing'] = 'seller financing' in text
            data['real_estate_included'] = 'real estate' in text
            data['inventory_included'] = 'inventory' in text
        
        return data