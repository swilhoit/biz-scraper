from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import json

class BizBuySellScraper(BaseScraper):
    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from all pages for a given search URL."""
        listing_urls = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                self.logger.info(f"Reached max pages limit: {max_pages}")
                break

            url = f"{search_url}{page}/"
            soup = self.get_page(url)
            
            if not soup:
                self.logger.info(f"No content found for {url}, stopping pagination.")
                break

            initial_listing_count = len(listing_urls)

            # Prioritize JSON-LD data
            json_ld_script = soup.find('script', {'type': 'application/ld+json'})
            if json_ld_script:
                try:
                    data = json.loads(json_ld_script.string)
                    if 'about' in data:
                        for item in data['about']:
                            if 'item' in item and 'url' in item['item']:
                                listing_url = item['item']['url']
                                if listing_url not in listing_urls:
                                    listing_urls.append(listing_url)
                except (json.JSONDecodeError, KeyError) as e:
                    self.logger.error(f"Error parsing JSON-LD on page {page}: {e}")

            # Fallback to HTML selectors if JSON-LD fails or is incomplete
            if len(listing_urls) == initial_listing_count:
                listings = soup.select('div.search-result-card a')
                if listings:
                    for listing in listings:
                        href = listing.get('href')
                        if href:
                            if href.startswith('/'):
                                href = self.base_url + href
                            if href not in listing_urls:
                                listing_urls.append(href)
            
            # If we didn't find any new listings on this page, stop.
            if len(listing_urls) == initial_listing_count:
                self.logger.info(f"No new listings found on page {page}. Stopping pagination.")
                break
            
            self.logger.info(f"Found {len(listing_urls) - initial_listing_count} new listings on page {page}")
            page += 1
            
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {'listing_url': url}
        
        # Prioritize JSON-LD data
        json_ld_script = soup.find('script', {'type': 'application/ld+json'})
        if json_ld_script:
            try:
                ld_data = json.loads(json_ld_script.string)
                if isinstance(ld_data, dict):
                    data['title'] = ld_data.get('name')
                    data['description'] = ld_data.get('description')
                    if 'offers' in ld_data:
                        data['price'] = float(ld_data['offers'].get('price', 0))
                    if 'availableAtOrFrom' in ld_data.get('offers', {}):
                        address = ld_data['offers']['availableAtOrFrom'].get('address', {})
                        city = address.get('addressLocality')
                        state = address.get('addressRegion')
                        if city and state:
                            data['location'] = f"{city}, {state}"
                        elif city:
                            data['location'] = city
                        elif state:
                            data['location'] = state
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.error(f"Error parsing JSON-LD for {url}: {e}")
        
        # Fallback to HTML scraping if JSON-LD is incomplete or fails
        if not data.get('title'):
            title_tag = soup.select_one('h1.font-h1-new')
            data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
            
        if not data.get('description'):
            desc_tag = soup.select_one('div.business-description')
            data['description'] = desc_tag.text.strip() if desc_tag else 'Description not found'

        # Financials are often in a dedicated section
        if not data.get('price'):
            price_tag = soup.select_one('div.asking-price')
            if price_tag:
                data['price'] = self.parse_price(price_tag.text)
        
        financials = soup.select('div.financials-desktop__wrapper--item')
        for item in financials:
            label = item.select_one('p:first-child').text.lower() if item.select_one('p:first-child') else ''
            value = item.select_one('p:last-child').text if item.select_one('p:last-child') else ''
            
            if 'cash flow' in label:
                data['cash_flow'] = self.parse_price(value)
            elif 'gross revenue' in label:
                data['revenue'] = self.parse_price(value)

        return data