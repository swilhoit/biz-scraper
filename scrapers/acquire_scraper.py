from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import json
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

class AcquireScraper(BaseScraper):
    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from the main listings page by parsing embedded JSON."""
        listing_urls = []
        
        if not search_url:
            self.logger.error("No search URL configured for Acquire.com")
            return listing_urls
            
        soup = self.get_page(search_url)
        if not soup:
            return listing_urls

        next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
        if not next_data_script:
            self.logger.error("Could not find __NEXT_DATA__ script tag on the page.")
            return listing_urls
            
        try:
            data = json.loads(next_data_script.string)
            listings = data.get('props', {}).get('pageProps', {}).get('listings', [])
            
            for listing in listings:
                slug = listing.get('slug')
                if slug:
                    listing_urls.append(f"{self.site_config['base_url']}/app/listing/{slug}")

        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error parsing __NEXT_DATA__ JSON: {e}")
            
        return listing_urls

    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing page by parsing embedded JSON."""
        soup = self.get_page(url)
        if not soup:
            return None

        next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
        if not next_data_script:
            self.logger.error(f"Could not find __NEXT_DATA__ script tag on {url}")
            return None

        try:
            data = json.loads(next_data_script.string)
            listing_data = data.get('props', {}).get('pageProps', {}).get('listing', {})
            
            if not listing_data:
                self.logger.warning(f"No listing data found in __NEXT_DATA__ for {url}")
                return None
            
            # Extract the relevant fields
            business_data = {
                'listing_url': url,
                'title': listing_data.get('headline'),
                'description': listing_data.get('about'),
                'price': self.parse_price(str(listing_data.get('askingPrice'))),
                'revenue': self.parse_price(str(listing_data.get('financialSummary', {}).get('revenue'))),
                'cash_flow': self.parse_price(str(listing_data.get('financialSummary', {}).get('profit'))),
                'established_year': listing_data.get('foundedIn'),
                'industry': listing_data.get('category'),
            }
            
            if business_data.get('price') and business_data.get('cash_flow'):
                try:
                    business_data['multiple'] = round(business_data['price'] / business_data['cash_flow'], 2)
                except (TypeError, ZeroDivisionError):
                    pass # Multiple is not critical

            return business_data

        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error parsing listing __NEXT_DATA__ JSON for {url}: {e}")
            return None
