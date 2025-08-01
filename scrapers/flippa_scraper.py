from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import json
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

class FlippaScraper(BaseScraper):
    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from __NEXT_DATA__"""
        listing_urls = []
        page = 1

        while not max_pages or page <= max_pages:
            url = f"{search_url}&page={page}"
            soup = self.get_page(url)
            if not soup:
                break
            
            json_script = soup.find('script', {'id': '__NEXT_DATA__'})
            if not json_script:
                self.logger.warning(f"Could not find __NEXT_DATA__ on {url}")
                break
            
            try:
                page_data = json.loads(json_script.string)
                edges = page_data.get('props', {}).get('pageProps', {}).get('data', {}).get('search', {}).get('listings', {}).get('edges', [])
                if not edges:
                    self.logger.info(f"No listings found in __NEXT_DATA__ on page {page}")
                    break
                    
                for edge in edges:
                    node = edge.get('node', {})
                    if node.get('pretty_path'):
                        listing_urls.append(f"{self.base_url}{node['pretty_path']}")
                
                self.logger.info(f"Found {len(edges)} listings on page {page}")
                
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.error(f"Error parsing __NEXT_DATA__ on {url}: {e}")
                break
            
            page += 1
        
        return listing_urls

    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing, prioritizing JSON data"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {'listing_url': url}
        
        # Prioritize __NEXT_DATA__ JSON blob
        json_script = soup.find('script', {'id': '__NEXT_DATA__'})
        if json_script:
            try:
                page_data = json.loads(json_script.string)
                listing_data = page_data.get('props', {}).get('pageProps', {}).get('listing', {})
                
                data['title'] = listing_data.get('title')
                data['description'] = listing_data.get('summary')
                data['price'] = listing_data.get('price_usd')
                
                financials = listing_data.get('financials', {})
                data['revenue'] = financials.get('average_monthly_revenue_usd')
                data['cash_flow'] = financials.get('average_monthly_profit_usd')
                
                location = listing_data.get('location', {})
                data['location'] = f"{location.get('city', '')}, {location.get('country', '')}"
                
                data['industry'] = listing_data.get('category', {}).get('name')
                
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.warning(f"Could not parse JSON data for {url}: {e}")
                # If JSON fails, proceed with HTML scraping as a fallback
        
        # Fallback to HTML scraping if JSON is incomplete or fails
        if not data.get('title'):
            title_tag = soup.select_one('h1.listing-title')
            data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
            
        if not data.get('description'):
            desc_tag = soup.select_one('div.listing-summary')
            data['description'] = desc_tag.text.strip() if desc_tag else 'Description not found'

        # Other details as fallbacks
        if not data.get('price'):
            price_tag = soup.select_one('.listing-price')
            if price_tag:
                data['price'] = self.parse_price(price_tag.text)
        
        return data