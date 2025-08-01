from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re
import json

class FlippaScraper(BaseScraper):
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages"""
        listing_urls = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            # Flippa uses different URL structure
            url = f"{self.site_config['search_url']}&page={page}"
            soup = self.get_page(url)
            
            if not soup:
                break
            
            # Flippa might have listings in different formats
            # Look for listing cards or links
            listings = soup.find_all('a', href=re.compile(r'/\d+-[\w-]+$'))
            
            if not listings:
                # Try finding by class
                listings = soup.find_all('a', class_=re.compile('listing|card'))
                listings = [l for l in listings if l.get('href') and '/businesses/' in l.get('href', '')]
            
            if not listings:
                # Check if there's JSON data
                scripts = soup.find_all('script', type='application/json')
                for script in scripts:
                    try:
                        json_data = json.loads(script.string)
                        # Look for listings in JSON structure
                        if isinstance(json_data, dict) and 'listings' in json_data:
                            for listing in json_data['listings']:
                                if 'url' in listing:
                                    listing_urls.append(listing['url'])
                    except:
                        pass
                
                if not listing_urls:
                    self.logger.warning(f"No listings found on page {page}")
                    break
            else:
                for listing in listings:
                    href = listing.get('href')
                    if href:
                        if href.startswith('/'):
                            href = self.base_url + href
                        if href not in listing_urls and '/businesses/' in href:
                            listing_urls.append(href)
            
            self.logger.info(f"Found {len(listing_urls)} total listings after page {page}")
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
        
        # Flippa often uses React/JSON data
        # Look for JSON-LD or embedded data
        json_scripts = soup.find_all('script', type=['application/ld+json', 'application/json'])
        for script in json_scripts:
            try:
                json_data = json.loads(script.string)
                if isinstance(json_data, dict):
                    # Extract relevant fields
                    data['title'] = json_data.get('name') or json_data.get('title')
                    data['description'] = json_data.get('description')
                    if 'price' in json_data:
                        data['price'] = float(json_data['price'])
            except:
                pass
        
        # Title
        if not data['title']:
            title_elem = soup.find('h1')
            if title_elem:
                data['title'] = title_elem.text.strip()
        
        # Look for metrics/stats
        metric_containers = soup.find_all(['div', 'span'], class_=re.compile('metric|stat|value'))
        for container in metric_containers:
            text = container.text.strip().lower()
            parent_text = container.parent.text.lower() if container.parent else ''
            
            if 'price' in parent_text and '$' in text:
                data['price'] = self.parse_price(text)
            elif 'revenue' in parent_text and '$' in text:
                data['revenue'] = self.parse_price(text)
            elif ('profit' in parent_text or 'income' in parent_text) and '$' in text:
                data['cash_flow'] = self.parse_price(text)
        
        # Description
        desc_elem = soup.find('div', class_=re.compile('description|overview'))
        if desc_elem:
            data['description'] = desc_elem.text.strip()
        
        # Location and industry from tags or labels
        tag_elements = soup.find_all(['span', 'div'], class_=re.compile('tag|label|badge'))
        for tag in tag_elements:
            text = tag.text.strip()
            # Common location patterns
            if re.match(r'^[A-Z]{2}$', text) or ',' in text:
                data['location'] = text
            # Industry keywords
            elif any(word in text.lower() for word in ['ecommerce', 'saas', 'content', 'service', 'retail']):
                data['industry'] = text
        
        # Fallback to meta tags
        if not data['title']:
            title_meta = soup.find('meta', property='og:title')
            if title_meta:
                data['title'] = title_meta.get('content', '').strip()
        
        if not data['description']:
            desc_meta = soup.find('meta', property='og:description')
            if desc_meta:
                data['description'] = desc_meta.get('content', '').strip()
        
        # Calculate multiple if we have price and cash flow
        if data['price'] and data['cash_flow'] and data['cash_flow'] > 0:
            data['multiple'] = round(data['price'] / data['cash_flow'], 2)
        
        return data