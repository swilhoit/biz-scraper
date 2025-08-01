from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class WebsitePropertiesScraper(BaseScraper):
    """Scraper for WebsiteProperties - High-value digital businesses"""
    
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages"""
        listing_urls = []
        
        # WebsiteProperties has multiple category pages
        search_urls = self.site_config.get('search_urls', [])
        if not search_urls:
            search_urls = [self.site_config.get('search_url')]
        
        for base_url in search_urls:
            if not base_url:
                continue
                
            page = 1
            while True:
                if max_pages and page > max_pages:
                    break
                
                # Add pagination
                if page == 1:
                    url = base_url
                else:
                    # WebsiteProperties might use ?page=2 or /page/2/
                    if '?' in base_url:
                        url = f"{base_url}&page={page}"
                    else:
                        url = base_url.rstrip('/') + f'/page/{page}/'
                
                soup = self.get_page(url)
                if not soup:
                    break
                
                # Look for listing cards
                listings = soup.find_all('div', class_=['property-card', 'website-card', 'listing-card'])
                
                if not listings:
                    # Try finding links to individual properties
                    listings = soup.find_all('a', href=re.compile(r'/properties/[^/]+/?$|/websites/[^/]+/?$'))
                
                if not listings:
                    self.logger.info(f"No listings found on {base_url} page {page}")
                    break
                
                found_on_page = 0
                for listing in listings:
                    if listing.name == 'div':
                        # Find the link within the div
                        link = listing.find('a', href=True)
                        if link:
                            href = link.get('href')
                        else:
                            continue
                    else:
                        href = listing.get('href')
                    
                    if href:
                        if href.startswith('/'):
                            href = self.base_url + href
                        if href not in listing_urls:
                            listing_urls.append(href)
                            found_on_page += 1
                
                self.logger.info(f"Found {found_on_page} listings on {base_url} page {page}")
                
                if found_on_page == 0:
                    break
                    
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
            'description': None,
            'multiple': None
        }
        
        # Title
        title_elem = soup.find('h1', class_='property-title')
        if not title_elem:
            title_elem = soup.find('h1')
        if title_elem:
            data['title'] = title_elem.text.strip()
        
        # Price - Look for asking price
        price_elem = soup.find(['span', 'div'], class_=['asking-price', 'price', 'property-price'])
        if price_elem:
            data['price'] = self.parse_price(price_elem.text)
        else:
            # Search in text
            price_match = re.search(r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?)\s*([KkMm])?', soup.get_text(), re.I)
            if price_match:
                price_value = float(price_match.group(1).replace(',', ''))
                if price_match.group(2):
                    if price_match.group(2).upper() == 'K':
                        price_value *= 1000
                    elif price_match.group(2).upper() == 'M':
                        price_value *= 1000000
                data['price'] = price_value
        
        # Financial metrics - often in a table or list
        metrics_section = soup.find(['div', 'section'], class_=['metrics', 'financials', 'property-metrics'])
        if metrics_section:
            items = metrics_section.find_all(['li', 'tr', 'div'])
            for item in items:
                text = item.text.lower()
                value_match = re.search(r'\$?([\d,]+(?:\.\d+)?)\s*([KkMm])?', item.text)
                if value_match:
                    value = float(value_match.group(1).replace(',', ''))
                    multiplier = value_match.group(2)
                    if multiplier:
                        if multiplier.upper() == 'K':
                            value *= 1000
                        elif multiplier.upper() == 'M':
                            value *= 1000000
                    
                    if 'revenue' in text or 'sales' in text:
                        data['revenue'] = value
                    elif 'profit' in text or 'income' in text or 'cash flow' in text:
                        data['cash_flow'] = value
                    elif 'multiple' in text:
                        multiple_match = re.search(r'([\d.]+)x?', item.text)
                        if multiple_match:
                            data['multiple'] = float(multiple_match.group(1))
        
        # Industry/Type
        type_elem = soup.find(['span', 'div'], class_=['property-type', 'website-type', 'category'])
        if type_elem:
            data['industry'] = type_elem.text.strip()
        else:
            # Look in meta tags or breadcrumbs
            breadcrumb = soup.find('nav', class_='breadcrumb')
            if breadcrumb:
                links = breadcrumb.find_all('a')
                for link in links:
                    text = link.text.lower()
                    if any(word in text for word in ['amazon', 'fba', 'ecommerce', 'saas', 'content']):
                        data['industry'] = link.text.strip()
                        break
        
        # Description
        desc_elem = soup.find('div', class_=['property-description', 'description', 'overview'])
        if desc_elem:
            data['description'] = desc_elem.text.strip()
        
        # Additional details
        details_list = soup.find(['ul', 'div'], class_=['property-details', 'listing-details'])
        if details_list:
            details_text = details_list.text.lower()
            
            # Year established
            year_match = re.search(r'established[:\s]+(\d{4})', details_text, re.I)
            if year_match:
                data['established_year'] = int(year_match.group(1))
            
            # Employees
            emp_match = re.search(r'(\d+)\s*employees?', details_text, re.I)
            if emp_match:
                data['employees'] = int(emp_match.group(1))
            
            # Check for features
            data['seller_financing'] = 'seller financing' in details_text
            data['inventory_included'] = 'inventory' in details_text
        
        # Calculate multiple if needed
        if data['price'] and data['cash_flow'] and not data['multiple']:
            data['multiple'] = round(data['price'] / data['cash_flow'], 2)
        
        return data