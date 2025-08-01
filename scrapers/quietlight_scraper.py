from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class QuietLightScraper(BaseScraper):
    """Scraper for QuietLight - Most successful site with high-value listings"""
    
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages"""
        listing_urls = []
        
        # QuietLight has multiple category pages
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
                
                # QuietLight uses /page/2/ format for pagination
                if page == 1:
                    url = base_url
                else:
                    url = base_url.rstrip('/') + f'/page/{page}/'
                
                soup = self.get_page(url)
                if not soup:
                    break
                
                # QuietLight uses div.listing-card or similar
                listings = soup.find_all('div', class_=['listing-card', 'listing-item', 'business-listing'])
                
                if not listings:
                    # Try finding links with patterns
                    listings = soup.find_all('a', href=re.compile(r'/listings/[^/]+/?$'))
                
                if not listings:
                    self.logger.info(f"No more listings found on {base_url} page {page}")
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
                        if href not in listing_urls and '/listings/' in href:
                            listing_urls.append(href)
                            found_on_page += 1
                
                self.logger.info(f"Found {found_on_page} listings on {base_url} page {page}")
                
                # Check if there's a next page
                next_page = soup.find('a', class_='next')
                if not next_page:
                    next_page = soup.find('a', text=re.compile(r'Next|→'))
                
                if not next_page or found_on_page == 0:
                    break
                    
                page += 1
        
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing - QuietLight has detailed financials"""
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
        
        # Title - QuietLight often uses h1 or h2
        title_elem = soup.find('h1', class_='listing-title')
        if not title_elem:
            title_elem = soup.find('h1')
        if not title_elem:
            title_elem = soup.find('h2', class_='listing-title')
        if title_elem:
            data['title'] = title_elem.text.strip()
        
        # Look for financial summary section
        financial_section = soup.find('div', class_=['financial-summary', 'listing-financials'])
        if not financial_section:
            financial_section = soup.find('div', id='financials')
        
        # Price - QuietLight uses "Asking Price" prominently
        price_patterns = [
            (r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?)\s*([KkMm])?', 'text'),
            (r'\$?([\d,]+(?:\.\d+)?)\s*([KkMm])?\s*asking', 'text'),
            ('span.asking-price', 'element'),
            ('div.price', 'element')
        ]
        
        for pattern, pattern_type in price_patterns:
            if pattern_type == 'text':
                match = re.search(pattern, soup.get_text(), re.I)
                if match:
                    price_text = match.group(1)
                    multiplier = match.group(2) if len(match.groups()) > 1 else None
                    data['price'] = self._parse_price_with_multiplier(price_text, multiplier)
                    break
            else:
                elem = soup.find(pattern.split('.')[0], class_=pattern.split('.')[1])
                if elem:
                    data['price'] = self.parse_price(elem.text)
                    break
        
        # Revenue and Cash Flow - QuietLight provides detailed financials
        if financial_section:
            rows = financial_section.find_all(['tr', 'div'])
            for row in rows:
                text = row.text.lower()
                if 'revenue' in text or 'sales' in text:
                    revenue_match = re.search(r'\$?([\d,]+(?:\.\d+)?)\s*([KkMm])?', row.text)
                    if revenue_match:
                        data['revenue'] = self._parse_price_with_multiplier(
                            revenue_match.group(1), 
                            revenue_match.group(2) if len(revenue_match.groups()) > 1 else None
                        )
                elif 'cash flow' in text or 'profit' in text or 'income' in text:
                    cash_flow_match = re.search(r'\$?([\d,]+(?:\.\d+)?)\s*([KkMm])?', row.text)
                    if cash_flow_match:
                        data['cash_flow'] = self._parse_price_with_multiplier(
                            cash_flow_match.group(1),
                            cash_flow_match.group(2) if len(cash_flow_match.groups()) > 1 else None
                        )
                elif 'multiple' in text:
                    multiple_match = re.search(r'([\d.]+)x?', row.text)
                    if multiple_match:
                        data['multiple'] = float(multiple_match.group(1))
        
        # Industry/Niche
        industry_elem = soup.find(['span', 'div'], class_=['industry', 'niche', 'category'])
        if industry_elem:
            data['industry'] = industry_elem.text.strip()
        else:
            # Look for tags
            tags = soup.find_all(['span', 'div'], class_='tag')
            industries = []
            for tag in tags:
                tag_text = tag.text.strip()
                if any(word in tag_text.lower() for word in ['amazon', 'fba', 'ecommerce', 'saas', 'content']):
                    industries.append(tag_text)
            if industries:
                data['industry'] = ', '.join(industries)
        
        # Description
        desc_elem = soup.find('div', class_=['listing-description', 'business-description', 'overview'])
        if desc_elem:
            data['description'] = desc_elem.text.strip()
        
        # Additional details
        details_section = soup.find('div', class_=['listing-details', 'business-details'])
        if details_section:
            # Year established
            year_match = re.search(r'established[:\s]+(\d{4})', details_section.text, re.I)
            if year_match:
                data['established_year'] = int(year_match.group(1))
            
            # Check for keywords
            details_text = details_section.text.lower()
            data['seller_financing'] = 'seller financing' in details_text or 'sba' in details_text
            data['inventory_included'] = 'inventory' in details_text
        
        # Calculate multiple if we have price and cash flow
        if data['price'] and data['cash_flow'] and not data['multiple']:
            data['multiple'] = round(data['price'] / data['cash_flow'], 2)
        
        return data
    
    def _parse_price_with_multiplier(self, price_text: str, multiplier: Optional[str]) -> float:
        """Parse price with K/M multiplier"""
        try:
            value = float(price_text.replace(',', ''))
            if multiplier:
                if multiplier.upper() == 'K':
                    value *= 1000
                elif multiplier.upper() == 'M':
                    value *= 1000000
            return value
        except ValueError:
            return None