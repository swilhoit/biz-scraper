from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re
import json

class EmpireFlippersScraper(BaseScraper):
    """Scraper for EmpireFlippers - JavaScript-heavy site with high-value listings"""
    
    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from marketplace"""
        listing_urls = []
        page = 1
        while True:
            if max_pages and page > max_pages:
                break
            
            url = f"{search_url}?page={page}" if '?' not in search_url else f"{search_url}&page={page}"
            
            soup = self.get_page(url)
            if not soup:
                break
            
            listings = soup.select('div.listing-item a')
            
            if not listings:
                self.logger.warning(f"No listings found on page {page}")
                break
            
            for listing in listings:
                href = listing.get('href')
                if href:
                    if href.startswith('/'):
                        href = self.base_url + href
                    if href not in listing_urls and '/listing/' in href:
                        listing_urls.append(href)
            
            self.logger.info(f"Found {len(listing_urls)} total listings after page {page}")
            
            if not soup.select('a.next-page-link'):
                break
                
            page += 1
        
        return list(set(listing_urls))
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {'listing_url': url}
        
        # Title
        title_tag = soup.select_one('h1')
        data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
        
        # Description
        desc_tag = soup.select_one('div.listing-details p')
        data['description'] = desc_tag.text.strip() if desc_tag else 'Description not found'
        
        # Financials and details from a table
        metrics_section = soup.select_one('div.metrics-wrapper')
        if metrics_section:
            for item in metrics_section.select('div.metric-item'):
                label_tag = item.select_one('div.label')
                value_tag = item.select_one('div.value')
                if label_tag and value_tag:
                    label = label_tag.text.strip().lower()
                    value = value_tag.text.strip()
                    
                    if 'price' in label:
                        data['price'] = self.parse_price(value)
                    elif 'monthly net profit' in label:
                        monthly_profit = self.parse_price(value)
                        if monthly_profit:
                            data['cash_flow'] = monthly_profit * 12
                    elif 'monthly revenue' in label:
                        monthly_revenue = self.parse_price(value)
                        if monthly_revenue:
                            data['revenue'] = monthly_revenue * 12
                    elif 'monthly multiple' in label:
                        try:
                            data['multiple'] = float(re.search(r'(\d+)x', value).group(1))
                        except (ValueError, TypeError, AttributeError):
                            pass
                    elif 'business created' in label:
                        try:
                            data['established_year'] = int(value)
                        except (ValueError, TypeError):
                            pass
        
        monetization_tag = soup.select_one('div.monetization div.value')
        if monetization_tag:
            data['industry'] = monetization_tag.text.strip()

        return data