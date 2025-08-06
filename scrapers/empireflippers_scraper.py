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
        # Use rendering for better data extraction
        soup = self.get_page(url, render=True)
        if not soup:
            return None
        
        data = {'listing_url': url}
        
        # Title
        title_tag = soup.select_one('h1')
        data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
        
        # Get page text for pattern matching
        page_text = soup.get_text()
        
        # Extract price - look for the info-price div and get all text
        price_elem = soup.select_one('div.info-price')
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            # Remove "Listing Price" label and extract number
            price_match = re.search(r'\$([\d,]+)', price_text)
            if price_match:
                data['price'] = self.parse_price(price_match.group(1))
        else:
            # Fallback pattern
            price_match = re.search(r'(?:Listing Price)[:\s]*\$([\d,]+)', page_text, re.I)
            if price_match:
                data['price'] = self.parse_price(price_match.group(1))
        
        # Extract revenue - look for the pattern "Avg. Monthly Revenue $X"
        revenue_match = re.search(r'Avg\.\s*Monthly\s*Revenue\s*\$([\d,]+)', page_text, re.I)
        if revenue_match:
            monthly_revenue = self.parse_price(revenue_match.group(1))
            if monthly_revenue:
                data['revenue'] = monthly_revenue * 12  # Convert to annual
        
        # Extract profit/cash flow - look for "Avg. Monthly Profit $X"
        profit_match = re.search(r'Avg\.\s*Monthly\s*Profit\s*\$([\d,]+)', page_text, re.I)
        if profit_match:
            monthly_profit = self.parse_price(profit_match.group(1))
            if monthly_profit:
                data['cash_flow'] = monthly_profit * 12  # Convert to annual
        
        # Extract multiple
        multiple_match = re.search(r'(?:Multiple)[:\s]*([\d.]+)x?', page_text, re.I)
        if multiple_match:
            try:
                data['multiple'] = float(multiple_match.group(1))
            except ValueError:
                pass
        
        # Extract business type/monetization
        monetization_match = re.search(r'(?:Monetizations?|Business Type)[:\s]*([^\n]+)', page_text, re.I)
        if monetization_match:
            data['industry'] = monetization_match.group(1).strip()
        
        # Extract year established
        year_match = re.search(r'(?:Business Created|Established|Founded)[:\s]*(\d{4})', page_text, re.I)
        if year_match:
            try:
                data['established_year'] = int(year_match.group(1))
            except ValueError:
                pass
        
        # Description - try multiple selectors
        desc_selectors = [
            'div.listing-description',
            'div.business-description',
            'div.description',
            'meta[name="description"]'
        ]
        
        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                if desc_elem.name == 'meta':
                    data['description'] = desc_elem.get('content', '')
                else:
                    data['description'] = desc_elem.text.strip()
                break
        
        if not data.get('description'):
            # Extract from page text
            desc_match = re.search(r'(?:Description|Overview|About)[:\s]*([^\n]{50,500})', page_text, re.I)
            if desc_match:
                data['description'] = desc_match.group(1).strip()
            else:
                data['description'] = 'See listing for details'

        return data