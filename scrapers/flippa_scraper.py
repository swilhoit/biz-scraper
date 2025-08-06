from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class FlippaScraper(BaseScraper):
    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs by parsing HTML links"""
        listing_urls = []
        page = 1

        while not max_pages or page <= max_pages:
            # Flippa uses offset parameter for pagination
            offset = (page - 1) * 50  # Assuming 50 listings per page
            url = f"{search_url}&offset={offset}"
            
            # Need rendering for Flippa
            soup = self.get_page(url, render=True)
            if not soup:
                break
            
            # Find all links that match Flippa's listing URL pattern
            all_links = soup.find_all('a', href=True)
            new_listings = []
            
            for link in all_links:
                href = link['href']
                # Flippa listing URLs are like /11052740 or https://flippa.com/11052740
                if re.search(r'/\d{7,}$', href):
                    if href.startswith('/'):
                        href = f"{self.base_url}{href}"
                    if href not in listing_urls and self.base_url in href:
                        new_listings.append(href)
                        listing_urls.append(href)
            
            if not new_listings:
                self.logger.info(f"No new listings found on page {page}")
                break
                
            self.logger.info(f"Found {len(new_listings)} new listings on page {page}")
            page += 1
        
        return listing_urls

    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing using HTML parsing"""
        soup = self.get_page(url, render=True)
        if not soup:
            return None
        
        data = {'listing_url': url}
        
        # Title - usually in h1 or meta tags
        title_tag = soup.find('h1') or soup.find('meta', {'property': 'og:title'})
        if title_tag:
            data['title'] = title_tag.text.strip() if hasattr(title_tag, 'text') else title_tag.get('content', '')
        else:
            data['title'] = 'Title not found'
        
        # Price - look for asking price specifically
        price_elem = soup.select_one('div[class*="price"]')
        if price_elem:
            price_text = price_elem.get_text()
            # Extract the main price (not inventory)
            price_match = re.search(r'USD\s*\$?([\d,]+)', price_text)
            if price_match:
                data['price'] = self.parse_price(price_match.group(1))
        else:
            # Fallback pattern
            page_text_for_price = soup.get_text()
            price_match = re.search(r'(?:Asking Price|Buy It Now)[:\s]*\$?([\d,]+)', page_text_for_price, re.I)
            if price_match:
                data['price'] = self.parse_price(price_match.group(1))
        
        # Description
        desc_tag = soup.find('meta', {'name': 'description'}) or soup.find('meta', {'property': 'og:description'})
        if desc_tag:
            data['description'] = desc_tag.get('content', '')
        
        # Try to find financial information
        page_text = soup.get_text()
        
        # Revenue patterns
        revenue_match = re.search(r'(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', page_text, re.IGNORECASE)
        if revenue_match:
            data['revenue'] = self.parse_price(revenue_match.group(1))
        
        # Profit/Cash flow patterns  
        profit_match = re.search(r'(?:profit|cash flow|net income)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', page_text, re.IGNORECASE)
        if profit_match:
            data['cash_flow'] = self.parse_price(profit_match.group(1))
        
        # Industry/Category
        category_match = re.search(r'(?:category|industry|niche)[:\s]*([^\n,]+)', page_text, re.IGNORECASE)
        if category_match:
            data['industry'] = category_match.group(1).strip()
        
        return data