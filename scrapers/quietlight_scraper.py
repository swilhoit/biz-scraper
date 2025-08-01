from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class QuietLightScraper(BaseScraper):
    """Scraper for QuietLight - known for high-value online businesses."""
    
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages."""
        listing_urls = []
        search_urls = self.site_config.get('search_urls', [self.site_config.get('search_url')])
        
        for base_url in filter(None, search_urls):
            page = 1
            while not max_pages or page <= max_pages:
                url = f"{base_url.rstrip('/')}/page/{page}/" if page > 1 else base_url
                soup = self.get_page(url)
                if not soup:
                    break
                
                listings = soup.select('div.listing-card a.listing-card__link')
                if not listings:
                    self.logger.info(f"No listings found on {url}")
                    break
                
                found_on_page = 0
                for link in listings:
                    href = link.get('href')
                    if href and '/listings/' in href:
                        full_url = self.base_url + href if href.startswith('/') else href
                        if full_url not in listing_urls:
                            listing_urls.append(full_url)
                            found_on_page += 1
                
                self.logger.info(f"Found {found_on_page} new listings on page {page}")
                if not soup.select_one('a.next'):
                    break
                page += 1
        return listing_urls

    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing, focusing on structured data."""
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {'listing_url': url}

        # Title
        title_tag = soup.select_one('h3')
        data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
        
        # Financials are key
        financials = self._extract_financials(soup)
        data.update(financials)

        # Description
        desc_tag = soup.select_one('div.inform_price.single_business_price p')
        data['description'] = desc_tag.text.strip() if desc_tag else 'Description not found'
        
        # Industry/Category
        industry_tag = soup.select_one('div.listing-card__category-name')
        data['industry'] = industry_tag.text.strip() if industry_tag else 'Industry not found'
        
        # Other details
        details_text = soup.get_text().lower()
        data['seller_financing'] = 'seller financing' in details_text
        
        # Calculate multiple if possible
        if data.get('price') and data.get('cash_flow'):
            try:
                data['multiple'] = round(data['price'] / data['cash_flow'], 2)
            except (TypeError, ZeroDivisionError):
                pass
        
        return data

    def _extract_financials(self, soup) -> Dict:
        """Helper to extract financial data from a listing page."""
        financials = {}
        financial_section = soup.select_one('div.inform_revenue.single_business')
        if financial_section:
            items = financial_section.select('li')
            for item in items:
                label_tag = item.select_one('h6')
                value_tag = item.select_one('p')
                if label_tag and value_tag:
                    label = label_tag.text.lower()
                    value = value_tag.text
                    
                    if 'revenue' in label:
                        financials['revenue'] = self.parse_price(value)
                    elif 'income' in label:
                        financials['cash_flow'] = self.parse_price(value)
                    elif 'multiple' in label:
                        try:
                            financials['multiple'] = float(value)
                        except (ValueError, TypeError):
                            pass
        
        price_tag = soup.select_one('div.inform_price.single_business_price h4')
        if price_tag:
            price_text = price_tag.text.lower()
            price_text = price_text.replace('asking price:', '').replace('+ inventory', '').strip()
            if 'accepting offers' in price_text:
                financials['price'] = 0  # Or some other indicator for "Accepting Offers"
            else:
                financials['price'] = self.parse_price(price_text)

        return financials