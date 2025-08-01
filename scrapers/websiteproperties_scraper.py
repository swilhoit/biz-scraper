from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re

class WebsitePropertiesScraper(BaseScraper):
    """Scraper for WebsiteProperties.com, specializing in high-value digital assets."""
    
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from the main listings page."""
        listing_urls = []
        search_urls = self.site_config.get('search_urls', [self.site_config.get('search_url')])

        for search_url in filter(None, search_urls):
            page = 1
            while not max_pages or page <= max_pages:
                url = f"{search_url}/page/{page}/" if page > 1 else search_url
                soup = self.get_page(url)
                if not soup:
                    break
                
                listings = soup.select('article.listing-card h3.mb-2 a')
                if not listings:
                    self.logger.info(f"No listings found on {url}")
                    break
                    
                for link in listings:
                    href = link.get('href')
                    if href and href not in listing_urls:
                        listing_urls.append(href)
                
                self.logger.info(f"Found {len(listings)} listings on page {page}")
                if not soup.select_one('a.next.page-numbers'):
                    break
                page += 1
            
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing page."""
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {'listing_url': url}
        
        title_tag = soup.select_one('h2.blog-single-title')
        data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
        
        financials = self._extract_financials(soup)
        data.update(financials)

        desc_tag = soup.select_one('div.listing-single-content p')
        data['description'] = desc_tag.text.strip() if desc_tag else 'Description not found'
        
        details = self._extract_details(soup)
        data.update(details)
        
        if data.get('price') and data.get('cash_flow'):
            try:
                data['multiple'] = round(data['price'] / data['cash_flow'], 2)
            except (TypeError, ZeroDivisionError):
                pass
                
        return data

    def _extract_financials(self, soup) -> Dict:
        """Extract financial data from the page."""
        financials = {}
        financial_table = soup.select_one('table.listing-data-table')
        if financial_table:
            for row in financial_table.find_all('tr'):
                cells = row.find_all(['th', 'td'])
                if len(cells) == 2:
                    label = cells[0].text.strip().lower()
                    value = cells[1].text.strip()

                    if 'gross revenue' in label:
                        financials['revenue'] = self.parse_price(value)
                    elif 'cash flow' in label:
                        financials['cash_flow'] = self.parse_price(value)

        price_tag = soup.select_one('h5.mt-4')
        if price_tag:
            price_text = price_tag.text.lower().replace('asking price:', '').strip()
            if 'accepting offers' in price_text:
                financials['price'] = 0
            else:
                financials['price'] = self.parse_price(price_text)
            
        return financials

    def _extract_details(self, soup) -> Dict:
        """Extract additional details like established year and employee count."""
        details = {}
        details_table = soup.select_one('table.listing-data-table')
        if details_table:
            for row in details_table.find_all('tr'):
                cells = row.find_all(['th', 'td'])
                if len(cells) == 2:
                    label = cells[0].text.strip().lower()
                    value = cells[1].text.strip()

                    if 'year established' in label:
                        try:
                            details['established_year'] = int(re.search(r'\d{4}', value).group())
                        except (ValueError, AttributeError):
                            details['established_year'] = None
                    elif 'employees' in label:
                        try:
                            details['employees'] = int(re.search(r'\d+', value).group())
                        except (ValueError, AttributeError):
                            details['employees'] = None
                    elif 'industry' in label:
                        details['industry'] = value
                    elif 'location' in label:
                        details['location'] = value

        return details