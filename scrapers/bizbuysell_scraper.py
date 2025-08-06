from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import json

class BizBuySellScraper(BaseScraper):
    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from all pages for a given search URL."""
        listing_urls = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                self.logger.info(f"Reached max pages limit: {max_pages}")
                break

            url = f"{search_url}{page}/"
            soup = self.get_page(url)
            
            if not soup:
                self.logger.info(f"No content found for {url}, stopping pagination.")
                break

            initial_listing_count = len(listing_urls)

            # Prioritize JSON-LD data
            json_ld_script = soup.find('script', {'type': 'application/ld+json'})
            if json_ld_script:
                try:
                    data = json.loads(json_ld_script.string)
                    if 'about' in data:
                        for item in data['about']:
                            if 'item' in item and 'url' in item['item']:
                                listing_url = item['item']['url']
                                if listing_url not in listing_urls:
                                    listing_urls.append(listing_url)
                except (json.JSONDecodeError, KeyError) as e:
                    self.logger.error(f"Error parsing JSON-LD on page {page}: {e}")

            # Fallback to HTML selectors if JSON-LD fails or is incomplete
            if len(listing_urls) == initial_listing_count:
                listings = soup.select('div.search-result-card a')
                if listings:
                    for listing in listings:
                        href = listing.get('href')
                        if href:
                            if href.startswith('/'):
                                href = self.base_url + href
                            if href not in listing_urls:
                                listing_urls.append(href)
            
            # If we didn't find any new listings on this page, stop.
            if len(listing_urls) == initial_listing_count:
                self.logger.info(f"No new listings found on page {page}. Stopping pagination.")
                break
            
            self.logger.info(f"Found {len(listing_urls) - initial_listing_count} new listings on page {page}")
            page += 1
            
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        # Try with rendering for better data extraction on individual pages
        soup = self.get_page(url, render=True)
        if not soup:
            # Fallback to non-rendered
            soup = self.get_page(url)
            if not soup:
                return None
        
        data = {'listing_url': url}
        
        # Prioritize JSON-LD data
        json_ld_script = soup.find('script', {'type': 'application/ld+json'})
        if json_ld_script:
            try:
                ld_data = json.loads(json_ld_script.string)
                if isinstance(ld_data, dict):
                    data['title'] = ld_data.get('name')
                    data['description'] = ld_data.get('description')
                    if 'offers' in ld_data:
                        data['price'] = float(ld_data['offers'].get('price', 0))
                    if 'availableAtOrFrom' in ld_data.get('offers', {}):
                        address = ld_data['offers']['availableAtOrFrom'].get('address', {})
                        city = address.get('addressLocality')
                        state = address.get('addressRegion')
                        if city and state:
                            data['location'] = f"{city}, {state}".strip()
                        elif city:
                            data['location'] = city.strip()
                        elif state:
                            data['location'] = state.strip()
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.error(f"Error parsing JSON-LD for {url}: {e}")
        
        # Fallback to HTML scraping if JSON-LD is incomplete or fails
        if not data.get('title'):
            title_tag = soup.select_one('h1.font-h1-new') or soup.select_one('h1')
            data['title'] = title_tag.text.strip() if title_tag else 'Title not found'
            
        if not data.get('description'):
            desc_tag = soup.select_one('div.business-description') or soup.select_one('div.description')
            data['description'] = desc_tag.text.strip() if desc_tag else 'Description not found'

        # Financials are often in a dedicated section
        # Look for the financials div which contains all financial data
        financials_div = soup.select_one('div.financials')
        if financials_div:
            # Extract each financial metric from the p tags
            financial_items = financials_div.select('p')
            for item in financial_items:
                title_elem = item.select_one('span.title')
                if title_elem:
                    label = title_elem.text.strip().lower()
                    # Get the value - it's usually in the next span or the parent p
                    value_text = item.text.replace(title_elem.text, '').strip()
                    
                    if 'asking price' in label and not data.get('price'):
                        data['price'] = self.parse_price(value_text)
                    elif 'cash flow' in label or 'sde' in label:
                        if 'not disclosed' not in value_text.lower():
                            data['cash_flow'] = self.parse_price(value_text)
                    elif 'gross revenue' in label or ('revenue' in label and 'gross' in label):
                        if 'not disclosed' not in value_text.lower():
                            data['revenue'] = self.parse_price(value_text)
                    elif 'ebitda' in label:
                        if 'not disclosed' not in value_text.lower():
                            data['ebitda'] = self.parse_price(value_text)
        
        # Fallback if financials div not found
        if not data.get('price'):
            price_tag = soup.select_one('span.price.asking') or soup.select_one('div.asking-price')
            if price_tag:
                data['price'] = self.parse_price(price_tag.text)
        
        # Pattern matching as additional fallback
        if not data.get('revenue') or not data.get('cash_flow'):
            page_text = soup.get_text()
            
            # Note: BizBuySell often requires login to see full financial details
            # We can only get what's publicly available
            import re
            
            # Look for revenue if not found
            if not data.get('revenue'):
                revenue_match = re.search(r'Gross Revenue[:\s]*\$?([\d,]+)', page_text, re.I)
                if revenue_match:
                    data['revenue'] = self.parse_price(revenue_match.group(1))
            
            # Look for cash flow if not found
            if not data.get('cash_flow'):
                cf_match = re.search(r'Cash Flow.*?(?:SDE)?[:\s]*\$?([\d,]+)', page_text, re.I)
                if cf_match:
                    data['cash_flow'] = self.parse_price(cf_match.group(1))
        
        # Try to extract industry/business type
        if not data.get('industry'):
            # Look for category or type
            category_elem = soup.select_one('div.category, span.category, div.business-type')
            if category_elem:
                data['industry'] = category_elem.text.strip()
        
        # Extract established year
        if not data.get('established_year'):
            # Look in the financials area first
            if financials_div:
                established_text = financials_div.text
                year_match = re.search(r'Established[:\s]*(\d{4})', established_text, re.I)
                if year_match:
                    try:
                        data['established_year'] = int(year_match.group(1))
                    except ValueError:
                        pass
            
            # Fallback to full page search
            if not data.get('established_year'):
                page_text = soup.get_text()
                year_match = re.search(r'Established[:\s]*(\d{4})', page_text, re.I)
                if year_match:
                    try:
                        data['established_year'] = int(year_match.group(1))
                    except ValueError:
                        pass

        return data