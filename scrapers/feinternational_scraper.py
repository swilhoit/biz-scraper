"""
FE International Scraper
Scrapes business listings from FE International
"""
from typing import Dict, List, Optional
import re
from .base_scraper import BaseScraper
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

class FEInternationalScraper(BaseScraper):
    def get_page(self, url: str):
        """Override to use Playwright for JS-heavy page"""
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            try:
                page.goto(url, wait_until='networkidle')
                page.wait_for_timeout(5000)  # Wait for 5 seconds
                content = page.content()
                if not content:
                    return None
                return BeautifulSoup(content, 'lxml')
            except Exception as e:
                self.logger.error(f"Error fetching {url} with Playwright: {e}")
                return None
            finally:
                browser.close()
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from the main listings page."""
        listing_urls = []
        search_url = self.site_config.get('search_url')
        if not search_url:
            self.logger.error("No search URL configured for FEInternational")
            return listing_urls

        soup = self.get_page(search_url)
        if not soup:
            return listing_urls

        listing_cards = soup.select('a.card_businesses_item')
        for card in listing_cards:
            href = card.get('href')
            if href:
                full_url = href if href.startswith('http') else f"{self.site_config['base_url']}{href}"
                if full_url not in listing_urls:
                    listing_urls.append(full_url)
        
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single FE International listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        listing_data = {
            'listing_url': url,
            'source_site': self.name
        }
        
        # Title
        title_elem = soup.find('h1', class_='listing-title') or \
                    soup.find('h1') or \
                    soup.find('h2', class_='title')
        if title_elem:
            listing_data['title'] = title_elem.get_text().strip()
        
        # Extract key metrics - FE International often uses a metrics section
        metrics_section = soup.find('div', class_='key-metrics') or \
                         soup.find('section', class_='metrics') or \
                         soup.find('div', class_='listing-stats')
        
        if metrics_section:
            metrics_text = metrics_section.get_text()
            
            # Price
            price_match = re.search(r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', metrics_text, re.I)
            if price_match:
                listing_data['price'] = self.parse_price(price_match.group(1))
            
            # Revenue
            revenue_match = re.search(r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', metrics_text, re.I)
            if revenue_match:
                listing_data['revenue'] = self.parse_price(revenue_match.group(1))
            
            # Profit
            profit_match = re.search(r'(?:profit|net\s*income|ebitda)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', metrics_text, re.I)
            if profit_match:
                listing_data['cash_flow'] = self.parse_price(profit_match.group(1))
            
            # Multiple
            multiple_match = re.search(r'([\d.]+)x?\s*multiple', metrics_text, re.I)
            if multiple_match:
                try:
                    listing_data['multiple'] = float(multiple_match.group(1))
                except:
                    pass
        
        # Fallback to searching entire page
        page_text = soup.get_text()
        
        if not listing_data.get('price'):
            price_patterns = [
                r'price[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
                r'\$?([\d,]+(?:\.\d+)?[KkMm]?)\s*asking',
                r'for\s*sale[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)'
            ]
            for pattern in price_patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    listing_data['price'] = self.parse_price(match.group(1))
                    break
        
        if not listing_data.get('revenue'):
            revenue_patterns = [
                r'annual\s*revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
                r'yearly\s*revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
                r'ttm\s*revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)'
            ]
            for pattern in revenue_patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    listing_data['revenue'] = self.parse_price(match.group(1))
                    break
        
        # Industry/Type
        industry_section = soup.find('div', class_='business-type') or \
                          soup.find('span', class_='category')
        if industry_section:
            listing_data['industry'] = industry_section.get_text().strip()
        else:
            # Determine from URL or content
            if 'amazon-fba' in url.lower():
                listing_data['industry'] = 'Amazon FBA'
            elif 'ecommerce' in url.lower():
                listing_data['industry'] = 'E-commerce'
            elif 'saas' in url.lower():
                listing_data['industry'] = 'SaaS'
            elif 'content' in url.lower():
                listing_data['industry'] = 'Content/Publishing'
        
        # Location
        location_elem = soup.find('span', class_='location') or \
                       soup.find('div', class_='business-location')
        if location_elem:
            listing_data['location'] = location_elem.get_text().strip()
        
        # Year established
        year_match = re.search(r'established[:\s]*(\d{4})', page_text, re.I)
        if year_match:
            listing_data['established_year'] = int(year_match.group(1))
        
        # Description
        desc_elem = soup.find('div', class_='listing-description') or \
                   soup.find('section', class_='description') or \
                   soup.find('div', class_='overview')
        if desc_elem:
            listing_data['description'] = desc_elem.get_text().strip()[:1000]
        
        return listing_data if listing_data.get('title') else None