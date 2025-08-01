"""
FE International Scraper
Scrapes business listings from FE International
"""
from typing import Dict, List, Optional
import re
from .base_scraper import BaseScraper

class FEInternationalScraper(BaseScraper):
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from FE International"""
        listing_urls = []
        
        # FE International has different business categories
        categories = [
            'buy-a-website',
            'buy-a-website/amazon-fba',
            'buy-a-website/ecommerce',
            'buy-a-website/saas',
            'buy-a-website/content'
        ]
        
        for category in categories:
            self.logger.info(f"Scraping FE International category: {category}")
            
            page_url = f"{self.base_url}/{category}/"
            soup = self.get_page(page_url)
            
            if not soup:
                continue
            
            # Find listing cards
            listing_cards = soup.find_all('div', class_='listing-card') or \
                          soup.find_all('article', class_='business-listing') or \
                          soup.find_all('div', class_='property-item')
            
            for card in listing_cards:
                link = card.find('a')
                if link and link.get('href'):
                    href = link['href']
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    if full_url not in listing_urls:
                        listing_urls.append(full_url)
            
            # Also try to find links directly
            links = soup.find_all('a', href=re.compile(r'/portfolio/|/listing/|/business/'))
            for link in links:
                href = link.get('href', '')
                if href:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    if full_url not in listing_urls and '/portfolio/' in full_url:
                        listing_urls.append(full_url)
            
            if max_pages and len(listing_urls) >= max_pages * 20:
                break
        
        return listing_urls[:max_pages * 20] if max_pages else listing_urls
    
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