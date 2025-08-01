"""
WebsiteClosers Scraper
Scrapes business listings from WebsiteClosers.com
"""
from typing import Dict, List, Optional
import re
from .base_scraper import BaseScraper

class WebsiteClosersScraper(BaseScraper):
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from WebsiteClosers"""
        listing_urls = []
        
        # WebsiteClosers has a listings page
        self.logger.info("Scraping WebsiteClosers listings")
        
        # Try multiple possible URLs
        listing_pages = [
            f"{self.base_url}/listings/",
            f"{self.base_url}/current-listings/",
            f"{self.base_url}/businesses-for-sale/",
            f"{self.base_url}/portfolio/"
        ]
        
        for page_url in listing_pages:
            soup = self.get_page(page_url)
            if not soup:
                continue
            
            # Find listing links
            # WebsiteClosers uses various structures
            selectors = [
                'a.listing-link',
                'div.listing-item a',
                'article.listing a',
                'div.property-box a',
                'h3.listing-title a',
                'a[href*="/listing/"]',
                'a[href*="/business/"]'
            ]
            
            for selector in selectors:
                links = soup.select(selector)
                if links:
                    for link in links:
                        href = link.get('href', '')
                        if href and not href.startswith('#'):
                            full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                            if full_url not in listing_urls and '/listing/' in full_url:
                                listing_urls.append(full_url)
                    if listing_urls:  # If we found links, don't try other selectors
                        break
            
            # Also try to find links in listing containers
            containers = soup.find_all('div', class_=re.compile(r'listing|property|business'))
            for container in containers:
                link = container.find('a')
                if link and link.get('href'):
                    href = link['href']
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    if full_url not in listing_urls and any(x in full_url for x in ['/listing/', '/business/', '/property/']):
                        listing_urls.append(full_url)
            
            if listing_urls:  # If we found listings, don't try other pages
                break
            
            if max_pages and len(listing_urls) >= max_pages * 20:
                break
        
        return listing_urls[:max_pages * 20] if max_pages else listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single WebsiteClosers listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        listing_data = {
            'listing_url': url,
            'source_site': self.name
        }
        
        # Title
        title_elem = soup.find('h1', class_='listing-title') or \
                    soup.find('h1', class_='entry-title') or \
                    soup.find('h1')
        if title_elem:
            listing_data['title'] = title_elem.get_text().strip()
        
        # Look for a details table or metrics section
        details_table = soup.find('table', class_='listing-details') or \
                       soup.find('div', class_='listing-details') or \
                       soup.find('ul', class_='property-details')
        
        page_text = soup.get_text()
        
        # Price
        price_patterns = [
            r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'list\s*price[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'\$?([\d,]+(?:\.\d+)?[KkMm]?)\s*(?:asking|list)'
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                listing_data['price'] = self.parse_price(match.group(1))
                break
        
        # Revenue
        revenue_patterns = [
            r'gross\s*revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'annual\s*revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)'
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                listing_data['revenue'] = self.parse_price(match.group(1))
                break
        
        # Cash Flow/Profit
        profit_patterns = [
            r'cash\s*flow[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'net\s*profit[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'net\s*income[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'sde[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)'
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                listing_data['cash_flow'] = self.parse_price(match.group(1))
                break
        
        # Multiple
        multiple_match = re.search(r'multiple[:\s]*([\d.]+)x?', page_text, re.I)
        if not multiple_match:
            multiple_match = re.search(r'([\d.]+)x?\s*multiple', page_text, re.I)
        if multiple_match:
            try:
                listing_data['multiple'] = float(multiple_match.group(1))
            except:
                pass
        
        # Industry
        industry_patterns = [
            r'industry[:\s]*([^,\n]+)',
            r'type[:\s]*([^,\n]+)',
            r'category[:\s]*([^,\n]+)'
        ]
        
        for pattern in industry_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                listing_data['industry'] = match.group(1).strip()
                break
        
        # If no industry found, check for common keywords
        if not listing_data.get('industry'):
            industry_keywords = {
                'ecommerce': 'E-commerce',
                'e-commerce': 'E-commerce',
                'amazon': 'Amazon FBA',
                'fba': 'Amazon FBA',
                'saas': 'SaaS',
                'software': 'Software',
                'content': 'Content/Publishing',
                'affiliate': 'Affiliate Marketing',
                'dropship': 'Dropshipping'
            }
            
            for keyword, industry in industry_keywords.items():
                if keyword in page_text.lower():
                    listing_data['industry'] = industry
                    break
        
        # Location
        location_match = re.search(r'location[:\s]*([^,\n]+)', page_text, re.I)
        if location_match:
            listing_data['location'] = location_match.group(1).strip()
        
        # Year established
        year_match = re.search(r'established[:\s]*(\d{4})', page_text, re.I)
        if year_match:
            listing_data['established_year'] = int(year_match.group(1))
        
        # Description
        desc_elem = soup.find('div', class_='listing-description') or \
                   soup.find('div', class_='content-area') or \
                   soup.find('section', class_='description')
        if desc_elem:
            listing_data['description'] = desc_elem.get_text().strip()[:1000]
        
        return listing_data if listing_data.get('title') else None