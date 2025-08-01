"""
Acquire.com Scraper
Scrapes business listings from Acquire marketplace
"""
from typing import Dict, List, Optional
import re
from .base_scraper import BaseScraper

class AcquireScraper(BaseScraper):
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from Acquire.com"""
        listing_urls = []
        
        # Acquire uses different category pages
        categories = [
            'amazon-fba-for-sale',
            'ecommerce-for-sale',
            'saas-for-sale',
            'marketplace-for-sale'
        ]
        
        for category in categories:
            self.logger.info(f"Scraping Acquire.com category: {category}")
            
            # Acquire uses React, so we might get limited results from static scraping
            page_url = f"{self.base_url}/{category}/"
            soup = self.get_page(page_url)
            
            if not soup:
                continue
            
            # Look for listing links - Acquire's structure varies
            # Try multiple selectors
            selectors = [
                'a[href*="/startup/"]',
                'a[href*="/business/"]',
                'div.listing-card a',
                'article a[href]'
            ]
            
            for selector in selectors:
                links = soup.select(selector)
                if links:
                    for link in links:
                        href = link.get('href', '')
                        if href and ('/startup/' in href or '/business/' in href):
                            full_url = href if href.startswith('http') else f"https://app.acquire.com{href}"
                            if full_url not in listing_urls:
                                listing_urls.append(full_url)
                    break
            
            # Also try to find links in JavaScript content
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Look for URLs in JSON data
                    urls = re.findall(r'"url"\s*:\s*"([^"]+/(?:startup|business)/[^"]+)"', script.string)
                    for url in urls:
                        full_url = url if url.startswith('http') else f"https://app.acquire.com{url}"
                        if full_url not in listing_urls:
                            listing_urls.append(full_url)
            
            if max_pages and len(listing_urls) >= max_pages * 20:
                break
        
        return listing_urls[:max_pages * 20] if max_pages else listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single Acquire.com listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        listing_data = {
            'listing_url': url,
            'source_site': self.name
        }
        
        # Title - Acquire often puts titles in h1 or h2
        title_elem = soup.find('h1') or soup.find('h2')
        if title_elem:
            listing_data['title'] = title_elem.get_text().strip()
        
        # Price - look for various price patterns
        price_patterns = [
            r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'\$?([\d,]+(?:\.\d+)?[KkMm]?)\s*asking'
        ]
        
        page_text = soup.get_text()
        for pattern in price_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                listing_data['price'] = self.parse_price(match.group(1))
                break
        
        # Revenue
        revenue_patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'annual\s*revenue[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'arr[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
            r'mrr[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)'
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                revenue = self.parse_price(match.group(1))
                # Convert MRR to annual
                if 'mrr' in pattern.lower() and revenue:
                    revenue *= 12
                listing_data['revenue'] = revenue
                break
        
        # Profit/Cash Flow
        profit_patterns = [
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
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
        multiple_match = re.search(r'([\d.]+)x?\s*multiple', page_text, re.I)
        if multiple_match:
            try:
                listing_data['multiple'] = float(multiple_match.group(1))
            except:
                pass
        
        # Industry/Type
        industry_keywords = {
            'saas': 'SaaS',
            'software': 'Software',
            'ecommerce': 'E-commerce',
            'e-commerce': 'E-commerce',
            'amazon': 'Amazon FBA',
            'fba': 'Amazon FBA',
            'marketplace': 'Marketplace',
            'app': 'Mobile App',
            'subscription': 'Subscription'
        }
        
        for keyword, industry in industry_keywords.items():
            if keyword in page_text.lower():
                listing_data['industry'] = industry
                break
        
        # Description
        desc_elem = soup.find('div', class_='description') or soup.find('section', class_='about')
        if desc_elem:
            listing_data['description'] = desc_elem.get_text().strip()[:1000]
        else:
            # Try to find any substantial text block
            for tag in ['article', 'section', 'div']:
                elems = soup.find_all(tag)
                for elem in elems:
                    text = elem.get_text().strip()
                    if len(text) > 200 and not text.startswith('<'):
                        listing_data['description'] = text[:1000]
                        break
                if listing_data.get('description'):
                    break
        
        return listing_data if listing_data.get('title') else None