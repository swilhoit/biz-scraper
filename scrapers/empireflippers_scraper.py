from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re
import json

class EmpireFlippersScraper(BaseScraper):
    """Scraper for EmpireFlippers - JavaScript-heavy site with high-value listings"""
    
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from marketplace"""
        listing_urls = []
        
        # EmpireFlippers has different URLs for different business types
        search_urls = [
            self.site_config.get('search_url'),
            self.site_config.get('amazon_url')
        ]
        
        for base_url in search_urls:
            if not base_url:
                continue
                
            page = 1
            while True:
                if max_pages and page > max_pages:
                    break
                
                # EmpireFlippers uses page parameter
                url = f"{base_url}?page={page}" if '?' not in base_url else f"{base_url}&page={page}"
                
                soup = self.get_page(url)
                if not soup:
                    break
                
                # Look for listing cards or JSON data
                # EmpireFlippers often loads data via JavaScript
                listings = soup.find_all('div', class_=['listing-item', 'listing-card', 'marketplace-listing'])
                
                # Also check for links
                if not listings:
                    listings = soup.find_all('a', href=re.compile(r'/listing/[^/]+/?$'))
                
                # Check if data is in JSON format
                if not listings:
                    scripts = soup.find_all('script', type='application/json')
                    for script in scripts:
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, dict) and 'listings' in data:
                                for listing in data['listings']:
                                    if 'slug' in listing:
                                        listing_url = f"{self.base_url}/listing/{listing['slug']}/"
                                        listing_urls.append(listing_url)
                        except:
                            pass
                
                if listings:
                    for listing in listings:
                        if listing.name == 'div':
                            link = listing.find('a', href=True)
                            if link:
                                href = link.get('href')
                            else:
                                continue
                        else:
                            href = listing.get('href')
                        
                        if href:
                            if href.startswith('/'):
                                href = self.base_url + href
                            if href not in listing_urls and '/listing/' in href:
                                listing_urls.append(href)
                
                self.logger.info(f"Found {len(listing_urls)} total listings after page {page}")
                
                # Check for next page
                if not soup.find('a', class_=['next', 'pagination-next']) and len(listings) == 0:
                    break
                    
                page += 1
        
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        data = {
            'listing_url': url,
            'title': None,
            'price': None,
            'revenue': None,
            'cash_flow': None,
            'location': None,
            'industry': None,
            'description': None,
            'multiple': None
        }
        
        # EmpireFlippers often has data in JSON-LD
        json_ld = soup.find('script', type='application/ld+json')
        if json_ld:
            try:
                ld_data = json.loads(json_ld.string)
                if isinstance(ld_data, dict):
                    data['title'] = ld_data.get('name')
                    data['description'] = ld_data.get('description')
                    if 'offers' in ld_data:
                        data['price'] = float(ld_data['offers'].get('price', 0))
            except:
                pass
        
        # Title - EmpireFlippers uses listing codes like "60462"
        if not data['title']:
            title_elem = soup.find('h1', class_='listing-title')
            if not title_elem:
                title_elem = soup.find('h1')
            if title_elem:
                data['title'] = title_elem.text.strip()
                
                # Generate business name from niche if title is just a code
                if re.match(r'^\d+$', data['title']):
                    niche_elem = soup.find(['span', 'div'], text=re.compile(r'Niche|Category'))
                    if niche_elem and niche_elem.next_sibling:
                        niche = niche_elem.next_sibling.text.strip()
                        monetization_elem = soup.find(['span', 'div'], text=re.compile(r'Monetization'))
                        if monetization_elem and monetization_elem.next_sibling:
                            monetization = monetization_elem.next_sibling.text.strip()
                            data['title'] = f"{niche} {monetization} Business #{data['title']}"
        
        # Financial data - often in a summary section
        summary_section = soup.find('div', class_=['listing-summary', 'financial-summary'])
        if summary_section:
            # Price
            price_elem = summary_section.find(text=re.compile(r'Price|Asking'))
            if price_elem:
                price_match = re.search(r'\$?([\d,]+)', price_elem.parent.text)
                if price_match:
                    data['price'] = float(price_match.group(1).replace(',', ''))
            
            # Monthly profit (convert to annual)
            profit_elem = summary_section.find(text=re.compile(r'Monthly Net Profit'))
            if profit_elem:
                profit_match = re.search(r'\$?([\d,]+)', profit_elem.parent.text)
                if profit_match:
                    monthly_profit = float(profit_match.group(1).replace(',', ''))
                    data['cash_flow'] = monthly_profit * 12
            
            # Multiple
            multiple_elem = summary_section.find(text=re.compile(r'Multiple'))
            if multiple_elem:
                multiple_match = re.search(r'([\d.]+)x?', multiple_elem.parent.text)
                if multiple_match:
                    data['multiple'] = float(multiple_match.group(1))
        
        # Metrics table
        metrics_table = soup.find('table', class_='metrics')
        if not metrics_table:
            metrics_table = soup.find('div', class_='listing-metrics')
        
        if metrics_table:
            rows = metrics_table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].text.strip().lower()
                    value = cells[1].text.strip()
                    
                    if 'price' in label:
                        data['price'] = self.parse_price(value)
                    elif 'revenue' in label and 'monthly' in label:
                        monthly_rev = self.parse_price(value)
                        if monthly_rev:
                            data['revenue'] = monthly_rev * 12
                    elif 'profit' in label and 'monthly' in label:
                        monthly_profit = self.parse_price(value)
                        if monthly_profit:
                            data['cash_flow'] = monthly_profit * 12
                    elif 'multiple' in label:
                        multiple_match = re.search(r'([\d.]+)', value)
                        if multiple_match:
                            data['multiple'] = float(multiple_match.group(1))
        
        # Industry/Monetization
        monetization_elem = soup.find(['span', 'div'], class_=['monetization', 'business-type'])
        if monetization_elem:
            data['industry'] = monetization_elem.text.strip()
        
        # Description
        if not data['description']:
            desc_elem = soup.find('div', class_=['listing-description', 'description'])
            if desc_elem:
                data['description'] = desc_elem.text.strip()
        
        return data