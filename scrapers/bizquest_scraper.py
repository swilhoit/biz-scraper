from .base_scraper import BaseScraper
from typing import Dict, List, Optional
import re
import json

class BizQuestScraper(BaseScraper):
    def __init__(self, site_config: Dict, max_workers: int = 10):
        super().__init__(site_config, max_workers)
        self.js_rendering = False  # Disable JS rendering - not needed

    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get listing URLs from search pages"""
        listing_urls = []
        page = 1
        
        # BizQuest uses /businesses-for-sale/page-X/ format
        base_url = "https://www.bizquest.com/businesses-for-sale"
        
        while True:
            if max_pages and page > max_pages:
                break
            
            # Construct page URL
            if page == 1:
                url = f"{base_url}/"
            else:
                url = f"{base_url}/page-{page}/"
                
            soup = self.get_page(url, render=self.js_rendering)
            
            if not soup:
                self.logger.info(f"No content for {url}, stopping.")
                break
            
            # Find all business listing links
            listings = soup.select('a[href*="/business-for-sale/"][href$="/"]')
            
            # Filter to unique listing URLs
            new_listings = []
            for listing in listings:
                href = listing.get('href')
                if href and '/business-for-sale/' in href and href != '/business-for-sale/':
                    if href.startswith('/'):
                        full_url = "https://www.bizquest.com" + href
                    else:
                        full_url = href
                        
                    # Skip if already seen
                    if full_url not in listing_urls:
                        new_listings.append(full_url)
                        listing_urls.append(full_url)
            
            if not new_listings:
                self.logger.warning(f"No new listings found on page {page}")
                break
                
            self.logger.info(f"Found {len(new_listings)} new listings on page {page}")
            page += 1
            
        return listing_urls
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing with comprehensive data extraction"""
        data = {'listing_url': url}
        
        # Get the page (no JS rendering needed for BizQuest)
        soup = self.get_page(url, render=False)
        if not soup:
            self.logger.error(f"Failed to load page: {url}")
            return None
        
        # Extract all text for parsing
        page_text = soup.get_text(separator=' ', strip=True)
        
        # Title extraction
        title_elem = soup.find('h1') or soup.find('h2', class_='business-title')
        if title_elem:
            data['title'] = title_elem.text.strip()
        else:
            # Extract from URL path
            match = re.search(r'/business-for-sale/([^/]+)/', url)
            if match:
                data['title'] = match.group(1).replace('-', ' ').title()
        
        # Price extraction - look for various formats
        price_patterns = [
            r'(?:asking price|price)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
            r'\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)\s*(?:asking|sale|price)',
            r'for sale[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
        ]
        
        # First check structured data
        price_elem = soup.find('span', class_='price') or soup.find('div', class_='price')
        if price_elem:
            price_text = price_elem.text.strip()
            data['asking_price'] = self.parse_price(price_text)
            data['asking_price_raw'] = price_text
        else:
            # Fall back to regex patterns
            for pattern in price_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    data['asking_price'] = self.parse_price(match.group(1))
                    data['asking_price_raw'] = match.group(0)
                    break
        
        # Cash Flow / Profit extraction (BizQuest often shows Cash Flow prominently)
        cash_flow_patterns = [
            r'cash flow[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
            r'(?:annual cash flow|yearly cash flow)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
            r'(?:net income|net profit)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
            r'(?:ebitda|sde)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
        ]
        
        for pattern in cash_flow_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                value = self.parse_price(match.group(1))
                data['cash_flow'] = value
                data['cash_flow_raw'] = match.group(0)
                # Also store as profit for compatibility
                data['profit'] = value
                data['profit_raw'] = match.group(0)
                data['profit_numeric'] = value
                break
        
        # Revenue/Sales extraction
        revenue_patterns = [
            r'(?:gross revenue|gross sales|annual revenue|yearly revenue|revenue)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
            r'(?:sales|annual sales|yearly sales)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)',
            r'\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)\s*(?:in revenue|in sales)',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                value = self.parse_price(match.group(1))
                data['revenue'] = value
                data['revenue_raw'] = match.group(0)
                data['revenue_numeric'] = value
                break
        
        # If we found cash flow but no revenue, estimate revenue
        if 'cash_flow' in data and 'revenue' not in data:
            # Look for margin info
            margin_match = re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:margin|profit margin)', page_text, re.IGNORECASE)
            if margin_match:
                margin = float(margin_match.group(1)) / 100
                if margin > 0:
                    data['revenue'] = data['cash_flow'] / margin
                    data['revenue_raw'] = f"Estimated from {margin*100}% margin"
        
        # Location extraction
        location_patterns = [
            r'(?:location|located in|based in)[:\s]*([^,]+,\s*[A-Z]{2})',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})',  # City, ST
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, page_text)
            if match:
                location = match.group(1).strip()
                data['location'] = location
                data['location_raw'] = location
                # Parse city and state
                parts = location.split(',')
                if len(parts) >= 2:
                    data['city'] = parts[0].strip()
                    data['state'] = parts[-1].strip()
                break
        
        # Business type/category extraction
        category_keywords = {
            'Restaurant': ['restaurant', 'cafe', 'coffee', 'food', 'dining', 'bar', 'grill', 'pizza', 'bakery'],
            'Retail': ['retail', 'store', 'shop', 'boutique', 'mart', 'market'],
            'Service': ['service', 'consulting', 'agency', 'cleaning', 'repair', 'maintenance'],
            'Manufacturing': ['manufacturing', 'factory', 'production', 'industrial'],
            'E-commerce': ['e-commerce', 'ecommerce', 'online', 'internet', 'amazon', 'fba', 'shopify'],
            'Franchise': ['franchise', 'franchising'],
            'Healthcare': ['medical', 'health', 'clinic', 'dental', 'wellness', 'pharmacy'],
            'Technology': ['technology', 'tech', 'software', 'saas', 'it', 'digital', 'app'],
            'Automotive': ['auto', 'car', 'vehicle', 'mechanic', 'dealership'],
            'Real Estate': ['real estate', 'property', 'realty', 'rental'],
        }
        
        page_text_lower = page_text.lower()
        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword in page_text_lower:
                    data['category'] = category
                    data['business_type'] = category
                    break
            if 'category' in data:
                break
        
        # Default category if not found
        if 'category' not in data:
            data['category'] = 'General'
            data['business_type'] = 'Business'
        
        # Year established extraction
        year_patterns = [
            r'(?:established|founded|since)[:\s]*(\d{4})',
            r'(\d{4})\s*(?:established|founded)',
            r'(?:in business since|operating since)[:\s]*(\d{4})',
        ]
        
        for pattern in year_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                year = int(match.group(1))
                if 1900 <= year <= 2025:
                    data['established_year'] = year
                    break
        
        # Description extraction
        desc_elem = soup.find('meta', {'name': 'description'})
        if desc_elem:
            data['description'] = desc_elem.get('content', '').strip()
        else:
            # Try to get from first paragraph or business description section
            desc_section = soup.find('div', class_='description') or soup.find('section', class_='business-description')
            if desc_section:
                data['description'] = desc_section.text.strip()[:500]
            else:
                # Use cleaned snippet from page text
                data['description'] = re.sub(r'\s+', ' ', page_text[:500])
        
        # Additional valuable fields
        
        # Employees
        employee_match = re.search(r'(\d+)\s*(?:employees|staff|workers)', page_text, re.IGNORECASE)
        if employee_match:
            data['employees'] = int(employee_match.group(1))
        
        # Inventory value
        inventory_match = re.search(r'inventory[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', page_text, re.IGNORECASE)
        if inventory_match:
            data['inventory_value'] = self.parse_price(inventory_match.group(1))
        
        # Real estate status
        if re.search(r'real estate included|includes real estate|property included', page_text, re.IGNORECASE):
            data['real_estate_included'] = True
        elif re.search(r'lease|rent|no real estate', page_text, re.IGNORECASE):
            data['real_estate_included'] = False
        
        # Reason for selling
        reason_match = re.search(r'(?:reason for selling|selling because)[:\s]*([^.]+)', page_text, re.IGNORECASE)
        if reason_match:
            data['reason_for_selling'] = reason_match.group(1).strip()[:200]
        
        # Financing available
        if re.search(r'financing available|seller financing|owner financing|sba', page_text, re.IGNORECASE):
            data['financing_available'] = True
        
        # Training provided
        if re.search(r'training provided|training included|will train', page_text, re.IGNORECASE):
            data['training_provided'] = True
        
        # Ensure numeric fields are set for compatibility
        if 'asking_price' in data:
            data['asking_price_numeric'] = data['asking_price']
        if 'revenue' in data:
            data['revenue_numeric'] = data['revenue']
        if 'profit' in data:
            data['profit_numeric'] = data['profit']
        if 'cash_flow' in data:
            data['cash_flow_numeric'] = data['cash_flow']
        
        return data
    
    def parse_price(self, price_str: str) -> float:
        """Parse price string to float with improved handling"""
        if not price_str:
            return 0.0
        
        # Convert to string if needed
        price_str = str(price_str)
        
        # Remove dollar signs, commas, and extra spaces
        price_str = price_str.replace('$', '').replace(',', '').strip()
        
        # Handle abbreviations
        multipliers = {
            'k': 1000,
            'thousand': 1000,
            'm': 1000000,
            'mil': 1000000,
            'million': 1000000,
            'mm': 1000000,
            'b': 1000000000,
            'billion': 1000000000
        }
        
        # Check for multiplier
        for suffix, multiplier in multipliers.items():
            if price_str.lower().endswith(suffix):
                # Remove suffix and multiply
                num_part = price_str[:-len(suffix)].strip()
                try:
                    return float(num_part) * multiplier
                except ValueError:
                    pass
        
        # Try to parse as regular number
        try:
            return float(price_str)
        except ValueError:
            return 0.0