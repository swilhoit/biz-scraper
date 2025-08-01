#!/usr/bin/env python3
"""
Enhanced Detail Scraper
Fetches additional details from listing pages for existing database records
"""

import time
import logging
from typing import Dict, Optional, List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import and_
from database import get_session, Business
from scrapers.base_scraper import BaseScraper
from config import SITES

class DetailScraper(BaseScraper):
    """Scrapes additional details from individual listing pages"""
    
    def __init__(self):
        # Initialize with a generic config
        super().__init__({
            'name': 'DetailScraper',
            'base_url': 'various',
            'search_urls': []
        })
        self.logger = logging.getLogger('DetailScraper')
        
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Not used for detail scraper"""
        return []
    
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Not used directly - see enhance_listing"""
        return None
    
    def enhance_listing(self, business: Business) -> bool:
        """
        Enhance an existing business listing with additional details
        
        Args:
            business: Business object from database
            
        Returns:
            bool: True if enhancement was successful
        """
        if not business.listing_url:
            return False
            
        self.logger.info(f"Enhancing details for: {business.title or business.listing_url}")
        
        # Get the page
        soup = self.get_page(business.listing_url)
        if not soup:
            self.logger.warning(f"Failed to fetch page: {business.listing_url}")
            return False
        
        # Extract enhanced details based on source site
        enhanced_data = self.extract_enhanced_details(soup, business.source_site)
        
        if enhanced_data:
            # Update the business object
            for key, value in enhanced_data.items():
                if hasattr(business, key) and value:
                    setattr(business, key, value)
            
            business.enhanced_at = datetime.utcnow()
            
            try:
                self.db_session.commit()
                self.logger.info(f"Enhanced: {business.title}")
                return True
            except Exception as e:
                self.db_session.rollback()
                self.logger.error(f"Error saving enhanced data: {e}")
                
        return False
    
    def extract_enhanced_details(self, soup, source_site: str) -> Dict:
        """
        Extract additional details from the page
        
        Args:
            soup: BeautifulSoup object
            source_site: Name of the source site
            
        Returns:
            Dict: Enhanced data fields
        """
        enhanced_data = {}
        
        # Get all text for pattern matching
        page_text = soup.get_text()
        
        # Extract seller information
        seller_info = self.extract_seller_info(soup, page_text)
        if seller_info:
            enhanced_data.update(seller_info)
        
        # Extract additional financials
        financials = self.extract_detailed_financials(soup, page_text)
        if financials:
            enhanced_data.update(financials)
        
        # Extract business details
        details = self.extract_business_details(soup, page_text)
        if details:
            enhanced_data.update(details)
        
        # Site-specific extraction
        if source_site == 'BizBuySell':
            enhanced_data.update(self.extract_bizbuysell_details(soup))
        elif source_site == 'BizQuest':
            enhanced_data.update(self.extract_bizquest_details(soup))
        elif source_site == 'QuietLight':
            enhanced_data.update(self.extract_quietlight_details(soup))
        
        return enhanced_data
    
    def extract_seller_info(self, soup, page_text: str) -> Dict:
        """Extract seller information"""
        info = {}
        
        # Common patterns for seller info
        import re
        
        # Seller financing
        if re.search(r'seller\s*financing\s*available', page_text, re.I):
            info['seller_financing'] = True
        elif re.search(r'no\s*seller\s*financing', page_text, re.I):
            info['seller_financing'] = False
            
        # Reason for selling
        reason_match = re.search(r'reason\s*for\s*selling[:\s]*([^.]+)', page_text, re.I)
        if reason_match:
            info['reason_for_selling'] = reason_match.group(1).strip()
        
        return info
    
    def extract_detailed_financials(self, soup, page_text: str) -> Dict:
        """Extract detailed financial information"""
        financials = {}
        import re
        
        # EBITDA
        ebitda_patterns = [
            r'ebitda[:\s]*\$?([\d,]+)',
            r'adjusted\s*ebitda[:\s]*\$?([\d,]+)'
        ]
        for pattern in ebitda_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                financials['ebitda'] = self.parse_price(match.group(1))
                break
        
        # Multiple
        multiple_patterns = [
            r'multiple[:\s]*([\d.]+)x?',
            r'([\d.]+)x?\s*multiple'
        ]
        for pattern in multiple_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                try:
                    financials['multiple'] = float(match.group(1))
                except:
                    pass
                break
        
        # Inventory value
        inventory_match = re.search(r'inventory[:\s]*\$?([\d,]+)', page_text, re.I)
        if inventory_match:
            financials['inventory_value'] = self.parse_price(inventory_match.group(1))
        
        return financials
    
    def extract_business_details(self, soup, page_text: str) -> Dict:
        """Extract additional business details"""
        details = {}
        import re
        
        # Year established
        year_patterns = [
            r'established[:\s]*(\d{4})',
            r'founded[:\s]*(\d{4})',
            r'since[:\s]*(\d{4})'
        ]
        for pattern in year_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                details['year_established'] = int(match.group(1))
                break
        
        # Number of employees
        employee_patterns = [
            r'(\d+)\s*employees?',
            r'employees?[:\s]*(\d+)'
        ]
        for pattern in employee_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                details['employees'] = int(match.group(1))
                break
        
        # Website/URL
        website_match = re.search(r'website[:\s]*([^\s]+)', page_text, re.I)
        if website_match:
            details['website'] = website_match.group(1)
        
        return details
    
    def extract_bizbuysell_details(self, soup) -> Dict:
        """Extract BizBuySell-specific details"""
        details = {}
        
        # Look for specific BizBuySell elements
        detail_sections = soup.find_all('div', class_='details-list')
        for section in detail_sections:
            items = section.find_all('li')
            for item in items:
                text = item.get_text()
                if 'Inventory' in text:
                    value = self.extract_value_from_text(text)
                    if value:
                        details['inventory_value'] = value
                elif 'FF&E' in text:
                    value = self.extract_value_from_text(text)
                    if value:
                        details['ffe_value'] = value
        
        return details
    
    def extract_bizquest_details(self, soup) -> Dict:
        """Extract BizQuest-specific details"""
        details = {}
        
        # BizQuest often has a details table
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text().strip()
                    value = cells[1].get_text().strip()
                    
                    if 'Financing' in label:
                        details['seller_financing'] = 'Yes' in value
                    elif 'Employees' in label:
                        try:
                            details['employees'] = int(value.split()[0])
                        except:
                            pass
        
        return details
    
    def extract_quietlight_details(self, soup) -> Dict:
        """Extract QuietLight-specific details"""
        details = {}
        
        # QuietLight has detailed metrics
        metrics_section = soup.find('div', class_='listing-metrics')
        if metrics_section:
            metrics = metrics_section.find_all('div', class_='metric')
            for metric in metrics:
                label = metric.find('span', class_='label')
                value = metric.find('span', class_='value')
                if label and value:
                    label_text = label.get_text().strip()
                    value_text = value.get_text().strip()
                    
                    if 'Multiple' in label_text:
                        try:
                            details['multiple'] = float(value_text.replace('x', ''))
                        except:
                            pass
                    elif 'Traffic' in label_text:
                        details['monthly_traffic'] = value_text
        
        return details
    
    def extract_value_from_text(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        import re
        match = re.search(r'\$?([\d,]+(?:\.\d+)?)', text)
        if match:
            return self.parse_price(match.group(1))
        return None
    
    def enhance_all_listings(self, source_site: Optional[str] = None, 
                           limit: Optional[int] = None,
                           max_workers: int = 5):
        """
        Enhance all listings in the database with additional details
        
        Args:
            source_site: Only enhance listings from this site
            limit: Maximum number of listings to enhance
            max_workers: Number of concurrent workers
        """
        # Query for listings that haven't been enhanced
        query = self.db_session.query(Business).filter(
            Business.enhanced_at.is_(None)
        )
        
        if source_site:
            query = query.filter(Business.source_site == source_site)
        
        if limit:
            query = query.limit(limit)
        
        listings = query.all()
        self.logger.info(f"Found {len(listings)} listings to enhance")
        
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_listing = {
                executor.submit(self.enhance_listing, listing): listing 
                for listing in listings
            }
            
            for future in as_completed(future_to_listing):
                listing = future_to_listing[future]
                try:
                    if future.result():
                        success_count += 1
                        self.logger.info(f"Enhanced {success_count}/{len(listings)}")
                except Exception as e:
                    self.logger.error(f"Error enhancing {listing.listing_url}: {e}")
                
                time.sleep(1)  # Rate limiting
        
        self.logger.info(f"Enhancement complete. Success: {success_count}/{len(listings)}")
        
def main():
    """Run the detail scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhance business listings with additional details')
    parser.add_argument('--source', help='Only enhance listings from this source')
    parser.add_argument('--limit', type=int, help='Maximum number of listings to enhance')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    scraper = DetailScraper()
    scraper.enhance_all_listings(
        source_site=args.source,
        limit=args.limit,
        max_workers=args.workers
    )

if __name__ == '__main__':
    main()