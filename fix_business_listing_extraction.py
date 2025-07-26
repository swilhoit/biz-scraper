#!/usr/bin/env python3

import os
import csv
import re
import json
import logging
import time
import random
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fixed_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FixedBusinessScraper:
    def __init__(self):
        self.api_key = "054d8cdaa4e8453e3afa7e5e9316c72f"
        self.base_api_url = "http://api.scraperapi.com"
        self.scraped_urls: Set[str] = set()
        
        # Navigation/category keywords to exclude
        self.exclude_keywords = {
            'page', 'next', 'previous', 'more', 'see all', 'browse', 'search',
            'filter', 'sort', 'category', 'businesses for sale', 'amazon stores',
            'register', 'login', 'sign up', 'learn more', 'contact us',
            'about us', 'home', 'listing date', 'asking price', 'cash flow',
            'listing number', 'websites for sale', 'domain names', 'apps for sale'
        }

    def make_request(self, url: str, retries: int = 2, use_render: bool = False) -> Optional[requests.Response]:
        """Make request using ScraperAPI with retries"""
        for attempt in range(1, retries + 1):
            try:
                params = {
                    'api_key': self.api_key,
                    'url': url,
                    'country_code': 'us'
                }
                
                if use_render:
                    params['render'] = 'true'
                
                response = requests.get(self.base_api_url, params=params, timeout=90)
                response.raise_for_status()
                return response
                
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed for {url}: {e}")
                if attempt < retries:
                    time.sleep(random.uniform(2, 5))
        
        return None

    def is_valid_business_listing(self, title: str, url: str, description: str) -> bool:
        """Validate if this is a real business listing, not navigation"""
        title_lower = title.lower()
        
        # Skip if title contains navigation keywords
        if any(keyword in title_lower for keyword in self.exclude_keywords):
            return False
        
        # Skip if title is too short
        if len(title.strip()) < 20:
            return False
        
        # Skip if URL contains category/navigation patterns
        url_lower = url.lower()
        if any(pattern in url_lower for pattern in ['/category/', '/search/', '/filter/', '/browse/']):
            return False
        
        # Must have some business-specific content
        business_indicators = [
            'business', 'company', 'store', 'shop', 'service', 'agency', 
            'franchise', 'route', 'restaurant', 'retail', 'manufacturing',
            'distribution', 'consulting', 'construction', 'automotive'
        ]
        
        combined_text = f"{title} {description}".lower()
        if not any(indicator in combined_text for indicator in business_indicators):
            return False
        
        return True

    def extract_financial_data(self, text: str) -> Dict[str, str]:
        """Extract financial data with improved patterns"""
        financials = {'price': '', 'revenue': '', 'profit': ''}
        
        # Price patterns
        price_patterns = [
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'sale[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'\$?([\d,]+(?:\.\d+)?)\s*asking'
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                financials['price'] = f"${match.group(1)}"
                break
        
        # Revenue patterns
        revenue_patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'gross[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)'
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                financials['revenue'] = f"${match.group(1)}"
                break
        
        # Profit/Cash flow patterns
        profit_patterns = [
            r'cash\s+flow[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'net[:\s]*\$?([\d,]+(?:\.\d+)?)',
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)'
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                financials['profit'] = f"${match.group(1)}"
                break
        
        return financials

    def scrape_bizquest_fixed(self, url: str) -> List[Dict]:
        """Fixed BizQuest scraper targeting only individual listings"""
        logger.info(f"🎯 Scraping BizQuest: {url}")
        
        response = self.make_request(url, retries=3)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        # Target specific business listing containers
        business_cards = soup.select('div.business-card, div.listing-card, div.property-card, article.listing')
        
        # If no specific cards found, try broader search but with validation
        if not business_cards:
            business_cards = soup.select('div[class*="card"], div[class*="listing"], div[class*="business"]')
        
        logger.info(f"BizQuest: Found {len(business_cards)} potential cards")
        
        for card in business_cards:
            try:
                # Must have a link to actual business listing
                link_elem = card.find('a', href=True)
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                
                # Skip if this looks like navigation
                if any(nav in href.lower() for nav in ['/businesses-for-sale/', '/category/', '/search/']):
                    continue
                
                listing_url = urljoin(url, href)
                
                # Extract title
                title_elem = card.select_one('h1, h2, h3, h4, .title, .business-title')
                title = title_elem.get_text().strip() if title_elem else link_elem.get_text().strip()
                
                # Get description
                desc_elem = card.select_one('.description, .summary, p')
                description = desc_elem.get_text().strip() if desc_elem else ""
                
                # Validate this is a real business listing
                if not self.is_valid_business_listing(title, listing_url, description):
                    continue
                
                # Skip if already processed
                if listing_url in self.scraped_urls:
                    continue
                self.scraped_urls.add(listing_url)
                
                # Extract financial data
                card_text = card.get_text()
                financials = self.extract_financial_data(card_text)
                
                listing = {
                    'source': 'BizQuest',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': description[:500],
                    'url': listing_url,
                    'multiple': '',
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error processing BizQuest card: {e}")
        
        logger.info(f"BizQuest: Extracted {len(listings)} valid listings")
        return listings

    def scrape_bizbuysell_fixed(self, url: str) -> List[Dict]:
        """Fixed BizBuySell scraper targeting only individual opportunity listings"""
        logger.info(f"🎯 Scraping BizBuySell: {url}")
        
        response = self.make_request(url, retries=3)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        # Target specific opportunity links (actual business listings)
        opportunity_links = soup.select('a[href*="/business-opportunity/"]')
        logger.info(f"BizBuySell: Found {len(opportunity_links)} opportunity links")
        
        for link in opportunity_links:
            try:
                href = link.get('href', '')
                if not href or '/business-opportunity/' not in href:
                    continue
                
                listing_url = urljoin(url, href)
                title = link.get_text().strip()
                
                # Get parent container for context
                parent = link.find_parent(['div', 'article'])
                if not parent:
                    continue
                
                container_text = parent.get_text()
                
                # Validate this is a real business listing
                if not self.is_valid_business_listing(title, listing_url, container_text):
                    continue
                
                # Skip if already processed
                if listing_url in self.scraped_urls:
                    continue
                self.scraped_urls.add(listing_url)
                
                # Extract financial data
                financials = self.extract_financial_data(container_text)
                
                # Skip if no meaningful financial data
                if not any(financials.values()):
                    continue
                
                listing = {
                    'source': 'BizBuySell',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': container_text[:500].strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error processing BizBuySell link: {e}")
        
        logger.info(f"BizBuySell: Extracted {len(listings)} valid listings")
        return listings

    def scrape_quietlight_fixed(self, url: str) -> List[Dict]:
        """Fixed QuietLight scraper targeting only listing cards"""
        logger.info(f"🎯 Scraping QuietLight: {url}")
        
        response = self.make_request(url, retries=3)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        # Target specific listing cards
        listing_cards = soup.select('div.listing-card, div.business-card, div[class*="listing"]')
        logger.info(f"QuietLight: Found {len(listing_cards)} listing cards")
        
        for card in listing_cards:
            try:
                # Must have a link to actual listing
                link_elem = card.select_one('a[href*="/listings/"]')
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                listing_url = urljoin(url, href)
                
                # Extract title
                title_elem = card.select_one('h1, h2, h3, h4, .title')
                title = title_elem.get_text().strip() if title_elem else link_elem.get_text().strip()
                
                # Get description
                card_text = card.get_text()
                
                # Validate this is a real business listing
                if not self.is_valid_business_listing(title, listing_url, card_text):
                    continue
                
                # Skip if already processed
                if listing_url in self.scraped_urls:
                    continue
                self.scraped_urls.add(listing_url)
                
                # Extract financial data
                financials = self.extract_financial_data(card_text)
                
                listing = {
                    'source': 'QuietLight',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': card_text[:500].strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error processing QuietLight card: {e}")
        
        logger.info(f"QuietLight: Extracted {len(listings)} valid listings")
        return listings

    def scrape_websiteproperties_fixed(self, url: str) -> List[Dict]:
        """Fixed WebsiteProperties scraper"""
        logger.info(f"🎯 Scraping WebsiteProperties: {url}")
        
        response = self.make_request(url, retries=3)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = []
        
        # Target specific property cards
        property_cards = soup.select('div.property, div.listing, div[class*="property"], div[class*="business"]')
        logger.info(f"WebsiteProperties: Found {len(property_cards)} property cards")
        
        for card in property_cards:
            try:
                # Must have a link
                link_elem = card.find('a', href=True)
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                listing_url = urljoin(url, href)
                
                # Extract title
                title = link_elem.get_text().strip()
                
                # Get description
                card_text = card.get_text()
                
                # Validate this is a real business listing
                if not self.is_valid_business_listing(title, listing_url, card_text):
                    continue
                
                # Skip if already processed
                if listing_url in self.scraped_urls:
                    continue
                self.scraped_urls.add(listing_url)
                
                # Extract financial data
                financials = self.extract_financial_data(card_text)
                
                listing = {
                    'source': 'WebsiteProperties',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': card_text[:500].strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error processing WebsiteProperties card: {e}")
        
        logger.info(f"WebsiteProperties: Extracted {len(listings)} valid listings")
        return listings

    def run_fixed_scraper(self) -> List[Dict]:
        """Run the fixed scraper targeting legitimate business listings only"""
        logger.info("🚀 STARTING FIXED BUSINESS SCRAPER (NO CATEGORIES/NAVIGATION)")
        
        # Target URLs with specific business listings
        target_urls = [
            # BizQuest specific business pages
            "https://www.bizquest.com/business-for-sale/page-1/",
            "https://www.bizquest.com/business-for-sale/page-2/",
            "https://www.bizquest.com/business-for-sale/page-3/",
            
            # BizBuySell Amazon stores (known to have individual listings)
            "https://www.bizbuysell.com/amazon-stores-for-sale/",
            "https://www.bizbuysell.com/amazon-stores-for-sale/?page=2",
            
            # QuietLight specific categories
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/ecommerce-businesses-for-sale/",
            
            # WebsiteProperties Amazon section
            "https://websiteproperties.com/amazon-fba-business-for-sale/",
        ]
        
        all_listings = []
        
        for url in target_urls:
            try:
                if 'bizquest' in url:
                    listings = self.scrape_bizquest_fixed(url)
                elif 'bizbuysell' in url:
                    listings = self.scrape_bizbuysell_fixed(url)
                elif 'quietlight' in url:
                    listings = self.scrape_quietlight_fixed(url)
                elif 'websiteproperties' in url:
                    listings = self.scrape_websiteproperties_fixed(url)
                else:
                    continue
                
                all_listings.extend(listings)
                
                # Rate limiting
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
        
        # Remove duplicates based on URL
        unique_listings = []
        seen_urls = set()
        
        for listing in all_listings:
            if listing['url'] not in seen_urls:
                unique_listings.append(listing)
                seen_urls.add(listing['url'])
        
        logger.info(f"🎉 FIXED SCRAPER COMPLETED: {len(unique_listings)} unique legitimate business listings")
        
        return unique_listings

    def save_to_csv(self, listings: List[Dict], filename: str = "FIXED_LEGITIMATE_BUSINESS_LISTINGS.csv"):
        """Save listings to CSV file"""
        if not listings:
            logger.warning("No listings to save")
            return
        
        fieldnames = ['source', 'name', 'price', 'revenue', 'profit', 'description', 'url', 'multiple']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for listing in listings:
                # Ensure all required fields exist
                cleaned_listing = {field: listing.get(field, '') for field in fieldnames}
                writer.writerow(cleaned_listing)
        
        logger.info(f"💾 Saved {len(listings)} listings to {filename}")
        
        # Print summary
        source_counts = {}
        for listing in listings:
            source = listing.get('source', 'Unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
        
        print("\n" + "="*80)
        print("🎯 FIXED LEGITIMATE BUSINESS LISTINGS SUMMARY")
        print("="*80)
        print(f"📊 TOTAL LEGITIMATE BUSINESSES: {len(listings)}")
        print("\n📈 SOURCE BREAKDOWN:")
        for source, count in sorted(source_counts.items()):
            percentage = (count / len(listings)) * 100
            print(f"  {source}: {count} businesses ({percentage:.1f}%)")
        
        # Data quality metrics
        with_price = sum(1 for l in listings if l.get('price'))
        with_revenue = sum(1 for l in listings if l.get('revenue'))
        with_profit = sum(1 for l in listings if l.get('profit'))
        
        print("\n💰 DATA QUALITY:")
        print(f"  Price coverage: {with_price}/{len(listings)} ({(with_price/len(listings)*100):.1f}%)")
        print(f"  Revenue coverage: {with_revenue}/{len(listings)} ({(with_revenue/len(listings)*100):.1f}%)")
        print(f"  Profit coverage: {with_profit}/{len(listings)} ({(with_profit/len(listings)*100):.1f}%)")
        print("="*80)

def main():
    scraper = FixedBusinessScraper()
    listings = scraper.run_fixed_scraper()
    scraper.save_to_csv(listings)

if __name__ == "__main__":
    main() 