#!/usr/bin/env python3
"""
BizBuySell Final Optimized Scraper
Fixed version with proper financial data extraction
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import time
import re
from urllib.parse import urljoin
from typing import List, Dict, Optional
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BizBuySellFinalScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()

    def make_request(self, url: str, use_render: bool = False) -> Optional[requests.Response]:
        """Make request with ScraperAPI"""
        params = {
            'api_key': self.api_key,
            'url': url,
        }
        
        if use_render:
            params['render'] = 'true'
        
        try:
            logger.info(f"Fetching {url} ({'render' if use_render else 'no-render'})")
            response = self.session.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def scrape_amazon_stores_final(self) -> List[Dict]:
        """Final optimized scraper for BizBuySell Amazon stores"""
        logger.info("🎯 FINAL BIZBUYSELL AMAZON SCRAPING...")
        
        base_url = "https://www.bizbuysell.com/amazon-stores-for-sale/"
        all_listings = []
        
        # Scrape pages 1-5 to get more comprehensive results
        for page in range(1, 6):
            try:
                if page == 1:
                    url = base_url
                else:
                    url = f"{base_url}?page={page}"
                
                logger.info(f"📄 Scraping page {page}...")
                
                response = self.make_request(url, use_render=False)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                page_listings = self.extract_final_listings(soup, url)
                
                if page_listings:
                    all_listings.extend(page_listings)
                    logger.info(f"✅ Page {page}: Found {len(page_listings)} listings")
                else:
                    logger.warning(f"❌ Page {page}: No listings found")
                    break  # Stop if no listings found
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error scraping page {page}: {e}")
        
        logger.info(f"🎉 Total final listings found: {len(all_listings)}")
        return all_listings

    def extract_final_listings(self, soup: BeautifulSoup, page_url: str) -> List[Dict]:
        """Extract listings using optimized strategy"""
        listings = []
        
        # Use the working opportunity links strategy
        opportunity_links = soup.select('a[href*="opportunity"]')
        logger.info(f"  Found {len(opportunity_links)} opportunity links")
        
        for link in opportunity_links:
            try:
                href = link.get('href', '')
                if not href or '/business-opportunity/' not in href:
                    continue
                    
                full_url = urljoin(page_url, href)
                link_text = link.get_text().strip()
                
                # Skip navigation/UI links
                if any(skip in link_text.lower() for skip in ['register', 'login', 'sign up', 'learn more', 'see more', 'contact']):
                    continue
                
                # Skip very short names
                if len(link_text) < 15:
                    continue
                
                # Get comprehensive business data
                listing = self.extract_comprehensive_data(link, full_url, soup)
                if listing:
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error processing opportunity link: {e}")
        
        return listings

    def extract_comprehensive_data(self, link, full_url: str, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract comprehensive business data with proper financial parsing"""
        try:
            # Get business name from link text
            name = link.get_text().strip()
            
            # Find the largest parent container with business info
            parent = link.find_parent(['div'])
            for _ in range(3):  # Try to go up 3 levels to find full business block
                if parent and parent.parent:
                    parent = parent.parent
                else:
                    break
            
            if parent:
                full_text = parent.get_text()
            else:
                full_text = link.get_text()
            
            # Only include Amazon/FBA related businesses
            if not any(keyword in full_text.lower() for keyword in ['amazon', 'fba', 'ecommerce', 'e-commerce']):
                return None
            
            # Extract financial data with improved patterns
            price = self.extract_price_improved(full_text)
            revenue = self.extract_revenue_improved(full_text)
            profit = self.extract_profit_improved(full_text)
            multiple = self.calculate_multiple(price, profit)
            
            # Create clean description
            description = self.clean_description(full_text)
            
            listing = {
                'source': 'BizBuySell',
                'name': name[:150],  # Limit name length
                'price': price,
                'revenue': revenue,
                'profit': profit,
                'description': description,
                'url': full_url,
                'multiple': multiple,
            }
            
            return listing
            
        except Exception as e:
            logger.debug(f"Error extracting comprehensive data: {e}")
            return None

    def extract_price_improved(self, text: str) -> str:
        """Improved price extraction"""
        # Look for specific price patterns
        patterns = [
            r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',  # Standard currency format
        ]
        
        # Try each pattern
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Convert to number and validate
                clean_match = re.sub(r'[^\d.,KkMm]', '', match)
                if clean_match and any(c.isdigit() for c in clean_match):
                    # Convert K/M notation
                    if clean_match.upper().endswith('K'):
                        value = float(clean_match[:-1]) * 1000
                    elif clean_match.upper().endswith('M'):
                        value = float(clean_match[:-1]) * 1000000
                    else:
                        value = float(clean_match.replace(',', ''))
                    
                    # Reasonable business price range
                    if 10000 <= value <= 50000000:
                        return f"${value:,.0f}"
        
        return ""

    def extract_revenue_improved(self, text: str) -> str:
        """Improved revenue extraction"""
        patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'(\d{4})\s+revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',  # "2023 revenue $1.3M"
            r'\$([0-9]{1,3}(?:,[0-9]{3})*)[KkMm]?\s*(?:revenue|sales)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[-1]  # Take the last group (amount)
                
                clean_match = re.sub(r'[^\d.,KkMm]', '', match)
                if clean_match and any(c.isdigit() for c in clean_match):
                    try:
                        if clean_match.upper().endswith('K'):
                            value = float(clean_match[:-1]) * 1000
                        elif clean_match.upper().endswith('M'):
                            value = float(clean_match[:-1]) * 1000000
                        else:
                            value = float(clean_match.replace(',', ''))
                        
                        if 1000 <= value <= 100000000:
                            return f"${value:,.0f}"
                    except ValueError:
                        continue
        
        return ""

    def extract_profit_improved(self, text: str) -> str:
        """Improved profit extraction"""
        patterns = [
            r'cash\s+flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$([0-9]{1,3}(?:,[0-9]{3})*)[KkMm]?\s*(?:cash flow|profit|ebitda)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                clean_match = re.sub(r'[^\d.,KkMm]', '', match)
                if clean_match and any(c.isdigit() for c in clean_match):
                    try:
                        if clean_match.upper().endswith('K'):
                            value = float(clean_match[:-1]) * 1000
                        elif clean_match.upper().endswith('M'):
                            value = float(clean_match[:-1]) * 1000000
                        else:
                            value = float(clean_match.replace(',', ''))
                        
                        if 1000 <= value <= 10000000:
                            return f"${value:,.0f}"
                    except ValueError:
                        continue
        
        return ""

    def calculate_multiple(self, price: str, profit: str) -> str:
        """Calculate price/profit multiple"""
        try:
            if price and profit:
                price_val = float(price.replace('$', '').replace(',', ''))
                profit_val = float(profit.replace('$', '').replace(',', ''))
                if profit_val > 0:
                    multiple = price_val / profit_val
                    if 0.5 <= multiple <= 20:  # Reasonable business multiples
                        return f"{multiple:.1f}x"
        except:
            pass
        return ""

    def clean_description(self, text: str) -> str:
        """Clean and format description"""
        # Take first 300 characters
        description = text[:300]
        # Clean up whitespace
        description = re.sub(r'\s+', ' ', description).strip()
        return description

    def remove_duplicates(self, listings: List[Dict]) -> List[Dict]:
        """Remove duplicates based on URL"""
        unique_listings = []
        seen_urls = set()
        
        for listing in listings:
            url = listing.get('url', '').lower().strip()
            if url not in seen_urls:
                seen_urls.add(url)
                unique_listings.append(listing)
        
        return unique_listings

    def export_final_results(self, listings: List[Dict]) -> None:
        """Export final optimized results"""
        if not listings:
            print("❌ No final listings found!")
            return
        
        # Remove duplicates
        unique_listings = self.remove_duplicates(listings)
        
        df = pd.DataFrame(unique_listings)
        column_order = ['source', 'name', 'price', 'revenue', 'profit', 'multiple', 'description', 'url']
        df = df[column_order]
        
        filename = 'bizbuysell_final_amazon_businesses.csv'
        df.to_csv(filename, index=False, quoting=1)
        
        print(f"\n{'='*70}")
        print("🏆 BIZBUYSELL FINAL AMAZON BUSINESS SCRAPING RESULTS")
        print(f"{'='*70}")
        print(f"✅ Total unique Amazon businesses: {len(df)}")
        print(f"📊 Listings with prices: {df['price'].str.len().gt(0).sum()}")
        print(f"📊 Listings with revenue: {df['revenue'].str.len().gt(0).sum()}")
        print(f"📊 Listings with profit: {df['profit'].str.len().gt(0).sum()}")
        print(f"💾 Data exported to: {filename}")
        
        # Show summary stats
        if len(df) > 0:
            # Calculate financial stats
            prices = df[df['price'].str.len() > 0]['price'].str.replace('$', '').str.replace(',', '').astype(float)
            if len(prices) > 0:
                print(f"💰 Price range: ${prices.min():,.0f} - ${prices.max():,.0f}")
                print(f"💰 Average price: ${prices.mean():,.0f}")
        
        print(f"\n📋 TOP 10 AMAZON BUSINESS OPPORTUNITIES:")
        for i, (_, row) in enumerate(df.head(10).iterrows()):
            print(f"  {i+1}. {row['name'][:80]}{'...' if len(row['name']) > 80 else ''}")
            if row['price']:
                print(f"     💰 Price: {row['price']}")
            if row['revenue']:
                print(f"     📈 Revenue: {row['revenue']}")
            if row['profit']:
                print(f"     💵 Profit: {row['profit']}")
            if row['multiple']:
                print(f"     📊 Multiple: {row['multiple']}")
            print()

def main():
    """Main function"""
    scraper = BizBuySellFinalScraper()
    listings = scraper.scrape_amazon_stores_final()
    scraper.export_final_results(listings)

if __name__ == "__main__":
    main() 