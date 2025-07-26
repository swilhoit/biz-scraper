#!/usr/bin/env python3
"""
Comprehensive Business Listings Scraper - Fixed Version
Fixes all issues: financial extraction, pagination, rate limiting, coverage
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import time
import re
import csv
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveBusinessScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.scraped_data = []
        self.lock = threading.Lock()
        
        # Comprehensive URL list with pagination
        self.urls = self.generate_comprehensive_urls()

    def generate_comprehensive_urls(self) -> List[str]:
        """Generate comprehensive list of URLs with proper pagination"""
        urls = []
        
        # QuietLight - Multiple pages
        for page in range(1, 11):  # 10 pages
            if page == 1:
                urls.append("https://quietlight.com/amazon-fba-businesses-for-sale/")
            else:
                urls.append(f"https://quietlight.com/amazon-fba-businesses-for-sale/page/{page}/")
        
        # BizBuySell Amazon stores - More pages
        for page in range(1, 11):  # 10 pages
            if page == 1:
                urls.append("https://www.bizbuysell.com/amazon-stores-for-sale/")
            else:
                urls.append(f"https://www.bizbuysell.com/amazon-stores-for-sale/?page={page}")
        
        # BizBuySell general business search
        for page in range(1, 6):
            if page == 1:
                urls.append("https://www.bizbuysell.com/business-for-sale/")
            else:
                urls.append(f"https://www.bizbuysell.com/business-for-sale/?page={page}")
        
        # Flippa - Multiple pages  
        for page in range(1, 6):
            urls.append(f"https://flippa.com/search?filter%5Bproperty_type%5D%5B%5D=starter_site&filter%5Bproperty_type%5D%5B%5D=established_website&page={page}")
            urls.append(f"https://flippa.com/buy/monetization/amazon-fba?page={page}")
        
        # Empire Flippers - Multiple categories
        empire_categories = [
            "amazon-fba-businesses-for-sale",
            "ecommerce-businesses-for-sale", 
            "amazon-associates-businesses-for-sale"
        ]
        for category in empire_categories:
            for page in range(1, 4):
                urls.append(f"https://empireflippers.com/marketplace/{category}/?page={page}")
        
        # WebsiteProperties - Multiple pages
        for page in range(1, 6):
            if page == 1:
                urls.append("https://websiteproperties.com/amazon-fba-business-for-sale/")
            else:
                urls.append(f"https://websiteproperties.com/amazon-fba-business-for-sale/?page={page}")
        
        # Add more marketplaces
        additional_urls = [
            "https://www.bizquest.com/amazon-business-for-sale/",
            "https://www.bizquest.com/ecommerce-business-for-sale/",
            "https://investors.club/tech-stack/amazon-fba/",
            "https://investors.club/tech-stack/ecommerce/", 
            "https://acquire.com/amazon-fba-for-sale/",
            "https://acquire.com/ecommerce-for-sale/",
            "https://www.loopnet.com/biz/amazon-stores-for-sale/",
            "https://www.loopnet.com/biz/ecommerce-business-for-sale/",
        ]
        urls.extend(additional_urls)
        
        logger.info(f"Generated {len(urls)} URLs for comprehensive scraping")
        return urls

    def make_request(self, url: str, retries: int = 3, use_render: bool = False) -> Optional[requests.Response]:
        """Make a request using Scraper API with improved error handling"""
        params = {
            'api_key': self.api_key,
            'url': url,
        }
        
        # Only use render for JavaScript-heavy sites
        if use_render:
            params['render'] = 'true'
        
        for attempt in range(retries):
            try:
                # Add random delay to avoid rate limiting
                time.sleep(random.uniform(1, 3))
                
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=90)
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(1, 3)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
        return None

    def extract_financial_value(self, text: str, patterns: List[str]) -> str:
        """Extract financial values with improved patterns"""
        if not text:
            return ""
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[-1]  # Take the last capturing group
                
                # Clean the match
                clean_match = re.sub(r'[^\d.,KkMm]', '', str(match))
                if not clean_match or not any(c.isdigit() for c in clean_match):
                    continue
                
                try:
                    # Handle multipliers
                    multiplier = 1
                    if clean_match.upper().endswith('K'):
                        multiplier = 1000
                        clean_match = clean_match[:-1]
                    elif clean_match.upper().endswith('M'):
                        multiplier = 1000000
                        clean_match = clean_match[:-1]
                    
                    # Convert to number
                    value = float(clean_match.replace(',', '')) * multiplier
                    
                    # Validate reasonable business ranges
                    if 1000 <= value <= 100000000:
                        return f"${value:,.0f}"
                        
                except (ValueError, TypeError):
                    continue
        
        return ""

    def extract_price(self, text: str) -> str:
        """Extract asking price with comprehensive patterns"""
        patterns = [
            r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
            r'(\d+(?:,\d{3})*(?:\.\d+)?)[KkMm]?\s*(?:asking|price)',
            r'listed\s*(?:at|for)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_value(text, patterns)

    def extract_revenue(self, text: str) -> str:
        """Extract revenue with comprehensive patterns"""
        patterns = [
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'gross[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'(\d{4})\s+revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$([0-9]{1,3}(?:,[0-9]{3})*)[KkMm]?\s*(?:revenue|sales|gross)',
            r'annual\s*(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'(\d+(?:,\d{3})*(?:\.\d+)?)[KkMm]?\s*(?:in\s*)?(?:revenue|sales)',
        ]
        return self.extract_financial_value(text, patterns)

    def extract_profit(self, text: str) -> str:
        """Extract profit with comprehensive patterns"""
        patterns = [
            r'cash\s+flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sde[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'net[:\s]*(?:income|profit)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$([0-9]{1,3}(?:,[0-9]{3})*)[KkMm]?\s*(?:cash flow|profit|ebitda|sde)',
            r'(\d+(?:,\d{3})*(?:\.\d+)?)[KkMm]?\s*(?:profit|cash flow|ebitda)',
        ]
        return self.extract_financial_value(text, patterns)

    def scrape_quietlight_comprehensive(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Comprehensive QuietLight scraper"""
        listings = []
        
        # Multiple selector strategies for QuietLight
        selectors = [
            'div.listing-item',
            'div.business-listing', 
            'article.post',
            'div[class*="listing"]',
            'div[class*="business"]',
            '.listing-card',
            '.business-card'
        ]
        
        business_cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                business_cards = cards
                logger.info(f"QuietLight: Found {len(cards)} cards with selector: {selector}")
                break
        
        for card in business_cards:
            try:
                # Multiple title strategies
                title_elem = (
                    card.select_one('h2.listing-title') or
                    card.select_one('h3.business-title') or  
                    card.select_one('h2') or
                    card.select_one('h3') or
                    card.select_one('a[href*="/listings/"]') or
                    card.find('a', href=True)
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 10:
                    continue

                # Extract URL
                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Get comprehensive text for financial extraction
                full_text = card.get_text()
                
                # Extract financials
                price = self.extract_price(full_text)
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                # Better description
                desc_elem = (
                    card.select_one('.description') or
                    card.select_one('.summary') or 
                    card.select_one('.excerpt') or
                    card.select_one('p')
                )
                description = desc_elem.get_text().strip()[:500] if desc_elem else full_text[:300]
                
                listing = {
                    'source': 'QuietLight',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': re.sub(r'\s+', ' ', description).strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing QuietLight listing: {e}")
                
        return listings

    def scrape_bizbuysell_comprehensive(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Comprehensive BizBuySell scraper"""
        listings = []
        
        # Check if this is Amazon stores page - use optimized extraction
        if 'amazon-stores-for-sale' in base_url:
            return self.scrape_bizbuysell_amazon_optimized(soup, base_url)
        
        # For general BizBuySell pages
        selectors = [
            'div.result',
            'div.listing', 
            'article.business-item',
            'div.search-result',
            'div[class*="listing"]',
            'div[class*="business"]',
            'div[class*="result"]'
        ]
        
        business_cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                business_cards = cards
                logger.info(f"BizBuySell: Found {len(cards)} cards with selector: {selector}")
                break

        for card in business_cards:
            try:
                # Multiple title strategies
                title_elem = (
                    card.select_one('h2') or
                    card.select_one('h3') or
                    card.select_one('h4') or
                    card.select_one('a[data-gtm-id]') or
                    card.select_one('.title a') or
                    card.find('a', href=True)
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 5:
                    continue

                # Extract URL
                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Get comprehensive text
                full_text = card.get_text()
                
                # Extract financials
                price = self.extract_price(full_text)
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                # Description
                desc_elem = (
                    card.select_one('p.description') or
                    card.select_one('div.summary') or
                    card.select_one('.description')
                )
                description = desc_elem.get_text().strip()[:500] if desc_elem else full_text[:300]
                
                listing = {
                    'source': 'BizBuySell',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': re.sub(r'\s+', ' ', description).strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing BizBuySell listing: {e}")
        return listings

    def scrape_bizbuysell_amazon_optimized(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Optimized BizBuySell Amazon stores scraper with fixed financial extraction"""
        listings = []
        
        # Use opportunity links strategy
        opportunity_links = soup.select('a[href*="opportunity"]')
        logger.info(f"BizBuySell Amazon: Found {len(opportunity_links)} opportunity links")
        
        for link in opportunity_links:
            try:
                href = link.get('href', '')
                if not href or '/business-opportunity/' not in href:
                    continue
                    
                full_url = urljoin(base_url, href)
                link_text = link.get_text().strip()
                
                # Skip navigation
                if any(skip in link_text.lower() for skip in ['register', 'login', 'sign up', 'learn more', 'see more', 'contact']):
                    continue
                
                if len(link_text) < 15:
                    continue
                
                # Get parent container for financial data
                parent = link.find_parent(['div'])
                context_levels = 0
                while parent and context_levels < 5:  # Go up more levels
                    if parent.parent:
                        parent = parent.parent
                        context_levels += 1
                    else:
                        break
                
                # Get all text from parent container
                if parent:
                    full_text = parent.get_text()
                else:
                    full_text = link.get_text()
                
                # Only include ecommerce/Amazon related
                if not any(keyword in full_text.lower() for keyword in ['amazon', 'fba', 'ecommerce', 'e-commerce', 'online', 'digital']):
                    continue
                
                # Extract financial data with debugging
                price = self.extract_price(full_text)
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                # Create description
                description = re.sub(r'\s+', ' ', full_text[:400]).strip()
                
                listing = {
                    'source': 'BizBuySell',
                    'name': link_text[:150],
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': full_url,
                    'multiple': '',
                }
                
                listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error processing BizBuySell opportunity link: {e}")
        
        return listings

    def scrape_generic_comprehensive(self, soup: BeautifulSoup, base_url: str, source_name: str) -> List[Dict]:
        """Comprehensive generic scraper for all other sites"""
        listings = []
        
        # Comprehensive selector strategy
        selectors = [
            'div.listing', 'div.business', 'div.item', 'div.card', 'div.result',
            'article', 'section[class*="listing"]', 'div[class*="property"]',
            'div[class*="business"]', 'div[class*="item"]', 'div[class*="auction"]',
            '.listing-card', '.business-card', '.auction-card', '.property-card'
        ]
        
        containers = []
        for selector in selectors:
            found = soup.select(selector)
            if found and len(found) > len(containers):
                containers = found
                logger.info(f"{source_name}: Found {len(found)} containers with: {selector}")
        
        # Fallback to any div with class
        if not containers:
            containers = soup.find_all(['div', 'article'], {'class': True})[:50]

        for container in containers:
            try:
                # Multiple title strategies
                title_elem = (
                    container.find(['h1', 'h2', 'h3', 'h4', 'h5']) or
                    container.find('a', href=True)
                )
                
                title = title_elem.get_text().strip() if title_elem else ""

                # Filter out navigation
                if not title or len(title) < 5 or any(
                    skip_word in title.lower() for skip_word in [
                        'menu', 'navigation', 'footer', 'header', 'search', 'filter',
                        'login', 'register', 'contact', 'about', 'privacy', 'terms',
                        'sponsored', 'advertisement'
                    ]
                ):
                    continue

                # Extract URL
                link_elem = container.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Get full text
                full_text = container.get_text()
                
                # Extract financials
                price = self.extract_price(full_text)
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                # Description
                desc_elem = container.find(['p', 'div'], string=lambda text: text and len(text.strip()) > 50)
                description = desc_elem.get_text().strip()[:500] if desc_elem else full_text[:300]
                
                listing = {
                    'source': source_name,
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': re.sub(r'\s+', ' ', description).strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                
                # Only include if it has some valuable data
                if title and (price or revenue or profit or len(description) > 50):
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing {source_name} listing: {e}")
        
        return listings

    def scrape_url(self, url: str) -> List[Dict]:
        """Scrape a single URL with comprehensive strategy"""
        # Determine if we need JavaScript rendering
        js_heavy_sites = ['empireflippers.com', 'flippa.com', 'acquire.com']
        use_render = any(site in url for site in js_heavy_sites)
        
        response = self.make_request(url, use_render=use_render)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        domain = urlparse(url).netloc.lower()
        
        # Route to appropriate scraper
        if 'quietlight' in domain:
            return self.scrape_quietlight_comprehensive(soup, url)
        elif 'bizbuysell' in domain:
            return self.scrape_bizbuysell_comprehensive(soup, url)
        else:
            source_name = domain.replace('www.', '').replace('.com', '').title()
            return self.scrape_generic_comprehensive(soup, url, source_name)

    def calculate_multiple(self, listing: Dict) -> str:
        """Calculate business multiple"""
        try:
            price_str = listing.get('price', '')
            revenue_str = listing.get('revenue', '')
            profit_str = listing.get('profit', '')
            
            if not price_str:
                return ""
            
            # Extract numeric values
            price_val = self.extract_numeric_value(price_str)
            
            # Try revenue multiple first
            if revenue_str and price_val:
                revenue_val = self.extract_numeric_value(revenue_str)
                if revenue_val and revenue_val > 0:
                    multiple = round(price_val / revenue_val, 2)
                    return f"{multiple}x"
            
            # Try profit multiple
            if profit_str and price_val:
                profit_val = self.extract_numeric_value(profit_str)
                if profit_val and profit_val > 0:
                    multiple = round(price_val / profit_val, 2)
                    return f"{multiple}x (P/E)"
                    
            return ""
        except:
            return ""

    def extract_numeric_value(self, value_str: str) -> Optional[float]:
        """Extract numeric value from currency string"""
        try:
            if not value_str:
                return None
                
            # Remove currency symbols and clean
            clean_str = re.sub(r'[^\d.,KkMm]', '', value_str)
            if not clean_str or not any(c.isdigit() for c in clean_str):
                return None
            
            # Handle K/M suffixes
            multiplier = 1
            if clean_str.upper().endswith('K'):
                multiplier = 1000
                clean_str = clean_str[:-1]
            elif clean_str.upper().endswith('M'):
                multiplier = 1000000
                clean_str = clean_str[:-1]
            
            # Convert to float
            numeric_val = float(clean_str.replace(',', ''))
            return numeric_val * multiplier
            
        except (ValueError, TypeError):
            return None

    def remove_duplicates(self, listings: List[Dict]) -> List[Dict]:
        """Remove duplicates based on URL and name"""
        unique_listings = []
        seen_signatures = set()
        
        for listing in listings:
            # Create signature from URL and name
            url = listing.get('url', '').lower().strip()
            name = listing.get('name', '').lower().strip()[:50]
            signature = f"{urlparse(url).netloc}_{urlparse(url).path}_{name}"
            signature = re.sub(r'[^\w]', '_', signature)
            
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                # Calculate multiple
                listing['multiple'] = self.calculate_multiple(listing)
                unique_listings.append(listing)
        
        return unique_listings

    def scrape_all_comprehensive(self) -> None:
        """Scrape all URLs with comprehensive coverage"""
        logger.info("🚀 Starting COMPREHENSIVE business listings scraper...")
        logger.info(f"Will scrape {len(self.urls)} URLs")
        
        all_listings = []
        
        # Process in smaller batches to avoid rate limiting
        batch_size = 8  # Smaller batches
        for i in range(0, len(self.urls), batch_size):
            batch = self.urls[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(self.urls) + batch_size - 1)//batch_size}")
            
            # Scrape batch in parallel
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                future_to_url = {executor.submit(self.scrape_url, url): url for url in batch}
                
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        listings = future.result()
                        with self.lock:
                            all_listings.extend(listings)
                        logger.info(f"✅ {url}: Found {len(listings)} listings")
                    except Exception as e:
                        logger.error(f"❌ {url}: Error - {e}")
            
            # Wait between batches
            time.sleep(5)
        
        logger.info(f"🎉 Total raw listings found: {len(all_listings)}")
        
        # Remove duplicates
        unique_listings = self.remove_duplicates(all_listings)
        logger.info(f"🎯 Unique listings after deduplication: {len(unique_listings)}")
        
        # Filter out invalid listings
        valid_listings = []
        for listing in unique_listings:
            name = str(listing.get('name', '')).lower()
            
            # Skip navigation/junk
            if any(skip in name for skip in ['log in', 'pricing', 'sellers', 'buyers', 'sponsored', 'advertisement']):
                continue
                
            # Must have some valuable data
            if not any([listing.get('price'), listing.get('revenue'), listing.get('profit')]):
                continue
                
            # Name must be reasonable
            if len(str(listing.get('name', ''))) < 10:
                continue
                
            valid_listings.append(listing)
        
        self.scraped_data = valid_listings
        logger.info(f"✅ Valid listings after filtering: {len(valid_listings)}")

    def export_to_csv(self, filename: str = 'COMPREHENSIVE_BUSINESS_LISTINGS.csv') -> None:
        """Export comprehensive results to CSV"""
        if not self.scraped_data:
            logger.error("No data to export!")
            return
        
        df = pd.DataFrame(self.scraped_data)
        column_order = ['source', 'name', 'price', 'revenue', 'profit', 'multiple', 'description', 'url']
        df = df[column_order]
        
        # Sort by source and price
        df['price_numeric'] = df['price'].apply(lambda x: 
            float(re.sub(r'[^\d.]', '', str(x))) if x and re.search(r'[\d.]', str(x)) else 0
        )
        df = df.sort_values(['source', 'price_numeric'], ascending=[True, False])
        df = df.drop('price_numeric', axis=1)
        
        df.to_csv(filename, index=False, quoting=1)
        logger.info(f"💾 Data exported to {filename}")

    def print_comprehensive_summary(self) -> None:
        """Print comprehensive summary"""
        if not self.scraped_data:
            logger.error("No data to summarize!")
            return
        
        df = pd.DataFrame(self.scraped_data)
        
        print(f"\n{'='*80}")
        print("🏆 COMPREHENSIVE BUSINESS LISTINGS HARVEST COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Total unique businesses: {len(df)}")
        print(f"📊 Listings with prices: {df['price'].str.len().gt(0).sum()}")
        print(f"📊 Listings with revenue: {df['revenue'].str.len().gt(0).sum()}")
        print(f"📊 Listings with profit: {df['profit'].str.len().gt(0).sum()}")
        
        # Source breakdown
        print(f"\n📈 COMPREHENSIVE SOURCE BREAKDOWN:")
        source_counts = df['source'].value_counts()
        for source, count in source_counts.items():
            percentage = (count / len(df)) * 100
            print(f"  {source}: {count} listings ({percentage:.1f}%)")
        
        # Price analysis
        prices = df[df['price'].str.len() > 0]['price']
        if not prices.empty:
            price_values = prices.apply(lambda x: float(re.sub(r'[^\d.]', '', x)) if re.search(r'[\d.]', x) else 0)
            price_values = price_values[price_values > 0]
            if not price_values.empty:
                print(f"\n💰 PRICE ANALYSIS:")
                print(f"  Range: ${price_values.min():,.0f} - ${price_values.max():,.0f}")
                print(f"  Average: ${price_values.mean():,.0f}")
                print(f"  Median: ${price_values.median():,.0f}")
        
        # Show sample of high-value listings
        valuable = df[df['price'].str.len() > 0].head(10)
        if not valuable.empty:
            print(f"\n🎯 TOP 10 HIGH-VALUE OPPORTUNITIES:")
            for i, (_, row) in enumerate(valuable.iterrows()):
                print(f"  {i+1}. {row['name'][:70]}{'...' if len(row['name']) > 70 else ''}")
                if row['price']:
                    print(f"     💰 Price: {row['price']}")
                if row['revenue']:
                    print(f"     📈 Revenue: {row['revenue']}")
                if row['profit']:
                    print(f"     💵 Profit: {row['profit']}")
                print(f"     🔗 Source: {row['source']}")
                print()

def main():
    """Main function"""
    scraper = ComprehensiveBusinessScraper()
    scraper.scrape_all_comprehensive()
    scraper.export_to_csv()
    scraper.print_comprehensive_summary()

if __name__ == "__main__":
    main() 