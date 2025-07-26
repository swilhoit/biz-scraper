#!/usr/bin/env python3

import os
import requests
import pandas as pd
import re
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
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

class FixedComprehensiveBusinessScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.scraped_data = []
        self.processed_urls = set()  # Track processed URLs to avoid duplicates
        self.lock = threading.Lock()
        
        # Fixed URL generation with proper pagination
        self.urls = self.generate_fixed_urls()

    def generate_fixed_urls(self) -> List[str]:
        """Generate properly structured URLs with fixed pagination"""
        urls = []
        
        # FIXED: QuietLight - Use individual listing pages instead of pagination
        # The pagination was returning same content, so let's try different approach
        quietlight_base_urls = [
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://quietlight.com/saas-businesses-for-sale/",
            "https://quietlight.com/content-businesses-for-sale/",
            "https://quietlight.com/ecommerce-businesses-for-sale/",
        ]
        urls.extend(quietlight_base_urls)
        
        # EXPANDED: BizQuest - Go much deeper (50 pages instead of 5)
        for page in range(1, 51):  # 50 pages
            urls.append(f"https://www.bizquest.com/business-for-sale/page-{page}/")
            
        # Additional BizQuest categories
        bizquest_categories = [
            "ecommerce-business-for-sale",
            "amazon-business-for-sale", 
            "online-business-for-sale",
            "technology-business-for-sale",
            "retail-business-for-sale"
        ]
        for category in bizquest_categories:
            for page in range(1, 21):  # 20 pages each
                urls.append(f"https://www.bizquest.com/{category}/page-{page}/")
        
        # FIXED: BizBuySell - Use search queries for better coverage
        bizbuysell_searches = [
            "amazon-stores-for-sale",
            "ecommerce-business-for-sale", 
            "online-business-for-sale",
            "digital-business-for-sale",
            "internet-business-for-sale"
        ]
        for search in bizbuysell_searches:
            for page in range(1, 21):  # 20 pages each
                if page == 1:
                    urls.append(f"https://www.bizbuysell.com/{search}/")
                else:
                    urls.append(f"https://www.bizbuysell.com/{search}/?page={page}")
        
        # EXPANDED: Flippa - Add more categories and deeper pagination
        flippa_categories = [
            "amazon-fba",
            "ecommerce", 
            "dropshipping",
            "saas",
            "starter-sites",
            "established-websites"
        ]
        for category in flippa_categories:
            for page in range(1, 11):  # 10 pages each
                urls.append(f"https://flippa.com/buy/monetization/{category}?page={page}")
        
        # NEW MARKETPLACES: Add many more sources
        additional_marketplaces = [
            # Website Properties
            "https://websiteproperties.com/amazon-fba-business-for-sale/",
            "https://websiteproperties.com/ecommerce-business-for-sale/",
            "https://websiteproperties.com/saas-business-for-sale/",
            
            # Investors Club  
            "https://investors.club/tech-stack/amazon-fba/",
            "https://investors.club/tech-stack/ecommerce/",
            "https://investors.club/tech-stack/saas/",
            
            # Acquire.com
            "https://acquire.com/amazon-fba-for-sale/",
            "https://acquire.com/ecommerce-for-sale/", 
            "https://acquire.com/saas-for-sale/",
            
            # FE International  
            "https://feinternational.com/buy-a-website/amazon-fba/",
            "https://feinternational.com/buy-a-website/ecommerce/",
            "https://feinternational.com/buy-a-website/saas/",
            
            # Digital Exits
            "https://digitalexits.com/businesses-for-sale/",
            
            # Motion Invest
            "https://motioninvest.com/marketplace/",
            
            # Latona's
            "https://latonas.com/business-for-sale/",
            
            # Business Broker Network
            "https://www.businessbroker.net/",
            
            # BizBen
            "https://www.bizben.com/businesses-for-sale/",
            
            # LoopNet Business
            "https://www.loopnet.com/biz/amazon-stores-for-sale/",
            "https://www.loopnet.com/biz/ecommerce-business-for-sale/",
        ]
        urls.extend(additional_marketplaces)
        
        logger.info(f"Generated {len(urls)} URLs for comprehensive fixed scraping")
        return urls

    def make_request(self, url: str, retries: int = 3, use_render: bool = False) -> Optional[requests.Response]:
        """Enhanced request handling with better error recovery"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',  # Use US proxy
        }
        
        # Use render for JavaScript-heavy sites
        js_sites = ['empireflippers', 'flippa', 'motioninvest']
        if use_render or any(site in url.lower() for site in js_sites):
            params['render'] = 'true'
        
        for attempt in range(retries):
            try:
                # Randomized delays to avoid rate limiting
                time.sleep(random.uniform(2, 5))
                
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=120)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 500:
                    logger.warning(f"Server error for {url}, retrying...")
                    time.sleep(random.uniform(5, 10))
                else:
                    response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(2, 5)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
        return None

    def extract_financial_value(self, text: str, patterns: List[str]) -> str:
        """FIXED: Enhanced financial extraction with better patterns"""
        if not text:
            return ""
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                if isinstance(match, tuple):
                    # Handle multiple capture groups
                    match = next((m for m in match if m and m.strip()), match[-1])
                
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
                    
                    # Validate business value ranges
                    if 5000 <= value <= 200000000:  # $5K to $200M range
                        return f"${value:,.0f}"
                        
                except (ValueError, TypeError):
                    continue
        
        return ""

    def extract_price_enhanced(self, text: str) -> str:
        """FIXED: Enhanced price extraction with comprehensive patterns"""
        patterns = [
            # Direct price patterns
            r'asking\s*price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'price[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'asking[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'listed\s*(?:at|for)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            
            # Currency symbol patterns
            r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
            r'USD\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)[KkMm]?',
            
            # Value followed by context
            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)[KkMm]?\s*(?:asking|price|listed)',
            
            # BizBuySell specific patterns
            r'save\s*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'(?:^|\s)(\d{6,})\s*(?:price|asking|listed|save)',
        ]
        return self.extract_financial_value(text, patterns)

    def extract_revenue_enhanced(self, text: str) -> str:
        """FIXED: Enhanced revenue extraction"""
        patterns = [
            # Standard revenue patterns
            r'revenue[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'sales[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'gross[:\s]*(?:sales|revenue)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'annual\s*(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            
            # TTM patterns
            r'ttm\s*(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'trailing\s*twelve\s*months?[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            
            # Year-specific patterns
            r'(\d{4})\s*(?:revenue|sales)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*)[KkMm]?\s*(?:revenue|sales|gross)',
            
            # Million/K patterns
            r'(\d+(?:\.\d+)?)[Mm]\s*(?:in\s*)?(?:revenue|sales)',
            r'(\d+(?:\.\d+)?)[Kk]\s*(?:in\s*)?(?:revenue|sales)',
        ]
        return self.extract_financial_value(text, patterns)

    def scrape_quietlight_fixed(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """FIXED: QuietLight scraper with better business identification"""
        listings = []
        
        # Multiple strategies to find business listings
        business_selectors = [
            'div.listing-item',
            'article.post',
            'div[class*="business"]',
            'div[class*="listing"]', 
            '.business-listing',
            '.listing-card',
            'div.row div.col',  # Grid layout
        ]
        
        business_cards = []
        for selector in business_selectors:
            cards = soup.select(selector)
            if cards and len(cards) >= 5:  # Must have reasonable number of cards
                business_cards = cards
                logger.info(f"QuietLight: Found {len(cards)} cards with: {selector}")
                break
        
        # Fallback: Look for links to individual listings
        if not business_cards:
            listing_links = soup.select('a[href*="/listings/"]')
            for link in listing_links[:20]:  # Limit to first 20
                parent = link.find_parent(['div', 'article'])
                if parent and parent not in business_cards:
                    business_cards.append(parent)
        
        for card in business_cards:
            try:
                # Enhanced title extraction
                title_elem = (
                    card.select_one('h1, h2, h3') or
                    card.select_one('a[href*="/listings/"]') or
                    card.find('a', href=re.compile(r'/listings/'))
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 15:
                    continue

                # Skip navigation elements
                if any(skip in title.lower() for skip in ['view all', 'see more', 'browse', 'search']):
                    continue

                # Extract URL - prioritize listing URLs
                link_elem = (
                    card.select_one('a[href*="/listings/"]') or
                    card.find('a', href=True)
                )
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Enhanced text extraction
                full_text = card.get_text()
                
                # FIXED: Better financial extraction
                price = self.extract_price_enhanced(full_text)
                revenue = self.extract_revenue_enhanced(full_text)
                profit = self.extract_profit_enhanced(full_text)
                
                # Better description extraction
                desc_candidates = [
                    card.select_one('.description'),
                    card.select_one('.summary'),
                    card.select_one('.excerpt'),
                    card.select_one('p')
                ]
                
                description = ""
                for candidate in desc_candidates:
                    if candidate and len(candidate.get_text().strip()) > 50:
                        description = candidate.get_text().strip()[:500]
                        break
                
                if not description:
                    description = re.sub(r'\s+', ' ', full_text[:400]).strip()
                
                listing = {
                    'source': 'QuietLight',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                    'multiple': '',
                }
                
                # Only include if has meaningful data
                if title and (price or revenue or any(keyword in title.lower() for keyword in ['amazon', 'fba', 'ecommerce'])):
                    listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing QuietLight listing: {e}")
                
        return listings

    def extract_profit_enhanced(self, text: str) -> str:
        """FIXED: Enhanced profit extraction"""
        patterns = [
            # Standard profit patterns
            r'cash\s*flow[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'profit[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'net\s*(?:income|profit)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            
            # SDE patterns
            r'sde[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'seller\s*discretionary\s*earnings[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            
            # EBITDA patterns
            r'ebitda[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'earnings[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            
            # Income patterns
            r'income[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
            r'monthly\s*(?:profit|income|cash flow)[:\s]*\$?([\d,]+(?:\.\d+)?)[KkMm]?',
        ]
        return self.extract_financial_value(text, patterns)

    def scrape_bizquest_enhanced(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """ENHANCED: BizQuest scraper with better data extraction"""
        listings = []
        
        # BizQuest specific selectors
        selectors = [
            'div.result-item',
            'div.listing-item', 
            'div.business-listing',
            'article.business',
            'div[class*="listing"]',
            'div[class*="business"]',
            'div[class*="result"]'
        ]
        
        business_cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                business_cards = cards
                logger.info(f"BizQuest: Found {len(cards)} cards with: {selector}")
                break

        for card in business_cards:
            try:
                # Multiple title strategies
                title_elem = (
                    card.select_one('h2 a') or
                    card.select_one('h3 a') or
                    card.select_one('a.title') or
                    card.select_one('h2') or
                    card.select_one('h3') or
                    card.find('a', href=True)
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 10:
                    continue

                # Extract URL
                if title_elem.name == 'a':
                    listing_url = urljoin(base_url, title_elem.get('href', ''))
                else:
                    link_elem = card.find('a', href=True)
                    listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Enhanced text extraction
                full_text = card.get_text()
                
                # FIXED: Financial extraction with BizQuest patterns
                price = self.extract_price_enhanced(full_text)
                revenue = self.extract_revenue_enhanced(full_text)
                profit = self.extract_profit_enhanced(full_text)
                
                # Description
                desc_elem = (
                    card.select_one('.description') or
                    card.select_one('.summary') or
                    card.select_one('p')
                )
                description = desc_elem.get_text().strip()[:500] if desc_elem else full_text[:300]
                
                listing = {
                    'source': 'Bizquest',
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
                logger.debug(f"Error parsing BizQuest listing: {e}")
                
        return listings

    def scrape_url_fixed(self, url: str) -> List[Dict]:
        """FIXED: Enhanced URL scraping with better source detection"""
        # Skip if already processed
        if url in self.processed_urls:
            logger.info(f"Skipping already processed URL: {url}")
            return []
        
        self.processed_urls.add(url)
        
        response = self.make_request(url)
        if not response:
            return []

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Enhanced source detection
            domain = urlparse(url).netloc.lower()
            
            if 'quietlight' in domain:
                return self.scrape_quietlight_fixed(soup, url)
            elif 'bizquest' in domain:
                return self.scrape_bizquest_enhanced(soup, url)
            elif 'bizbuysell' in domain:
                return self.scrape_bizbuysell_fixed(soup, url)
            elif 'flippa' in domain:
                return self.scrape_flippa_enhanced(soup, url)
            elif any(site in domain for site in ['empireflippers', 'feinternational', 'digitalexits']):
                return self.scrape_premium_marketplace(soup, url, domain)
            else:
                return self.scrape_generic_enhanced(soup, url, domain)
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []

    def scrape_bizbuysell_fixed(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """FIXED: BizBuySell scraper with NO placeholder extraction"""
        listings = []
        
        # Use multiple strategies to find individual business listings
        opportunity_links = soup.select('a[href*="opportunity"]')
        logger.info(f"BizBuySell: Found {len(opportunity_links)} opportunity links")
        
        for link in opportunity_links:
            try:
                href = link.get('href', '')
                if not href or '/business-opportunity/' not in href:
                    continue
                    
                full_url = urljoin(base_url, href)
                link_text = link.get_text().strip()
                
                # Skip navigation elements
                if any(skip in link_text.lower() for skip in ['register', 'login', 'sign up', 'learn more', 'page', 'next', 'previous']):
                    continue
                
                if len(link_text) < 15:
                    continue
                
                # Find the IMMEDIATE parent container only (not going up 6 levels)
                # This prevents extracting header/template values
                immediate_parent = link.find_parent(['div', 'article', 'section'])
                if not immediate_parent:
                    immediate_parent = link
                
                # Get text from the immediate business listing container only
                container_text = immediate_parent.get_text()
                
                # CRITICAL: Validate that this is individual business data, not template data
                # Check for unique business identifiers
                has_unique_data = any([
                    len(set(re.findall(r'\$[\d,]+', container_text))) > 1,  # Multiple different prices
                    'location' in container_text.lower(),
                    'industry' in container_text.lower(),
                    'year' in container_text.lower(),
                    'established' in container_text.lower(),
                    len(container_text) > 200  # Substantial content
                ])
                
                if not has_unique_data:
                    logger.debug(f"Skipping template/header data for: {link_text[:30]}")
                    continue
                
                # Extract financial data from the specific business container
                price = self.extract_price_enhanced(container_text)
                revenue = self.extract_revenue_enhanced(container_text)
                profit = self.extract_profit_enhanced(container_text)
                
                # STRICT validation: Reject known placeholder patterns
                if self.is_placeholder_data(price, revenue, profit):
                    logger.debug(f"Rejected placeholder values: {price}, {revenue}, {profit}")
                    continue
                
                # Additional validation: Skip if all financial values are suspiciously identical
                financial_values = [v for v in [price, revenue, profit] if v and v != ""]
                if len(financial_values) >= 2:
                    # Check if values are too similar (indicating templates)
                    numeric_values = []
                    for val in financial_values:
                        try:
                            clean_val = re.sub(r'[^\d.]', '', val)
                            if clean_val:
                                numeric_values.append(float(clean_val))
                        except:
                            continue
                    
                    if len(numeric_values) >= 2:
                        # If values are too close, it might be template data
                        max_val = max(numeric_values)
                        min_val = min(numeric_values)
                        if max_val > 0 and (max_val - min_val) / max_val < 0.1:  # Within 10%
                            logger.debug(f"Rejected similar template values: {financial_values}")
                            continue
                
                # Create clean description from business-specific content
                description = re.sub(r'\s+', ' ', container_text[:300]).strip()
                
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
                
                # Only add if we have meaningful financial data
                if price or revenue or profit:
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error processing BizBuySell link: {e}")
        
        logger.info(f"BizBuySell: Extracted {len(listings)} valid listings (no placeholders)")
        return listings

    def is_placeholder_data(self, price: str, revenue: str, profit: str) -> bool:
        """Check if financial data matches known placeholder patterns"""
        # Known placeholder combinations
        placeholder_patterns = [
            ("$250,000", "$2,022", "$500,004"),
            ("$250,000", "$2,022", ""),
            ("$250,000", "", "$500,004"),
            ("", "$2,022", "$500,004"),
        ]
        
        current_data = (price, revenue, profit)
        
        for pattern in placeholder_patterns:
            if current_data == pattern:
                return True
                
        # Check for suspiciously round numbers that appear in templates
        suspicious_values = ["$250,000", "$500,000", "$1,000,000", "$2,022", "$500,004"]
        
        financial_values = [v for v in [price, revenue, profit] if v and v != ""]
        if len(financial_values) >= 2:
            suspicious_count = sum(1 for val in financial_values if val in suspicious_values)
            if suspicious_count >= 2:  # Multiple suspicious values
                return True
        
        return False

    def scrape_flippa_enhanced(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """ENHANCED: Flippa scraper with better extraction"""
        listings = []
        
        selectors = [
            'div[data-testid="listing-card"]',
            'div.listing-card',
            'div.auction-card',
            'div[class*="ListingCard"]',
            'div[class*="listing"]'
        ]
        
        business_cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                business_cards = cards
                logger.info(f"Flippa: Found {len(cards)} cards with: {selector}")
                break
        
        for card in business_cards:
            try:
                # Title extraction
                title_elem = (
                    card.select_one('h2') or
                    card.select_one('h3') or
                    card.select_one('[data-testid="listing-title"]') or
                    card.find('a', href=True)
                )
                
                title = title_elem.get_text().strip() if title_elem else ""
                if not title or len(title) < 10:
                    continue

                # URL extraction
                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = card.get_text()
                
                # Financial extraction
                price = self.extract_price_enhanced(full_text)
                revenue = self.extract_revenue_enhanced(full_text)
                profit = self.extract_profit_enhanced(full_text)
                
                description = re.sub(r'\s+', ' ', full_text[:400]).strip()
                
                listing = {
                    'source': 'Flippa',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description,
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
                
            except Exception as e:
                logger.debug(f"Error parsing Flippa listing: {e}")
                
        return listings

    def scrape_premium_marketplace(self, soup: BeautifulSoup, base_url: str, domain: str) -> List[Dict]:
        """NEW: Scraper for premium marketplaces"""
        listings = []
        source_name = domain.replace('www.', '').split('.')[0].title()
        
        # Generic premium marketplace patterns
        selectors = [
            'div.listing',
            'div.business-card',
            'div[class*="listing"]',
            'article',
            'div[class*="business"]'
        ]
        
        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                logger.info(f"{source_name}: Found {len(cards)} cards")
                break
        
        for card in cards[:20]:  # Limit to first 20
            try:
                title_elem = card.find(['h1', 'h2', 'h3', 'h4'])
                title = title_elem.get_text().strip() if title_elem else ""
                
                if not title or len(title) < 10:
                    continue
                
                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = card.get_text()
                
                price = self.extract_price_enhanced(full_text)
                revenue = self.extract_revenue_enhanced(full_text)
                profit = self.extract_profit_enhanced(full_text)
                
                listing = {
                    'source': source_name,
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': re.sub(r'\s+', ' ', full_text[:400]).strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                
                if title and (price or revenue):
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing {source_name} listing: {e}")
        
        return listings

    def scrape_generic_enhanced(self, soup: BeautifulSoup, base_url: str, domain: str) -> List[Dict]:
        """ENHANCED: Generic scraper for any marketplace"""
        listings = []
        source_name = domain.replace('www.', '').split('.')[0].title()
        
        # Comprehensive selector strategy
        selectors = [
            'div.listing', 'div.business', 'div.item', 'div.card', 
            'article', 'div[class*="listing"]', 'div[class*="business"]',
            'div[class*="property"]', 'div[class*="auction"]'
        ]
        
        for selector in selectors:
            containers = soup.select(selector)
            if containers and len(containers) >= 3:
                logger.info(f"{source_name}: Found {len(containers)} containers")
                break
        
        for container in containers[:30]:  # Process max 30 per page
            try:
                title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                title = title_elem.get_text().strip() if title_elem else ""

                if not title or len(title) < 8:
                    continue

                link_elem = container.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = container.get_text()
                
                price = self.extract_price_enhanced(full_text)
                revenue = self.extract_revenue_enhanced(full_text)
                profit = self.extract_profit_enhanced(full_text)
                
                listing = {
                    'source': source_name,
                    'name': title,
                    'price': price,
                    'revenue': revenue, 
                    'profit': profit,
                    'description': re.sub(r'\s+', ' ', full_text[:400]).strip(),
                    'url': listing_url,
                    'multiple': '',
                }
                
                if title and (price or revenue or len(full_text) > 100):
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error parsing {source_name} listing: {e}")
        
        return listings

    def scrape_all_fixed(self) -> None:
        """FIXED: Comprehensive scraping with better threading and deduplication"""
        logger.info(f"Starting comprehensive fixed scraping of {len(self.urls)} URLs")
        
        # Process in smaller batches to manage memory and rate limits
        batch_size = 15  # Reduced from 20
        batches = [self.urls[i:i + batch_size] for i in range(0, len(self.urls), batch_size)]
        
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} URLs)")
            
            with ThreadPoolExecutor(max_workers=10) as executor:  # Reduced workers
                future_to_url = {executor.submit(self.scrape_url_fixed, url): url for url in batch}
                
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        listings = future.result()
                        if listings:
                            with self.lock:
                                self.scraped_data.extend(listings)
                                logger.info(f"Added {len(listings)} listings from {url}")
                    except Exception as e:
                        logger.error(f"Error processing {url}: {e}")
            
            # Delay between batches
            time.sleep(random.uniform(3, 6))
        
        logger.info(f"Completed scraping. Total raw listings: {len(self.scraped_data)}")

    def remove_duplicates_enhanced(self) -> List[Dict]:
        """ENHANCED: Better duplicate removal"""
        if not self.scraped_data:
            return []

        df = pd.DataFrame(self.scraped_data)
        logger.info(f"Starting deduplication with {len(df)} raw listings")
        
        # Remove exact URL duplicates
        df = df.drop_duplicates(subset=['url'], keep='first')
        logger.info(f"After URL deduplication: {len(df)} listings")
        
        # Remove "Unlock Listing" and placeholder entries
        mask = (
            ~df['name'].str.contains('unlock listing', case=False, na=False) &
            ~df['name'].str.contains('sponsored', case=False, na=False) &
            ~df['name'].str.contains('advertisement', case=False, na=False)
        )
        df = df[mask]
        logger.info(f"After removing placeholders: {len(df)} listings")
        
        # Remove duplicate names (similar businesses)
        df['name_normalized'] = df['name'].str.lower().str[:60]
        df = df.drop_duplicates(subset=['name_normalized'], keep='first')
        df = df.drop(columns=['name_normalized'])
        logger.info(f"After name deduplication: {len(df)} listings")
        
        # Remove rows with insufficient data
        df = df[df['name'].str.len() >= 10]
        logger.info(f"After quality filter: {len(df)} listings")
        
        return df.to_dict('records')

    def export_to_csv_fixed(self, filename: str = 'FIXED_COMPREHENSIVE_BUSINESS_LISTINGS.csv') -> None:
        """Export fixed results to CSV"""
        if not self.scraped_data:
            logger.warning("No data to export")
            return

        # Remove duplicates
        clean_data = self.remove_duplicates_enhanced()
        
        df = pd.DataFrame(clean_data)
        df.to_csv(filename, index=False)
        logger.info(f"Exported {len(df)} unique businesses to {filename}")
        
        # Print summary
        self.print_fixed_summary(df)

    def print_fixed_summary(self, df: pd.DataFrame) -> None:
        """Print comprehensive summary of fixed results"""
        print("\n" + "="*80)
        print("🚀 FIXED COMPREHENSIVE SCRAPING RESULTS")
        print("="*80)
        
        print(f"\n📊 HARVEST SUMMARY:")
        print(f"Total unique businesses: {len(df):,}")
        
        # Source breakdown
        print(f"\n📈 SOURCE BREAKDOWN:")
        source_counts = df['source'].value_counts()
        for source, count in source_counts.items():
            percentage = (count / len(df)) * 100
            print(f"  {source}: {count:,} businesses ({percentage:.1f}%)")
        
        # Financial coverage
        price_coverage = df['price'].notna().sum()
        revenue_coverage = df['revenue'].notna().sum()
        profit_coverage = df['profit'].notna().sum()
        
        print(f"\n💰 FINANCIAL DATA COVERAGE:")
        print(f"  Price data: {price_coverage:,}/{len(df):,} ({price_coverage/len(df)*100:.1f}%)")
        print(f"  Revenue data: {revenue_coverage:,}/{len(df):,} ({revenue_coverage/len(df)*100:.1f}%)")
        print(f"  Profit data: {profit_coverage:,}/{len(df):,} ({profit_coverage/len(df)*100:.1f}%)")
        
        # Investment categories
        df['price_numeric'] = df['price'].str.replace(r'[^\d.]', '', regex=True).astype(float, errors='ignore')
        valid_prices = df[df['price_numeric'].notna() & (df['price_numeric'] > 0)]
        
        if len(valid_prices) > 0:
            high_value = valid_prices[valid_prices['price_numeric'] >= 1000000]
            medium_value = valid_prices[(valid_prices['price_numeric'] >= 100000) & (valid_prices['price_numeric'] < 1000000)]
            small_value = valid_prices[valid_prices['price_numeric'] < 100000]
            
            print(f"\n🎯 INVESTMENT CATEGORIES:")
            print(f"  High-value (>$1M): {len(high_value):,} businesses")
            print(f"  Medium ($100K-$1M): {len(medium_value):,} businesses")
            print(f"  Small (<$100K): {len(small_value):,} businesses")
            
            print(f"\n📈 PRICE ANALYSIS:")
            print(f"  Price range: ${valid_prices['price_numeric'].min():,.0f} - ${valid_prices['price_numeric'].max():,.0f}")
            print(f"  Average price: ${valid_prices['price_numeric'].mean():,.0f}")
            print(f"  Median price: ${valid_prices['price_numeric'].median():,.0f}")

def main():
    """Main execution function"""
    try:
        scraper = FixedComprehensiveBusinessScraper()
        
        # Run the fixed comprehensive scraping
        scraper.scrape_all_fixed()
        
        # Export results
        scraper.export_to_csv_fixed()
        
        logger.info("Fixed comprehensive scraping completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main() 