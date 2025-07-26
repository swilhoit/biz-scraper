#!/usr/bin/env python3
"""
Business Listings Scraper
Scrapes business listings from multiple marketplaces and exports to CSV
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

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BusinessScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.scraped_data = []
        self.lock = threading.Lock()
        
        # URLs to scrape
        self.urls = [
            "https://quietlight.com/amazon-fba-businesses-for-sale/",
            "https://www.bizbuysell.com/amazon-stores-for-sale/",
            "https://www.bizbuysell.com/amazon-stores-for-sale/?page=2",
            "https://www.bizbuysell.com/amazon-stores-for-sale/?page=3",
            "https://www.bizbuysell.com/amazon-stores-for-sale/?page=4",
            "https://www.bizbuysell.com/amazon-stores-for-sale/?page=5",
            "https://flippa.com/buy/monetization/amazon-fba",
            "https://www.loopnet.com/biz/amazon-stores-for-sale/",
            "https://empireflippers.com/marketplace/amazon-fba-businesses-for-sale/",
            "https://investors.club/tech-stack/amazon-fba/",
            "https://websiteproperties.com/amazon-fba-business-for-sale/",
            "https://www.bizquest.com/amazon-business-for-sale/",
            "https://acquire.com/amazon-fba-for-sale/"
        ]

    def make_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """Make a request using Scraper API with retries"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'render': 'true'  # Enable JavaScript rendering
        }
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=90)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
        return None

    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace and newlines
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove special characters but keep alphanumeric, punctuation, and currency symbols
        text = re.sub(r'[^\w\s\.,\$\-\(\)%]', '', text)
        return text.strip()

    def extract_price(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return ""
        # Look for price patterns like $1,000,000 or $1M or $1.5M
        price_patterns = [
            r'\$[\d,]+(?:\.\d{2})?',  # $1,000,000.00
            r'\$\d+(?:\.\d+)?[KkMm]',  # $1.5M or $500K
            r'[\d,]+(?:\.\d{2})?\s*(?:USD|dollars?)',  # 1,000,000 USD
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group()
        return ""

    def extract_revenue(self, text: str) -> str:
        """Extract revenue information from text"""
        if not text:
            return ""
        # Look for revenue patterns
        revenue_patterns = [
            r'revenue[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'gross[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'sales[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self.clean_text(match.group())
        return ""

    def extract_profit(self, text: str) -> str:
        """Extract profit information from text"""
        if not text:
            return ""
        # Look for profit patterns
        profit_patterns = [
            r'profit[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'net[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'earnings[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'cash\s+flow[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
            r'sde[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',  # Seller's Discretionary Earnings
        ]
        
        for pattern in profit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self.clean_text(match.group())
        return ""
    
    def extract_price_improved(self, text: str) -> str:
        """Improved price extraction for BizBuySell"""
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
                    try:
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
                    except ValueError:
                        continue
        
        return ""

    def extract_revenue_improved(self, text: str) -> str:
        """Improved revenue extraction for BizBuySell"""
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
        """Improved profit extraction for BizBuySell"""
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

    def clean_description(self, text: str) -> str:
        """Clean and format description for BizBuySell"""
        # Take first 300 characters
        description = text[:300]
        # Clean up whitespace
        description = re.sub(r'\s+', ' ', description).strip()
        return description

    def scrape_quietlight(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Scrape QuietLight.com listings"""
        listings = []
        
        # QuietLight uses specific container classes
        business_cards = soup.select('div.listing-item, div.business-listing, article.post')
        
        # Fallback to more generic selectors
        if not business_cards:
            business_cards = soup.select('div[class*="listing"], div[class*="business"]')
        
        for card in business_cards:
            try:
                # More specific title selectors for QuietLight
                title_elem = card.select_one('h2.listing-title, h3.business-title, h2, h3')
                title = self.clean_text(title_elem.get_text()) if title_elem else ""
                
                if not title or len(title) < 10:  # Increased minimum length
                    continue

                # Extract individual listing URL
                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Look for price in multiple locations
                price_elem = card.select_one('.listing-price, .price, .asking-price')
                price = self.extract_price(price_elem.get_text()) if price_elem else ""
                
                # Better description extraction
                desc_elem = card.select_one('.description, .summary, .excerpt, p')
                description = self.clean_text(desc_elem.get_text()) if desc_elem else ""
                
                full_text = card.get_text()
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                listing = {
                    'source': 'QuietLight',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description[:500],
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
                
            except Exception as e:
                logger.warning(f"Error parsing QuietLight listing: {e}")
                
        return listings

    def scrape_empireflippers(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Scrape EmpireFlippers.com listings - Fixed parsing"""
        listings = []
        
        # Empire Flippers specific selectors
        business_cards = soup.select('div.listing-card, div.business-card, div.marketplace-listing')
        
        # Alternative selectors if primary ones don't work
        if not business_cards:
            business_cards = soup.select('div[data-listing-id], article.listing')
        
        # Even more generic fallback
        if not business_cards:
            business_cards = soup.select('div[class*="listing"], div[class*="business"], div[class*="card"]')
        
        for card in business_cards:
            try:
                # Skip if this looks like a button or navigation element
                card_text = card.get_text().lower()
                if any(skip in card_text for skip in ['unlock listing', 'view details', 'learn more', 'sign up', 'login']):
                    continue
                
                # Look for actual business titles - Empire Flippers often uses specific patterns
                title_elem = card.select_one(
                    'h3.listing-title, h2.business-title, .title, h3, h2, '
                    'a[class*="title"], div[class*="title"] a'
                )
                
                title = self.clean_text(title_elem.get_text()) if title_elem else ""
                
                # Filter out generic titles and navigation elements
                if not title or len(title) < 10 or any(skip in title.lower() for skip in [
                    'unlock', 'view', 'learn', 'sign', 'login', 'register', 'browse', 'filter'
                ]):
                    continue

                # Extract individual listing URL
                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                # Empire Flippers price selectors
                price_elem = card.select_one('.price, .asking-price, .listing-price, .value')
                price = self.extract_price(price_elem.get_text()) if price_elem else ""
                
                # Look for metrics in Empire Flippers format
                metrics_section = card.select_one('.metrics, .financials, .stats')
                full_text = card.get_text()
                
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                # Better description for Empire Flippers
                desc_elem = card.select_one('.description, .summary, .business-description')
                description = self.clean_text(desc_elem.get_text()) if desc_elem else ""
                
                if not description:
                    # Extract first meaningful paragraph
                    paragraphs = card.find_all('p')
                    for p in paragraphs:
                        text = self.clean_text(p.get_text())
                        if len(text) > 30:
                            description = text
                            break
                
                listing = {
                    'source': 'EmpireFlippers',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description[:500],
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
                
            except Exception as e:
                logger.warning(f"Error parsing EmpireFlippers listing: {e}")
                
        return listings

    def scrape_bizbuysell(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Scrape BizBuySell.com listings - Optimized for Amazon stores"""
        listings = []
        
        # Check if this is the Amazon stores page - use optimized extraction
        if 'amazon-stores-for-sale' in base_url:
            return self.scrape_bizbuysell_amazon_optimized(soup, base_url)
        
        # For general BizBuySell pages, use existing logic
        business_cards = soup.select('div.result, div.listing, article.business-item, div.search-result')

        for card in business_cards:
            try:
                title_elem = card.select_one('h2, h3, h4, a[data-gtm-id], .title a')
                title = self.clean_text(title_elem.get_text()) if title_elem else ""
                
                if not title or len(title) < 5:
                    continue

                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                price_elem = card.select_one('.price, .listing-price, .asking-price')
                price = self.extract_price(price_elem.get_text()) if price_elem else ""
                
                desc_elem = card.select_one('p.description, div.summary, .description')
                description = self.clean_text(desc_elem.get_text()) if desc_elem else ""
                
                full_text = card.get_text()
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                listing = {
                    'source': 'BizBuySell',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description[:500],
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
            except Exception as e:
                logger.warning(f"Error parsing BizBuySell listing: {e}")
        return listings
    
    def scrape_bizbuysell_amazon_optimized(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Optimized scraper for BizBuySell Amazon stores section"""
        listings = []
        
        # Use the working opportunity links strategy
        opportunity_links = soup.select('a[href*="opportunity"]')
        logger.info(f"  Found {len(opportunity_links)} opportunity links on BizBuySell Amazon page")
        
        for link in opportunity_links:
            try:
                href = link.get('href', '')
                if not href or '/business-opportunity/' not in href:
                    continue
                    
                full_url = urljoin(base_url, href)
                link_text = link.get_text().strip()
                
                # Skip navigation/UI links
                if any(skip in link_text.lower() for skip in ['register', 'login', 'sign up', 'learn more', 'see more', 'contact']):
                    continue
                
                # Skip very short names
                if len(link_text) < 15:
                    continue
                
                # Get comprehensive business data
                listing = self.extract_bizbuysell_comprehensive_data(link, full_url, soup)
                if listing:
                    listings.append(listing)
                    
            except Exception as e:
                logger.debug(f"Error processing BizBuySell opportunity link: {e}")
        
        return listings
    
    def extract_bizbuysell_comprehensive_data(self, link, full_url: str, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract comprehensive business data from BizBuySell with proper financial parsing"""
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
                'multiple': '',
            }
            
            return listing
            
        except Exception as e:
            logger.debug(f"Error extracting BizBuySell comprehensive data: {e}")
            return None

    def scrape_flippa(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Scrape Flippa.com listings"""
        listings = []
        business_cards = soup.select('div.listing-card, article.auction-item, div.auction-card')
        
        for card in business_cards:
            try:
                title_elem = card.select_one('h2, h3, a.listing-title, .title a')
                title = self.clean_text(title_elem.get_text()) if title_elem else ""
                
                if not title or len(title) < 5:
                    continue

                link_elem = card.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                price_elem = card.select_one('.price, .amount, .current-bid, .starting-price')
                price = self.extract_price(price_elem.get_text()) if price_elem else ""
                
                full_text = card.get_text()
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                listing = {
                    'source': 'Flippa',
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': self.clean_text(card.get_text())[:500],
                    'url': listing_url,
                    'multiple': '',
                }
                listings.append(listing)
            except Exception as e:
                logger.warning(f"Error parsing Flippa listing: {e}")
        return listings

    def scrape_generic(self, soup: BeautifulSoup, base_url: str, source_name: str) -> List[Dict]:
        """Generic scraper for other sites"""
        listings = []
        
        possible_containers = soup.select(
            'div.listing, div.business, div.item, div.card, div.result, '
            'article, section[class*="listing"], div[class*="property"], '
            'div[class*="business"], div[class*="item"]'
        )

        if not possible_containers:
            possible_containers = soup.find_all(['div', 'article'], {'class': True})[:30]

        for container in possible_containers:
            try:
                title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'a'])
                title = self.clean_text(title_elem.get_text()) if title_elem else ""

                if not title or len(title) < 5 or any(
                    skip_word in title.lower() for skip_word in [
                        'menu', 'navigation', 'footer', 'header', 'search', 'filter',
                        'login', 'register', 'contact', 'about', 'privacy', 'terms'
                    ]
                ):
                    continue

                link_elem = container.find('a', href=True)
                listing_url = urljoin(base_url, link_elem['href']) if link_elem else base_url
                
                full_text = container.get_text()
                price = self.extract_price(full_text)
                revenue = self.extract_revenue(full_text)
                profit = self.extract_profit(full_text)
                
                desc_elem = container.find(['p', 'div'], string=lambda text: text and len(text.strip()) > 50)
                description = self.clean_text(desc_elem.get_text()) if desc_elem else self.clean_text(full_text)[:200]
                
                listing = {
                    'source': source_name,
                    'name': title,
                    'price': price,
                    'revenue': revenue,
                    'profit': profit,
                    'description': description[:500],
                    'url': listing_url,
                    'multiple': '',
                }
                
                if title and (price or revenue or profit or len(description) > 20):
                    listings.append(listing)
            except Exception as e:
                logger.warning(f"Error parsing {source_name} listing: {e}")
        return listings

    def scrape_url(self, url: str) -> List[Dict]:
        """Scrape a single URL and return business listings"""
        response = self.make_request(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        domain = urlparse(url).netloc.lower()
        
        if 'quietlight' in domain:
            return self.scrape_quietlight(soup, url)
        elif 'empireflippers' in domain:
            return self.scrape_empireflippers(soup, url)  # Fixed method call
        elif 'bizbuysell' in domain:
            return self.scrape_bizbuysell(soup, url)
        elif 'flippa' in domain:
            return self.scrape_flippa(soup, url)
        else:
            source_name = domain.replace('www.', '').replace('.com', '').title()
            return self.scrape_generic(soup, url, source_name)

    def process_listings_parallel(self, listings: List[Dict]) -> List[Dict]:
        """Process listings in parallel for better performance"""
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all listings for processing
            futures = [executor.submit(self.process_single_listing, listing) for listing in listings]
            
            processed_listings = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        processed_listings.append(result)
                except Exception as e:
                    logger.warning(f"Error processing listing: {e}")
            
            return processed_listings

    def process_single_listing(self, listing: Dict) -> Dict:
        """Process a single listing (calculate multiples, validate data)"""
        try:
            # Calculate multiples using both revenue and profit
            price_str = listing.get('price', '')
            revenue_str = listing.get('revenue', '')
            profit_str = listing.get('profit', '')
            
            # Calculate revenue multiple
            if price_str and revenue_str:
                price_val = self.extract_numeric_value(price_str)
                revenue_val = self.extract_numeric_value(revenue_str)
                
                if price_val and revenue_val and revenue_val > 0:
                    multiple = round(price_val / revenue_val, 2)
                    listing['multiple'] = f"{multiple}x"
            
            # If no revenue multiple, try profit multiple
            elif price_str and profit_str and not listing.get('multiple'):
                price_val = self.extract_numeric_value(price_str)
                profit_val = self.extract_numeric_value(profit_str)
                
                if price_val and profit_val and profit_val > 0:
                    multiple = round(price_val / profit_val, 2)
                    listing['multiple'] = f"{multiple}x (P/E)"
            
            return listing
            
        except Exception as e:
            logger.debug(f"Error processing listing: {e}")
            return listing
    
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

    def scrape_all_parallel(self) -> None:
        """Scrape all URLs in parallel for maximum speed"""
        logger.info("Starting parallel business listings scraper...")
        
        all_listings = []
        
        # Scrape all URLs in parallel (up to 20 concurrent as per ScraperAPI limit)
        with ThreadPoolExecutor(max_workers=min(len(self.urls), 20)) as executor:
            # Submit all URL scraping tasks
            future_to_url = {executor.submit(self.scrape_url, url): url for url in self.urls}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    listings = future.result()
                    with self.lock:
                        all_listings.extend(listings)
                    logger.info(f"Found {len(listings)} listings from {url}")
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
        
        # Process all listings in parallel
        logger.info(f"Processing {len(all_listings)} listings in parallel...")
        processed_listings = self.process_listings_parallel(all_listings)
        
        # Remove duplicates based on URL
        unique_listings = self.remove_duplicates(processed_listings)
        
        self.scraped_data = unique_listings
        logger.info(f"Total unique listings found: {len(self.scraped_data)}")

    def remove_duplicates(self, listings: List[Dict]) -> List[Dict]:
        """Remove duplicate listings based on URL"""
        unique_listings = []
        seen_urls = set()
        
        for listing in listings:
            url = listing.get('url', '').lower().strip()
            
            # Skip if URL is generic or missing
            if not url or url.count('/') <= 3:
                continue

            if url not in seen_urls:
                seen_urls.add(url)
                unique_listings.append(listing)
        
        return unique_listings

    def export_to_csv(self, filename: str = 'business_listings.csv') -> None:
        """Export scraped data to CSV"""
        if not self.scraped_data:
            logger.warning("No data to export")
            return
        
        df = pd.DataFrame(self.scraped_data)
        
        # Reorder columns for better readability
        column_order = ['source', 'name', 'price', 'revenue', 'profit', 'multiple', 'description', 'url']
        df = df[column_order]
        
        df.to_csv(filename, index=False)
        logger.info(f"Data exported to {filename}")
        logger.info(f"Total listings exported: {len(df)}")

    def print_summary(self) -> None:
        """Print a summary of scraped data"""
        if not self.scraped_data:
            logger.info("No data scraped")
            return
        
        df = pd.DataFrame(self.scraped_data)
        
        print("\n" + "="*50)
        print("SCRAPING SUMMARY")
        print("="*50)
        print(f"Total listings found: {len(df)}")
        print(f"Sources: {df['source'].nunique()}")
        print(f"Listings with prices: {df['price'].str.len().gt(0).sum()}")
        print(f"Listings with revenue: {df['revenue'].str.len().gt(0).sum()}")
        print(f"Listings with profit: {df['profit'].str.len().gt(0).sum()}")
        
        print("\nListings per source:")
        source_counts = df['source'].value_counts()
        for source, count in source_counts.items():
            print(f"  {source}: {count}")
        
        # Show sample listings
        print("\nSample listings:")
        for i, row in df.head(3).iterrows():
            print(f"\n{i+1}. {row['name'][:60]}...")
            print(f"   Source: {row['source']}")
            print(f"   Price: {row['price']}")
            print(f"   Revenue: {row['revenue']}")
            if row['multiple']:
                print(f"   Multiple: {row['multiple']}")


def main():
    """Main function to run the scraper"""
    try:
        scraper = BusinessScraper()
        scraper.scrape_all_parallel()  # Use parallel scraping
        scraper.export_to_csv()
        scraper.print_summary()
        
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        raise


if __name__ == "__main__":
    main()