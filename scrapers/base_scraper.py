import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
from database import Business, get_session
from sqlalchemy.exc import IntegrityError
from config import SCRAPER_API_URL, SCRAPER_API_PARAMS
from utils.amazon_detector import AmazonFBADetector

class BaseScraper(ABC):
    def __init__(self, site_config: Dict):
        self.site_config = site_config
        self.name = site_config['name']
        self.base_url = site_config['base_url']
        self.session = requests.Session()
        self.db_session = get_session()
        self.logger = logging.getLogger(self.name)
        
        # Handle different URL configurations
        if 'search_urls' in site_config:
            self.search_urls = site_config['search_urls']
        elif 'search_url' in site_config:
            self.search_urls = [site_config['search_url']]
        else:
            self.search_urls = []
        
    def get_page(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Fetch a page using ScraperAPI"""
        params = SCRAPER_API_PARAMS.copy()
        params['url'] = url
        
        for attempt in range(retries):
            try:
                response = self.session.get(SCRAPER_API_URL, params=params, timeout=60)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                self.logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        return None
    
    def save_business(self, business_data: Dict) -> bool:
        """Save business to database"""
        try:
            business_data['source_site'] = self.name
            business_data['scraped_at'] = datetime.utcnow()
            
            # Add Amazon FBA detection
            business_data = AmazonFBADetector.enhance_listing(business_data)
            
            existing = self.db_session.query(Business).filter_by(
                listing_url=business_data.get('listing_url')
            ).first()
            
            if existing:
                for key, value in business_data.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                self.logger.info(f"Updated: {business_data.get('title', 'Unknown')}")
            else:
                # Filter out non-database fields
                db_fields = {k: v for k, v in business_data.items() 
                           if hasattr(Business, k)}
                business = Business(**db_fields)
                self.db_session.add(business)
                self.logger.info(f"Added: {business_data.get('title', 'Unknown')}")
            
            self.db_session.commit()
            return True
            
        except IntegrityError:
            self.db_session.rollback()
            self.logger.error(f"Database integrity error for {business_data.get('listing_url')}")
            return False
        except Exception as e:
            self.db_session.rollback()
            self.logger.error(f"Error saving business: {e}")
            return False
    
    def parse_price(self, price_text: str) -> Optional[float]:
        """Parse price from text"""
        if not price_text:
            return None
        
        # Remove currency symbols and commas
        price_text = price_text.replace('$', '').replace(',', '').strip()
        
        # Handle millions/thousands
        multiplier = 1
        if 'million' in price_text.lower() or price_text.upper().endswith('M'):
            multiplier = 1_000_000
            price_text = price_text.upper().replace('MILLION', '').replace('M', '')
        elif 'thousand' in price_text.lower() or price_text.upper().endswith('K'):
            multiplier = 1_000
            price_text = price_text.upper().replace('THOUSAND', '').replace('K', '')
        
        try:
            return float(price_text.strip()) * multiplier
        except ValueError:
            return None
    
    @abstractmethod
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get list of listing URLs to scrape"""
        pass
    
    @abstractmethod
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        pass
    
    def run(self, max_listings: Optional[int] = None):
        """Run the scraper"""
        self.logger.info(f"Starting {self.name} scraper")
        
        listing_urls = self.get_listing_urls(max_pages=10 if not max_listings else None)
        self.logger.info(f"Found {len(listing_urls)} listings")
        
        if max_listings:
            listing_urls = listing_urls[:max_listings]
        
        success_count = 0
        for i, url in enumerate(listing_urls, 1):
            self.logger.info(f"Scraping {i}/{len(listing_urls)}: {url}")
            
            listing_data = self.scrape_listing(url)
            if listing_data and self.save_business(listing_data):
                success_count += 1
            
            time.sleep(1)  # Be respectful
        
        self.logger.info(f"Completed {self.name} scraper. Saved {success_count}/{len(listing_urls)} listings")
        self.db_session.close()