import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Optional
from datetime import datetime
from database import get_session, get_table_class
from sqlalchemy.exc import IntegrityError
from config import SCRAPER_API_URL, SCRAPER_API_PARAMS
from utils.amazon_detector import AmazonFBADetector
import concurrent.futures

class BaseScraper(ABC):
    def __init__(self, site_config: Dict, max_workers: int = 10):
        self.site_config = site_config
        self.name = site_config['name']
        self.base_url = site_config['base_url']
        self.logger = logging.getLogger(self.name)
        self.max_workers = max_workers

        # Configure requests session with retries
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        
        # Handle different URL configurations
        if 'search_urls' in site_config:
            self.search_urls = site_config['search_urls']
        elif 'search_url' in site_config:
            self.search_urls = [site_config['search_url']]
        else:
            self.search_urls = []
        
    def get_page(self, url: str, render: bool = False) -> Optional[BeautifulSoup]:
        """Fetch a page using ScraperAPI"""
        params = SCRAPER_API_PARAMS.copy()
        params['url'] = url
        if render:
            params['render'] = 'true'
        
        try:
            response = self.session.get(SCRAPER_API_URL, params=params, timeout=120) # Increased timeout for JS rendering
            response.raise_for_status()
            return BeautifulSoup(response.content, 'lxml')
        except Exception as e:
            self.logger.warning(f"Error fetching {url}: {e}")
            return None
    
    def save_business(self, business_data: Dict) -> bool:
        """Save business to the site-specific database table."""
        TableClass = get_table_class(self.name)
        if not TableClass:
            self.logger.error(f"Could not find table class for site: {self.name}")
            return False

        session = get_session()
        try:
            business_data['scraped_at'] = datetime.utcnow()
            
            # The Amazon detector can remain as it enhances the data dict
            business_data = AmazonFBADetector.enhance_listing(business_data)
            
            existing = session.query(TableClass).filter_by(
                listing_url=business_data.get('listing_url')
            ).first()
            
            if existing:
                for key, value in business_data.items():
                    # Ensure the attribute exists before setting it
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                self.logger.info(f"Updated: {business_data.get('title', 'Unknown')}")
            else:
                # Filter out keys that don't exist in the table model
                db_fields = {k: v for k, v in business_data.items() if hasattr(TableClass, k)}
                business = TableClass(**db_fields)
                session.add(business)
                self.logger.info(f"Added: {business_data.get('title', 'Unknown')}")
            
            session.commit()
            return True
        except IntegrityError:
            session.rollback()
            self.logger.error(f"Database integrity error for {business_data.get('listing_url')}")
            return False
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error saving business: {e}", exc_info=True)
            return False
        finally:
            session.close()
    
    def parse_price(self, price_text: str) -> Optional[float]:
        """Parse price from text, handling K/M multipliers."""
        if not price_text:
            return None
        
        price_text = price_text.lower().replace('$', '').replace(',', '').strip()
        
        multiplier = 1
        if 'm' in price_text:
            multiplier = 1_000_000
            price_text = price_text.replace('m', '')
        elif 'k' in price_text:
            multiplier = 1_000
            price_text = price_text.replace('k', '')
        
        try:
            # Handle cases like "1.2m" -> 1.2 * 1,000,000
            return float(price_text.strip()) * multiplier
        except (ValueError, TypeError):
            self.logger.warning(f"Could not parse price from text: {price_text}")
            return None
    
    @abstractmethod
    def get_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Get list of listing URLs to scrape"""
        pass
    
    @abstractmethod
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        pass

    def _scrape_and_save(self, url: str) -> bool:
        """Helper function to scrape and save a single listing."""
        self.logger.info(f"Scraping: {url}")
        try:
            listing_data = self.scrape_listing(url)
            if listing_data:
                return self.save_business(listing_data)
        except Exception as e:
            self.logger.error(f"Error scraping and saving {url}: {e}", exc_info=True)
        return False

    def run(self, max_listings: Optional[int] = None):
        """Run the scraper"""
        self.logger.info(f"Starting {self.name} scraper")
        
        try:
            # When testing with max_listings, only fetch 1 page.
            # Otherwise, fetch up to 10 pages.
            pages_to_scrape = 1 if max_listings else 10
            listing_urls = self.get_listing_urls(max_pages=pages_to_scrape)
            self.logger.info(f"Found {len(listing_urls)} listings for {self.name}")
            
            if max_listings:
                listing_urls = listing_urls[:max_listings]
            
            success_count = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {executor.submit(self._scrape_and_save, url): url for url in listing_urls}
                
                for future in concurrent.futures.as_completed(future_to_url):
                    try:
                        if future.result():
                            success_count += 1
                    except Exception as e:
                        url = future_to_url[future]
                        self.logger.error(f"An exception occurred for {url}: {e}", exc_info=True)

            self.logger.info(f"Completed {self.name}. Saved {success_count}/{len(listing_urls)} listings.")
        except Exception as e:
            self.logger.critical(f"A critical error occurred in {self.name} scraper: {e}", exc_info=True)
        finally:
            self.logger.info(f"Finished {self.name} scraper.")
