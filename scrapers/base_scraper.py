import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Optional
from datetime import datetime
from bigquery import get_bigquery_handler
from utils.amazon_detector import AmazonFBADetector
import concurrent.futures
import uuid
import time

class BaseScraper(ABC):
    def __init__(self, site_config: Dict, max_workers: int = 5):
        self.site_config = site_config
        self.name = site_config['name']
        self.base_url = site_config['base_url']
        self.logger = logging.getLogger(self.name)
        self.max_workers = max_workers
        self.bq_handler = get_bigquery_handler()

        # Configure requests session with retries
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        
        # Handle different URL configurations, prioritizing specific e-commerce/Amazon URLs
        self.search_urls = []
        if 'ecommerce_url' in site_config:
            self.search_urls.append(site_config['ecommerce_url'])
        if 'amazon_url' in site_config:
            self.search_urls.append(site_config['amazon_url'])

        # If no specific URLs are found, fall back to the general search_urls or search_url
        if not self.search_urls:
            if 'search_urls' in site_config:
                self.search_urls = site_config['search_urls']
            elif 'search_url' in site_config:
                self.search_urls = [site_config['search_url']]
        
    def get_page(self, url: str, render: bool = False) -> Optional[BeautifulSoup]:
        """Fetch a page using ScraperAPI"""
        from config import SCRAPER_API_URL, SCRAPER_API_PARAMS
        params = SCRAPER_API_PARAMS.copy()
        params['url'] = url
        if render:
            params['render'] = 'true'
        
        try:
            response = self.session.get(SCRAPER_API_URL, params=params, timeout=120)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'lxml')
        except Exception as e:
            self.logger.warning(f"Error fetching {url}: {e}")
            return None
    
    def save_to_bigquery(self, all_data: List[Dict]):
        """Saves a list of dictionaries to BigQuery."""
        if not all_data:
            self.logger.info("No data to save.")
            return
        
        self.logger.info(f"Preparing to save {len(all_data)} rows to BigQuery.")
        self.bq_handler.insert_rows(self.name, all_data)

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
            return float(price_text.strip()) * multiplier
        except (ValueError, TypeError):
            self.logger.warning(f"Could not parse price from text: {price_text}")
            return None
    
    def get_listing_urls(self, search_url: str, max_pages: Optional[int] = None) -> List[str]:
        """Get list of listing URLs to scrape from a specific search URL."""
        raise NotImplementedError("Each scraper must implement its own get_listing_urls method.")
    
    def _get_all_listing_urls(self, max_pages: Optional[int] = None) -> List[str]:
        """Iterate over all search URLs and collect listing URLs from each."""
        all_listing_urls = []
        for url in self.search_urls:
            self.logger.info(f"Getting listings from search URL: {url}")
            try:
                # The scraper-specific get_listing_urls will handle pagination for this single URL
                urls = self.get_listing_urls(url, max_pages=max_pages)
                all_listing_urls.extend(urls)
                self.logger.info(f"Found {len(urls)} listings at {url}.")
            except Exception as e:
                self.logger.error(f"Error getting listings from {url}: {e}", exc_info=True)
        
        # Return a list of unique URLs
        return list(dict.fromkeys(all_listing_urls))

    @abstractmethod
    def scrape_listing(self, url: str) -> Optional[Dict]:
        """Scrape a single listing"""
        pass

    def _scrape_and_save(self, url: str):
        """Scrapes a single listing and saves it to BigQuery immediately."""
        self.logger.info(f"Scraping: {url}")
        try:
            listing_data = self.scrape_listing(url)
            if listing_data:
                # Add unique ID, scraped_at timestamp, and perform any enhancements
                listing_data['id'] = abs(hash(listing_data.get('listing_url', url)))
                listing_data['scraped_at'] = datetime.utcnow().isoformat()
                listing_data = AmazonFBADetector.enhance_listing(listing_data)
                
                # Save the single record
                self.save_to_bigquery([listing_data])
                return True # Indicate success
        except Exception as e:
            self.logger.error(f"Error scraping and saving {url}: {e}", exc_info=True)
        return False # Indicate failure

    def run(self, max_listings: Optional[int] = None):
        """Run the scraper"""
        # Initialize run tracking
        run_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        error_count = 0
        status = "running"
        error_message = None
        
        # Log the start of the run
        initial_log = {
            "run_id": run_id,
            "site_name": self.name,
            "start_time": start_time.isoformat(),
            "status": status,
            "total_listings_found": 0,
            "existing_listings": 0,
            "new_listings": 0,
            "successful_scrapes": 0,
            "failed_scrapes": 0,
            "error_count": 0,
            "api_calls_made": 0,
            "api_credits_saved": 0
        }
        self.bq_handler.log_scraping_run(initial_log)
        
        self.logger.info(f"Starting {self.name} scraper with run_id: {run_id}")
        
        try:
            pages_to_scrape = 1 if max_listings else None
            
            # Count API calls for getting listing URLs (search pages)
            search_page_count = len(self.search_urls) * (pages_to_scrape if pages_to_scrape else 10)  # estimate
            api_calls_made = search_page_count
            
            listing_urls = self._get_all_listing_urls(max_pages=pages_to_scrape)
            self.logger.info(f"Found {len(listing_urls)} total listings for {self.name} across all search URLs.")
            
            if max_listings:
                listing_urls = listing_urls[:max_listings]
            
            # Check for existing URLs in BigQuery to avoid duplicate API calls
            self.logger.info(f"Checking for existing URLs in BigQuery...")
            existing_urls = self.bq_handler.get_existing_urls(self.name, listing_urls)
            
            # Filter out existing URLs
            new_urls = [url for url in listing_urls if url not in existing_urls]
            api_credits_saved = len(existing_urls)  # Each existing URL is an API call saved
            
            if existing_urls:
                self.logger.info(f"Skipping {len(existing_urls)} already scraped URLs to conserve API credits.")
            
            self.logger.info(f"Will scrape {len(new_urls)} new listings out of {len(listing_urls)} total found.")
            
            # Update log with found listings info
            self.bq_handler.update_scraping_log(run_id, {
                "total_listings_found": len(listing_urls),
                "existing_listings": len(existing_urls),
                "new_listings": len(new_urls),
                "api_credits_saved": api_credits_saved
            })
            
            if not new_urls:
                self.logger.info("No new listings to scrape. All URLs already exist in the database.")
                status = "completed"
                
            else:
                successful_scrapes = 0
                failed_scrapes = 0
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_url = {executor.submit(self._scrape_and_save, url): url for url in new_urls}
                    
                    for future in concurrent.futures.as_completed(future_to_url):
                        try:
                            if future.result():
                                successful_scrapes += 1
                            else:
                                failed_scrapes += 1
                        except Exception as e:
                            url = future_to_url[future]
                            self.logger.error(f"An exception occurred for {url}: {e}", exc_info=True)
                            failed_scrapes += 1
                            error_count += 1
                
                # Add API calls for individual listings
                api_calls_made += len(new_urls)
                
                self.logger.info(f"Successfully scraped and saved {successful_scrapes}/{len(new_urls)} new listings.")
                self.logger.info(f"Total in database: {len(existing_urls) + successful_scrapes} listings for {self.name}.")
                
                status = "completed"

        except Exception as e:
            self.logger.critical(f"A critical error occurred in {self.name} scraper: {e}", exc_info=True)
            status = "failed"
            error_message = str(e)
            error_count += 1
            
        finally:
            end_time = datetime.utcnow()
            duration_seconds = (end_time - start_time).total_seconds()
            
            # Final update to the log
            final_updates = {
                "end_time": end_time.isoformat(),
                "duration_seconds": duration_seconds,
                "status": status,
                "error_count": error_count,
                "api_calls_made": api_calls_made
            }
            
            if 'successful_scrapes' in locals():
                final_updates["successful_scrapes"] = successful_scrapes
                final_updates["failed_scrapes"] = failed_scrapes
            
            if error_message:
                final_updates["error_message"] = error_message
            
            self.bq_handler.update_scraping_log(run_id, final_updates)
            
            self.logger.info(f"Finished {self.name} scraper. Run ID: {run_id}, Duration: {duration_seconds:.2f}s")
