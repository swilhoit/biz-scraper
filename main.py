import logging
import argparse
from datetime import datetime
import sys
import concurrent.futures

# Import BigQuery handler and scrapers
from bigquery import get_bigquery_handler
from scrapers import (
    BizBuySellScraper, 
    BizQuestScraper, 
    FlippaScraper,
    QuietLightScraper,
    WebsitePropertiesScraper,
    EmpireFlippersScraper,
    # AcquireScraper,  # Disabled - requires playwright
    # FEInternationalScraper,  # Disabled - requires playwright
    WebsiteClosersScraper
)
from config import SITES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Map site names to scraper classes
SCRAPER_CLASSES = {
    'BizBuySell': BizBuySellScraper,
    'BizQuest': BizQuestScraper,
    'Flippa': FlippaScraper,
    'QuietLight': QuietLightScraper,
    'WebsiteProperties': WebsitePropertiesScraper,
    'EmpireFlippers': EmpireFlippersScraper,
    # 'Acquire': AcquireScraper,  # Disabled - requires playwright
    # 'FEInternational': FEInternationalScraper,  # Disabled - requires playwright
    'WebsiteClosers': WebsiteClosersScraper
}

def run_scraper(site):
    """Initializes and runs a scraper for a given site."""
    site_name = site['name']
    
    if not site.get('enabled', False):
        logging.warning(f"Skipping {site_name} as it is disabled in the config.")
        return
        
    if site_name not in SCRAPER_CLASSES:
        logging.warning(f"No scraper implemented for {site_name}")
        return
    
    logging.info(f"Processing {site_name} for historical data.")
    
    try:
        scraper_class = SCRAPER_CLASSES[site_name]
        scraper = scraper_class(site)
        # The 'run' method in BaseScraper now handles the full historical scrape.
        scraper.run() 
    except Exception as e:
        logging.error(f"Error running {site_name} scraper: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description='Scrapes business listings and stores them in Google BigQuery.')
    parser.add_argument(
        '--sites', 
        nargs='+', 
        help='Optional: Specific sites to scrape (e.g., BizBuySell QuietLight). If not provided, all enabled sites will be scraped.'
    )
    parser.add_argument(
        '--max-workers', 
        type=int, 
        default=4,
        help='Maximum number of concurrent site scrapers to run.'
    )
    
    args = parser.parse_args()

    # Initialize BigQuery Handler - this will also create the dataset if needed.
    # Make sure you have authenticated via `gcloud auth application-default login`
    # and have set GCP_PROJECT_ID and BQ_DATASET_NAME in your environment.
    try:
        get_bigquery_handler()
        logging.info("BigQuery handler initialized successfully.")
    except Exception as e:
        logging.critical(f"Failed to initialize BigQuery handler: {e}", exc_info=True)
        logging.critical("Please ensure you have authenticated with GCP and set the required environment variables.")
        return

    # Filter sites if specified
    sites_to_scrape = [site for site in SITES if site['enabled']]
    if args.sites:
        sites_to_scrape = [site for site in sites_to_scrape if site['name'] in args.sites]
    
    logging.info(f"Preparing to scrape the following sites: {[s['name'] for s in sites_to_scrape]}")

    # Run scrapers in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [executor.submit(run_scraper, site) for site in sites_to_scrape]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"A scraper thread generated an exception: {e}", exc_info=True)

if __name__ == '__main__':
    main()
