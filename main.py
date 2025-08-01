import logging
import argparse
from datetime import datetime
import sys

from database import init_database
from scrapers import (
    BizBuySellScraper, 
    BizQuestScraper, 
    FlippaScraper,
    QuietLightScraper,
    WebsitePropertiesScraper,
    EmpireFlippersScraper,
    AcquireScraper,
    FEInternationalScraper,
    WebsiteClosersScraper
)
from config import SITES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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
    'Acquire': AcquireScraper,
    'FEInternational': FEInternationalScraper,
    'WebsiteClosers': WebsiteClosersScraper
}

def main():
    parser = argparse.ArgumentParser(description='Business listing scraper using ScraperAPI')
    parser.add_argument(
        '--sites', 
        nargs='+', 
        help='Specific sites to scrape (e.g., BizBuySell)'
    )
    parser.add_argument(
        '--max-listings', 
        type=int, 
        help='Maximum number of listings to scrape per site'
    )
    
    args = parser.parse_args()
    
    # Initialize database
    init_database()
    
    # Filter sites if specified
    sites_to_scrape = SITES
    if args.sites:
        sites_to_scrape = [site for site in SITES if site['name'] in args.sites]
    
    # Run scrapers
    for site in sites_to_scrape:
        site_name = site['name']
        
        if site_name not in SCRAPER_CLASSES:
            logging.warning(f"No scraper implemented for {site_name}")
            continue
        
        logging.info(f"Processing {site_name}")
        
        try:
            scraper_class = SCRAPER_CLASSES[site_name]
            scraper = scraper_class(site)
            scraper.run(max_listings=args.max_listings)
        except Exception as e:
            logging.error(f"Error running {site_name} scraper: {e}", exc_info=True)

if __name__ == '__main__':
    main()