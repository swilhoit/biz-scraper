#!/usr/bin/env python3
"""
Run the enhanced detail scraper on MAXIMIZED_WORKING_CACHE.csv
"""

from enhanced_detail_scraper import EnhancedDetailScraper
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CacheDetailScraper(EnhancedDetailScraper):
    """Modified scraper to use MAXIMIZED_WORKING_CACHE.csv as input"""
    
    def load_listings(self, filename: str = 'MAXIMIZED_WORKING_CACHE.csv'):
        """Override to load from cache file"""
        return super().load_listings(filename)


def main():
    """Run the detail scraper on cached listings"""
    try:
        logger.info("Starting enhanced detail scraper on MAXIMIZED_WORKING_CACHE.csv...")
        
        # Create scraper instance that uses the cache file
        scraper = CacheDetailScraper()
        
        # Run the scraper (can limit for testing or run all)
        # For testing with 10 URLs:
        # scraper.scrape_all_details(max_workers=5, limit=10)
        
        # For all URLs:
        scraper.scrape_all_details(max_workers=10)
        
        # Export results with descriptive filename
        scraper.export_to_csv('cache_enhanced_business_details.csv')
        scraper.export_to_json('cache_enhanced_business_details.json')
        scraper.print_summary()
        
        logger.info("Detail scraping on cache completed successfully!")
        
    except Exception as e:
        logger.error(f"Detail scraper failed: {e}")
        raise


if __name__ == "__main__":
    main()