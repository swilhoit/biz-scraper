#!/usr/bin/env python3
"""
Test the detail scraper on a small subset of MAXIMIZED_WORKING_CACHE.csv
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
    """Test with just 5 listings"""
    try:
        logger.info("Testing enhanced detail scraper on 5 listings from cache...")
        
        scraper = CacheDetailScraper()
        
        # Test with just 5 URLs
        scraper.scrape_all_details(max_workers=3, limit=5)
        
        # Export test results
        scraper.export_to_csv('test_cache_details.csv')
        scraper.export_to_json('test_cache_details.json')
        scraper.print_summary()
        
        logger.info("Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


if __name__ == "__main__":
    main()