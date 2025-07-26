#!/usr/bin/env python3
"""
Test the enhanced detail scraper with a small batch
"""

from enhanced_detail_scraper import EnhancedDetailScraper
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_enhanced_scraper():
    """Test the enhanced scraper with 5 URLs"""
    try:
        logger.info("Starting enhanced detail scraper test with 5 URLs...")
        scraper = EnhancedDetailScraper()
        
        # Test with just 5 URLs to demonstrate comprehensive extraction
        scraper.scrape_all_details(max_workers=3, limit=5)
        
        # Export results
        scraper.export_to_csv('test_enhanced_details.csv')
        scraper.export_to_json('test_enhanced_details.json')
        scraper.print_summary()
        
        logger.info("Enhanced test completed successfully!")
        
    except Exception as e:
        logger.error(f"Enhanced test failed: {e}")
        raise


if __name__ == "__main__":
    test_enhanced_scraper()