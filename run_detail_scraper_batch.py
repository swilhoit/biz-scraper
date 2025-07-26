#!/usr/bin/env python3
"""
Run the detail scraper in batches with incremental saving
"""

from detail_scraper import DetailScraper
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_detail_scraper_batch():
    """Run the detail scraper with a smaller batch size"""
    try:
        logger.info("Starting detail scraper with limited batch...")
        scraper = DetailScraper()
        
        # Run with just 20 URLs and fewer workers to avoid timeout
        scraper.scrape_all_details(max_workers=5, limit=20)
        
        # Export results
        scraper.export_to_csv('business_details_batch.csv')
        scraper.export_to_json('business_details_batch.json')
        scraper.print_summary()
        
        logger.info("Batch scraping completed successfully!")
        
    except Exception as e:
        logger.error(f"Batch scraping failed: {e}")
        raise


if __name__ == "__main__":
    run_detail_scraper_batch()