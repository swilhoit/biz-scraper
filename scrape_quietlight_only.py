#!/usr/bin/env python3
"""
Scrape only QuietLight and other working sources
"""

from incremental_detail_scraper import IncrementalDetailScraper
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run scraper on non-BizBuySell listings"""
    
    # Load filtered listings
    df = pd.read_csv('FILTERED_LISTINGS_CACHE.csv')
    
    # Filter out BizBuySell for now (having issues)
    non_bbs_df = df[df['source'] != 'BizBuySell']
    
    # Save to temporary file
    temp_file = 'NON_BBS_LISTINGS.csv'
    non_bbs_df.to_csv(temp_file, index=False)
    
    logger.info(f"Processing {len(non_bbs_df)} non-BizBuySell listings")
    logger.info(f"Sources: {non_bbs_df['source'].value_counts().to_dict()}")
    
    # Run scraper
    scraper = IncrementalDetailScraper()
    scraper.scrape_with_incremental_save(
        input_file=temp_file,
        batch_size=10,
        max_workers=5
    )
    
    # Print summary
    stats = scraper.get_summary_stats()
    logger.info("\n=== SUMMARY ===")
    logger.info(f"Total scraped: {stats['total_listings']}")
    
if __name__ == "__main__":
    main()