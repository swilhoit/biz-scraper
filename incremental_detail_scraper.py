#!/usr/bin/env python3
"""
Incremental detail scraper that saves progress and can handle large datasets
"""

from enhanced_detail_scraper import EnhancedDetailScraper
import logging
import pandas as pd
import json
import os
from typing import Dict, List
import time
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IncrementalDetailScraper(EnhancedDetailScraper):
    """Enhanced scraper with incremental saving and resume capability"""
    
    def __init__(self, checkpoint_file: str = 'scraper_checkpoint.json'):
        super().__init__()
        self.checkpoint_file = checkpoint_file
        self.processed_urls = set()
        self.load_checkpoint()
        
    def load_checkpoint(self):
        """Load previously processed URLs from checkpoint"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.processed_urls = set(checkpoint.get('processed_urls', []))
                    logger.info(f"Loaded checkpoint: {len(self.processed_urls)} URLs already processed")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
                
    def save_checkpoint(self):
        """Save current progress to checkpoint file"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump({
                    'processed_urls': list(self.processed_urls),
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                }, f)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            
    def save_incremental_results(self, output_prefix: str = 'incremental_results'):
        """Save current results to files"""
        if self.detailed_data:
            # Save as CSV
            csv_file = f"{output_prefix}.csv"
            df = pd.DataFrame(self.detailed_data)
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved {len(df)} records to {csv_file}")
            
            # Save as JSON
            json_file = f"{output_prefix}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.detailed_data, f, indent=2, ensure_ascii=False)
                
    def scrape_with_incremental_save(self, input_file: str = 'FILTERED_LISTINGS_CACHE.csv', 
                                   batch_size: int = 10, max_workers: int = 5):
        """Scrape listings with incremental saving and progress tracking"""
        try:
            # Load listings
            listings = self.load_listings(input_file)
            
            # Filter out already processed URLs
            remaining_listings = listings[
                listings['url'].notna() & 
                (listings['url'] != '') &
                ~listings['url'].isin(self.processed_urls)
            ]
            logger.info(f"Total listings: {len(listings)}, Remaining to process: {len(remaining_listings)}")
            
            if remaining_listings.empty:
                logger.info("All listings have been processed!")
                return
            
            # Process in batches
            total_rows = len(remaining_listings)
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, total_rows)
                batch = remaining_listings.iloc[start_idx:end_idx]
                
                logger.info(f"\nProcessing batch {batch_num + 1}/{total_batches} ({len(batch)} listings)")
                
                # Process batch
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for idx, row in batch.iterrows():
                        url = row['url']
                        if url and url not in self.processed_urls:
                            future = executor.submit(self.scrape_detail_page, row)
                            futures.append((future, url))
                    
                    # Collect results
                    for future, url in futures:
                        try:
                            details = future.result(timeout=120)  # 2 minute timeout per URL
                            if details:
                                with self.lock:
                                    self.detailed_data.append(details)
                                    self.processed_urls.add(url)
                                logger.info(f"✓ Processed: {details.get('name', 'Unknown')[:50]}...")
                        except Exception as e:
                            logger.error(f"✗ Failed to process {url}: {e}")
                
                # Save progress after each batch
                self.save_checkpoint()
                self.save_incremental_results()
                logger.info(f"Batch {batch_num + 1} complete. Total processed: {len(self.detailed_data)}")
                
                # Small delay between batches to avoid rate limiting
                if batch_num < total_batches - 1:
                    time.sleep(2)
                    
        except KeyboardInterrupt:
            logger.info("\nScraping interrupted by user. Saving progress...")
            self.save_checkpoint()
            self.save_incremental_results()
            logger.info("Progress saved. Run again to resume.")
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            self.save_checkpoint()
            self.save_incremental_results()
            raise
            
    def get_summary_stats(self):
        """Get summary statistics of scraped data"""
        if not self.detailed_data:
            return "No data scraped yet"
            
        df = pd.DataFrame(self.detailed_data)
        
        stats = {
            'total_listings': len(df),
            'sources': df['source'].value_counts().to_dict() if 'source' in df else {},
            'with_price': df['asking_price'].notna().sum() if 'asking_price' in df else 0,
            'with_revenue': df['annual_revenue'].notna().sum() if 'annual_revenue' in df else 0,
            'with_profit': df['annual_profit'].notna().sum() if 'annual_profit' in df else 0,
        }
        
        return stats


def main():
    """Run the incremental scraper on MAXIMIZED_WORKING_CACHE.csv"""
    logger.info("Starting incremental detail scraper...")
    
    scraper = IncrementalDetailScraper()
    
    # Check if we're resuming
    if scraper.processed_urls:
        logger.info(f"Resuming from previous run. Already processed: {len(scraper.processed_urls)} URLs")
    
    # Run scraper with incremental saving
    scraper.scrape_with_incremental_save(
        input_file='FILTERED_LISTINGS_CACHE.csv',
        batch_size=10,  # Process 10 URLs at a time
        max_workers=5   # 5 concurrent requests
    )
    
    # Print final summary
    stats = scraper.get_summary_stats()
    logger.info("\n=== FINAL SUMMARY ===")
    logger.info(f"Total listings scraped: {stats['total_listings']}")
    logger.info(f"Listings with price: {stats['with_price']}")
    logger.info(f"Listings with revenue: {stats['with_revenue']}")
    logger.info(f"Listings with profit: {stats['with_profit']}")
    
    if stats['sources']:
        logger.info("\nListings by source:")
        for source, count in stats['sources'].items():
            logger.info(f"  {source}: {count}")


if __name__ == "__main__":
    main()