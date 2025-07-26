#!/usr/bin/env python3
"""
Filter out non-listing URLs from the cache file
"""

import pandas as pd
import json
import os

def is_valid_listing_url(url):
    """Check if URL is likely to be an actual business listing"""
    if not url or not isinstance(url, str):
        return False
    
    # Generic pages to skip
    skip_patterns = [
        '/how-to-',
        '/sell/',
        '/buy/',
        '/brokers/',
        '/mybbs/',
        '/valuation',
        '/franchise-for-sale/',
        '/fsbo/listings/add/',
        'Dashboard',
        'Summary.aspx',
        'mylistings.aspx',
        'MyBusiness',
        'SavedListings',
        '/buy/?',
        'utm_source=',
        'ten-x.com'
    ]
    
    # Check if URL contains any skip patterns
    for pattern in skip_patterns:
        if pattern in url:
            return False
    
    # Valid listing patterns
    valid_patterns = [
        '/listings/',  # QuietLight
        '/business-opportunity/',  # BizBuySell
        '/websites/',  # WebsiteProperties
        '/business/',  # Investors.Club
        '/startup/',  # Acquire
    ]
    
    # Check if URL contains any valid patterns
    for pattern in valid_patterns:
        if pattern in url:
            return True
    
    return False

def filter_listings():
    """Filter valid listings and create a clean file"""
    
    # Load the cache file
    df = pd.read_csv('MAXIMIZED_WORKING_CACHE.csv')
    print(f"Original listings: {len(df)}")
    
    # Filter valid listings
    df['is_valid'] = df['url'].apply(is_valid_listing_url)
    valid_df = df[df['is_valid']].drop('is_valid', axis=1)
    
    print(f"Valid listings: {len(valid_df)}")
    
    # Show breakdown by source
    print("\nValid listings by source:")
    source_counts = valid_df['source'].value_counts()
    for source, count in source_counts.items():
        print(f"  {source}: {count}")
    
    # Save filtered file
    output_file = 'FILTERED_LISTINGS_CACHE.csv'
    valid_df.to_csv(output_file, index=False)
    print(f"\nFiltered listings saved to: {output_file}")
    
    # Check what URLs have been processed
    if os.path.exists('scraper_checkpoint.json'):
        with open('scraper_checkpoint.json', 'r') as f:
            checkpoint = json.load(f)
            processed = set(checkpoint['processed_urls'])
        
        # Filter out already processed URLs
        remaining_df = valid_df[~valid_df['url'].isin(processed)]
        print(f"\nRemaining unprocessed valid listings: {len(remaining_df)}")
        
        # Save remaining listings
        remaining_file = 'REMAINING_LISTINGS.csv'
        remaining_df.to_csv(remaining_file, index=False)
        print(f"Remaining listings saved to: {remaining_file}")

if __name__ == "__main__":
    filter_listings()