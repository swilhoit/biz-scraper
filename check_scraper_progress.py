#!/usr/bin/env python3
"""
Check the progress of the incremental detail scraper
"""

import json
import pandas as pd
import os

def check_progress():
    """Display current scraping progress"""
    
    # Check checkpoint file
    if os.path.exists('scraper_checkpoint.json'):
        with open('scraper_checkpoint.json', 'r') as f:
            checkpoint = json.load(f)
            processed_count = len(checkpoint['processed_urls'])
            print(f"✓ Processed URLs: {processed_count}")
            print(f"✓ Last update: {checkpoint['timestamp']}")
    else:
        print("No checkpoint file found")
        return
    
    # Check original file
    if os.path.exists('MAXIMIZED_WORKING_CACHE.csv'):
        df = pd.read_csv('MAXIMIZED_WORKING_CACHE.csv')
        total_urls = len(df)
        remaining = total_urls - processed_count
        progress_pct = (processed_count / total_urls) * 100
        
        print(f"\n📊 Progress Summary:")
        print(f"   Total URLs: {total_urls}")
        print(f"   Processed: {processed_count} ({progress_pct:.1f}%)")
        print(f"   Remaining: {remaining}")
    
    # Check results file
    if os.path.exists('incremental_results.csv'):
        results_df = pd.read_csv('incremental_results.csv')
        print(f"\n📝 Results Summary:")
        print(f"   Successfully scraped: {len(results_df)} listings")
        
        # Check data quality
        key_fields = ['asking_price', 'annual_revenue', 'annual_profit', 'business_type', 'industry']
        print(f"\n📈 Data Quality:")
        for field in key_fields:
            if field in results_df.columns:
                non_empty = results_df[field].notna() & (results_df[field].astype(str).str.strip() != '')
                count = non_empty.sum()
                pct = (count / len(results_df)) * 100 if len(results_df) > 0 else 0
                print(f"   {field}: {count}/{len(results_df)} ({pct:.1f}%)")
        
        # Show sources breakdown
        if 'source' in results_df.columns:
            print(f"\n🏢 Listings by Source:")
            source_counts = results_df['source'].value_counts()
            for source, count in source_counts.items():
                print(f"   {source}: {count}")
    
    print("\n💡 To resume scraping, run: python incremental_detail_scraper.py")

if __name__ == "__main__":
    check_progress()