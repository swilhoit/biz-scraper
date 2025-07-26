#!/usr/bin/env python3
"""Export BizQuest listings with revenue data from detail pages"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_revenue_from_detail(url, api_key):
    """Extract just the revenue from a detail page"""
    params = {
        'api_key': api_key,
        'url': url,
        'render': 'true'
    }
    
    try:
        response = requests.get("http://api.scraperapi.com", params=params, timeout=60)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for Gross Revenue in financial details
        financial_containers = soup.select('app-financial-details .data-container')
        
        for container in financial_containers:
            label_elem = container.select_one('.text-info')
            value_elem = container.select_one('.price, b:not(.text-info)')
            
            if label_elem and value_elem:
                label = label_elem.get_text().strip().lower()
                if 'gross revenue' in label:
                    value = value_elem.get_text().strip()
                    # Clean and return the value
                    if '$' in value:
                        return value
                    elif re.search(r'[\d,]+', value):
                        return '$' + re.search(r'[\d,]+', value).group()
        
        return ''
        
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return ''

def main():
    api_key = os.getenv('SCRAPER_API_KEY')
    if not api_key:
        print("Error: SCRAPER_API_KEY not found")
        return
    
    # Read existing BizQuest results
    df = pd.read_csv('bizquest_results.csv')
    print(f"Found {len(df)} BizQuest listings")
    
    # Process first 20 listings to get revenue data
    listings_to_process = df.head(20).copy()
    
    print("\nFetching revenue data from detail pages...")
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_idx = {}
        
        for idx, row in listings_to_process.iterrows():
            future = executor.submit(extract_revenue_from_detail, row['url'], api_key)
            future_to_idx[future] = idx
        
        completed = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                revenue = future.result()
                if revenue:
                    listings_to_process.at[idx, 'revenue'] = revenue
                    logger.info(f"Got revenue {revenue} for {listings_to_process.at[idx, 'name'][:40]}...")
                completed += 1
                print(f"Progress: {completed}/{len(listings_to_process)}", end='\r')
            except Exception as e:
                logger.error(f"Error: {e}")
    
    print("\n\nExporting results...")
    
    # Combine processed listings with the rest
    df.update(listings_to_process)
    
    # Export to new CSV
    df.to_csv('bizquest_with_revenue.csv', index=False)
    
    # Show summary
    revenue_count = df['revenue'].astype(str).str.len().gt(0).sum()
    print(f"\nExported {len(df)} listings to bizquest_with_revenue.csv")
    print(f"Listings with revenue data: {revenue_count}")
    
    # Show samples
    revenue_df = df[df['revenue'].astype(str).str.len() > 0]
    if len(revenue_df) > 0:
        print("\nSample listings with revenue:")
        for _, row in revenue_df.head(5).iterrows():
            print(f"\n{row['name']}")
            print(f"  Price: {row['price']} | Revenue: {row['revenue']} | Cash Flow: {row['profit']}")

if __name__ == "__main__":
    main()