#!/usr/bin/env python3
"""
Combine all scraped results into a final dataset
"""

import pandas as pd
import os
import json

def combine_results():
    """Combine incremental results with any other scraped data"""
    
    all_data = []
    
    # Load incremental results
    if os.path.exists('incremental_results.csv'):
        df1 = pd.read_csv('incremental_results.csv')
        print(f"Loaded {len(df1)} records from incremental_results.csv")
        all_data.append(df1)
    
    # Check for other result files
    result_files = [
        'enhanced_business_details.csv',
        'test_cache_details.csv',
        'cache_enhanced_business_details.csv'
    ]
    
    for file in result_files:
        if os.path.exists(file):
            df = pd.read_csv(file)
            print(f"Loaded {len(df)} records from {file}")
            all_data.append(df)
    
    if all_data:
        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates based on URL
        combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
        
        print(f"\nTotal unique records: {len(combined_df)}")
        
        # Save combined results
        combined_df.to_csv('FINAL_SCRAPED_DETAILS.csv', index=False)
        print("Saved to FINAL_SCRAPED_DETAILS.csv")
        
        # Analysis
        print("\n=== DATA QUALITY SUMMARY ===")
        
        # Check key fields
        key_fields = ['asking_price', 'annual_revenue', 'annual_profit', 'business_type', 'industry']
        for field in key_fields:
            if field in combined_df.columns:
                non_empty = combined_df[field].notna() & (combined_df[field].astype(str).str.strip() != '')
                count = non_empty.sum()
                pct = (count / len(combined_df)) * 100
                print(f"{field}: {count}/{len(combined_df)} ({pct:.1f}%)")
        
        # Source breakdown
        if 'source' in combined_df.columns:
            print("\n=== LISTINGS BY SOURCE ===")
            source_counts = combined_df['source'].value_counts()
            for source, count in source_counts.items():
                print(f"{source}: {count}")
        
        # Sample of successful extractions
        print("\n=== SAMPLE SUCCESSFUL EXTRACTIONS ===")
        # Find records with the most populated fields
        field_counts = combined_df.notna().sum(axis=1)
        best_records = combined_df.loc[field_counts.nlargest(5).index]
        
        for idx, row in best_records.iterrows():
            print(f"\n{row.get('name', 'Unknown')[:60]}...")
            print(f"  Source: {row.get('source', 'N/A')}")
            print(f"  Price: {row.get('asking_price', 'N/A')}")
            print(f"  Revenue: {row.get('annual_revenue', 'N/A')}")
            print(f"  Industry: {row.get('industry', 'N/A')}")
            print(f"  URL: {row.get('url', 'N/A')[:60]}...")
    else:
        print("No result files found!")

if __name__ == "__main__":
    combine_results()