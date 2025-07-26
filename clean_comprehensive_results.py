#!/usr/bin/env python3

import pandas as pd
import re
from urllib.parse import urlparse

def clean_financial_value(value_str):
    """Clean and convert financial values to numbers"""
    if pd.isna(value_str) or value_str == '' or value_str == '0':
        return None
    
    # Remove currency symbols, commas, and extra text
    cleaned = re.sub(r'[^\d\.]', '', str(value_str))
    
    try:
        return float(cleaned) if cleaned else None
    except:
        return None

def deduplicate_businesses(df):
    """Remove duplicate businesses based on URL and name similarity"""
    print(f"Starting with {len(df)} listings")
    
    # Remove obvious duplicates by URL
    df = df.drop_duplicates(subset=['url'], keep='first')
    print(f"After URL deduplication: {len(df)} listings")
    
    # Remove "Unlock Listing" entries
    unlock_mask = df['name'].str.contains('Unlock Listing', case=False, na=False)
    df = df[~unlock_mask]
    print(f"After removing 'Unlock Listing': {len(df)} listings")
    
    # Remove entries with broken BizBuySell data (placeholder values)
    broken_bizbuysell = (
        (df['source'] == 'BizBuySell') & 
        (df['price'].str.contains('$250,000', na=False)) &
        (df['revenue'].str.contains('$2,022', na=False))
    )
    
    # Keep BizBuySell entries but mark for data fixing
    print(f"Found {broken_bizbuysell.sum()} BizBuySell entries with broken financial data")
    
    # Remove duplicate names (case-insensitive, first 50 chars)
    df['name_normalized'] = df['name'].str.lower().str[:50]
    df = df.drop_duplicates(subset=['name_normalized'], keep='first')
    df = df.drop(columns=['name_normalized'])
    print(f"After name deduplication: {len(df)} listings")
    
    return df

def analyze_sources(df):
    """Analyze the quality and coverage by source"""
    print("\n=== SOURCE ANALYSIS ===")
    
    for source in df['source'].unique():
        source_df = df[df['source'] == source]
        
        # Financial data coverage
        price_coverage = source_df['price'].notna().sum()
        revenue_coverage = source_df['revenue'].notna().sum()
        profit_coverage = source_df['profit'].notna().sum()
        
        print(f"\n{source}: {len(source_df)} listings")
        print(f"  Price coverage: {price_coverage}/{len(source_df)} ({price_coverage/len(source_df)*100:.1f}%)")
        print(f"  Revenue coverage: {revenue_coverage}/{len(source_df)} ({revenue_coverage/len(source_df)*100:.1f}%)")
        print(f"  Profit coverage: {profit_coverage}/{len(source_df)} ({profit_coverage/len(source_df)*100:.1f}%)")

def main():
    # Load the comprehensive results
    df = pd.read_csv('COMPREHENSIVE_BUSINESS_LISTINGS.csv')
    
    print("=== CLEANING COMPREHENSIVE BUSINESS LISTINGS ===")
    
    # Deduplicate
    df_clean = deduplicate_businesses(df)
    
    # Analyze by source
    analyze_sources(df_clean)
    
    # Clean financial values
    df_clean['price_numeric'] = df_clean['price'].apply(clean_financial_value)
    df_clean['revenue_numeric'] = df_clean['revenue'].apply(clean_financial_value)
    df_clean['profit_numeric'] = df_clean['profit'].apply(clean_financial_value)
    
    # Calculate multiples where possible
    df_clean['revenue_multiple'] = df_clean['price_numeric'] / df_clean['revenue_numeric']
    df_clean['profit_multiple'] = df_clean['price_numeric'] / df_clean['profit_numeric']
    
    # Final summary
    print(f"\n=== FINAL CLEANED RESULTS ===")
    print(f"Total unique businesses: {len(df_clean):,}")
    
    # High-value businesses (>$1M)
    high_value = df_clean[df_clean['price_numeric'] >= 1000000]
    print(f"High-value businesses (>$1M): {len(high_value):,}")
    
    # Medium businesses ($100K-$1M)
    medium_value = df_clean[
        (df_clean['price_numeric'] >= 100000) & 
        (df_clean['price_numeric'] < 1000000)
    ]
    print(f"Medium businesses ($100K-$1M): {len(medium_value):,}")
    
    # Top 10 opportunities
    print(f"\n🏆 TOP 10 OPPORTUNITIES:")
    top_10 = df_clean.nlargest(10, 'price_numeric')
    for i, (_, row) in enumerate(top_10.iterrows(), 1):
        price = f"${row['price_numeric']:,.0f}" if pd.notna(row['price_numeric']) else "N/A"
        revenue = f"${row['revenue_numeric']:,.0f}" if pd.notna(row['revenue_numeric']) else "N/A"
        name_short = str(row['name'])[:60] + "..." if len(str(row['name'])) > 60 else str(row['name'])
        print(f"{i:2d}. {price} | Rev: {revenue} | {name_short}")
    
    # Save cleaned results
    output_file = 'FINAL_COMPREHENSIVE_BUSINESS_LISTINGS.csv'
    df_clean.to_csv(output_file, index=False)
    print(f"\n✅ Cleaned results saved to: {output_file}")
    
    return df_clean

if __name__ == "__main__":
    df = main() 