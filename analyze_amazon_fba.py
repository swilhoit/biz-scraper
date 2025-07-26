#!/usr/bin/env python3

import pandas as pd
import re

def identify_amazon_fba_businesses(df):
    """Identify Amazon FBA businesses from the dataset"""
    
    # Create a copy to work with
    df_copy = df.copy()
    
    # Initialize Amazon FBA identification
    df_copy['is_amazon_fba'] = False
    
    # Method 1: Check names for Amazon FBA keywords
    amazon_keywords = [
        'amazon fba', 'amazon fbm', 'amazon business', 'amazon store', 
        'amazon brand', 'amazon seller', 'amazon ecommerce', 'amazon listing',
        'fba business', 'fba brand', 'fba store', 'fba seller'
    ]
    
    name_pattern = '|'.join(amazon_keywords)
    name_matches = df_copy['name'].str.contains(name_pattern, case=False, na=False)
    
    # Method 2: Check descriptions for Amazon FBA keywords
    desc_pattern = '|'.join(amazon_keywords)
    desc_matches = df_copy['description'].str.contains(desc_pattern, case=False, na=False)
    
    # Method 3: Check URLs for Amazon-specific patterns
    url_matches = df_copy['url'].str.contains('amazon|fba', case=False, na=False)
    
    # Combine all methods
    df_copy['is_amazon_fba'] = name_matches | desc_matches | url_matches
    
    return df_copy

def analyze_amazon_fba_breakdown(df):
    """Comprehensive analysis of Amazon FBA businesses"""
    
    print("=" * 80)
    print("🛒 AMAZON FBA BUSINESS ANALYSIS")
    print("=" * 80)
    
    # Identify Amazon FBA businesses
    df_analyzed = identify_amazon_fba_businesses(df)
    
    # Count Amazon FBA businesses
    amazon_fba_count = df_analyzed['is_amazon_fba'].sum()
    total_count = len(df_analyzed)
    
    print(f"\n📊 AMAZON FBA SUMMARY:")
    print(f"Total Amazon FBA businesses: {amazon_fba_count:,}")
    print(f"Total businesses: {total_count:,}")
    print(f"Amazon FBA percentage: {amazon_fba_count/total_count*100:.1f}%")
    
    # Breakdown by source
    print(f"\n📈 AMAZON FBA BY MARKETPLACE:")
    amazon_fba_df = df_analyzed[df_analyzed['is_amazon_fba']]
    
    source_breakdown = amazon_fba_df['source'].value_counts()
    for source, count in source_breakdown.items():
        total_from_source = len(df_analyzed[df_analyzed['source'] == source])
        percentage = (count / total_from_source) * 100
        print(f"  {source}: {count:,} Amazon FBA businesses ({percentage:.1f}% of {source} total)")
    
    # Financial analysis of Amazon FBA businesses
    print(f"\n💰 AMAZON FBA FINANCIAL ANALYSIS:")
    
    # Price analysis
    valid_prices = amazon_fba_df[amazon_fba_df['price_numeric'].notna() & (amazon_fba_df['price_numeric'] > 0)]
    if len(valid_prices) > 0:
        print(f"  Price range: ${valid_prices['price_numeric'].min():,.0f} - ${valid_prices['price_numeric'].max():,.0f}")
        print(f"  Average price: ${valid_prices['price_numeric'].mean():,.0f}")
        print(f"  Median price: ${valid_prices['price_numeric'].median():,.0f}")
        print(f"  Total value: ${valid_prices['price_numeric'].sum():,.0f}")
    
    # Revenue analysis
    valid_revenue = amazon_fba_df[amazon_fba_df['revenue_numeric'].notna() & (amazon_fba_df['revenue_numeric'] > 0)]
    if len(valid_revenue) > 0:
        print(f"  Revenue range: ${valid_revenue['revenue_numeric'].min():,.0f} - ${valid_revenue['revenue_numeric'].max():,.0f}")
        print(f"  Average revenue: ${valid_revenue['revenue_numeric'].mean():,.0f}")
        print(f"  Median revenue: ${valid_revenue['revenue_numeric'].median():,.0f}")
        print(f"  Total revenue: ${valid_revenue['revenue_numeric'].sum():,.0f}")
    
    # Investment categories for Amazon FBA
    print(f"\n🎯 AMAZON FBA INVESTMENT CATEGORIES:")
    if len(valid_prices) > 0:
        high_value = valid_prices[valid_prices['price_numeric'] >= 1000000]
        medium_value = valid_prices[(valid_prices['price_numeric'] >= 100000) & (valid_prices['price_numeric'] < 1000000)]
        small_value = valid_prices[valid_prices['price_numeric'] < 100000]
        
        print(f"  High-value Amazon FBA (>$1M): {len(high_value):,} businesses")
        print(f"  Medium Amazon FBA ($100K-$1M): {len(medium_value):,} businesses")
        print(f"  Small Amazon FBA (<$100K): {len(small_value):,} businesses")
    
    # Top Amazon FBA opportunities
    print(f"\n🏆 TOP 10 AMAZON FBA OPPORTUNITIES:")
    top_amazon_fba = amazon_fba_df.nlargest(10, 'price_numeric')
    
    for i, (_, row) in enumerate(top_amazon_fba.iterrows(), 1):
        price = f"${row['price_numeric']:,.0f}" if pd.notna(row['price_numeric']) else "N/A"
        revenue = f"${row['revenue_numeric']:,.0f}" if pd.notna(row['revenue_numeric']) else "N/A"
        source = row['source']
        name_short = str(row['name'])[:55] + "..." if len(str(row['name'])) > 55 else str(row['name'])
        
        print(f"{i:2d}. {price} | Rev: {revenue} | {source}")
        print(f"     {name_short}")
        print()
    
    # Show some examples of Amazon FBA business names
    print(f"\n📝 SAMPLE AMAZON FBA BUSINESS NAMES:")
    sample_names = amazon_fba_df['name'].head(10)
    for i, name in enumerate(sample_names, 1):
        name_truncated = str(name)[:70] + "..." if len(str(name)) > 70 else str(name)
        print(f"{i:2d}. {name_truncated}")
    
    # Compare Amazon FBA vs non-FBA
    print(f"\n⚖️  AMAZON FBA VS NON-FBA COMPARISON:")
    non_fba_df = df_analyzed[~df_analyzed['is_amazon_fba']]
    
    # Average prices
    fba_avg_price = amazon_fba_df['price_numeric'].mean()
    non_fba_avg_price = non_fba_df['price_numeric'].mean()
    
    print(f"  Amazon FBA average price: ${fba_avg_price:,.0f}")
    print(f"  Non-FBA average price: ${non_fba_avg_price:,.0f}")
    
    if pd.notna(fba_avg_price) and pd.notna(non_fba_avg_price):
        if fba_avg_price > non_fba_avg_price:
            print(f"  🟢 Amazon FBA businesses are {fba_avg_price/non_fba_avg_price:.1f}x more expensive on average")
        else:
            print(f"  🔴 Non-FBA businesses are {non_fba_avg_price/fba_avg_price:.1f}x more expensive on average")
    
    # Data quality comparison
    fba_data_quality = amazon_fba_df['revenue_numeric'].notna().sum() / len(amazon_fba_df) * 100
    non_fba_data_quality = non_fba_df['revenue_numeric'].notna().sum() / len(non_fba_df) * 100
    
    print(f"  Amazon FBA revenue data coverage: {fba_data_quality:.1f}%")
    print(f"  Non-FBA revenue data coverage: {non_fba_data_quality:.1f}%")
    
    return df_analyzed, amazon_fba_df

def main():
    # Load the cleaned results
    df = pd.read_csv('FINAL_COMPREHENSIVE_BUSINESS_LISTINGS.csv')
    
    # Perform Amazon FBA analysis
    df_analyzed, amazon_fba_df = analyze_amazon_fba_breakdown(df)
    
    # Save Amazon FBA specific results
    amazon_fba_df.to_csv('AMAZON_FBA_BUSINESSES.csv', index=False)
    print(f"\n✅ Amazon FBA businesses saved to: AMAZON_FBA_BUSINESSES.csv")
    
    # Save the analyzed dataset with FBA flags
    df_analyzed.to_csv('FINAL_COMPREHENSIVE_WITH_FBA_FLAGS.csv', index=False)
    print(f"✅ Complete dataset with FBA flags saved to: FINAL_COMPREHENSIVE_WITH_FBA_FLAGS.csv")
    
    return df_analyzed, amazon_fba_df

if __name__ == "__main__":
    df_analyzed, amazon_fba_df = main() 