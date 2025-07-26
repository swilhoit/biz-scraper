#!/usr/bin/env python3
"""
Clean CSV - Remove Non-Business Listings
Remove navigation links, category pages, and other non-business entries
"""

import pandas as pd
import re

def is_problematic_entry(row):
    """Check if an entry is clearly not a business listing"""
    name = str(row['name']).lower().strip()
    url = str(row['url']).lower()
    description = str(row['description']).lower()
    
    # Generic navigation/category terms
    generic_terms = [
        'businesses', 'franchises', 'brokers', 'dashboard', 'my business',
        'my listings', 'my saved listings', 'add a new listing', 'get valuation',
        'how to buy', 'how to sell', 'value a business', 'sell a business',
        'buy a franchise', 'business opportunities', 'retail franchises',
        'restaurant and food franchises', 'low cost franchises', 'established businesses',
        'asset sales', 'listings', 'list business for sale', 'cash flow',
        'established', 'internet', 'profitable', 'for sale by owner',
        'managed by broker', 'sign in', 'register', 'sponsored'
    ]
    
    # Check if name is generic
    if any(term in name for term in generic_terms):
        return True
    
    # Check for very short generic names
    if len(name) < 8:
        return True
    
    # Check for URLs that are clearly categories/navigation
    category_url_patterns = [
        '/buy/', '/sell/', '/franchise-for-sale/', '/how-to-', '/brokers/',
        '/mybbs/', '/fsbo/', '/business-valuation', '/listings/', '/business/cash-flow/',
        '/business/established/', '/business/internet/', '/business/profitable/',
        '/business/for-sale-by-owner/', '/business/brokered/', '/partner/'
    ]
    
    if any(pattern in url for pattern in category_url_patterns):
        return True
    
    # Check for names that are just single words or too generic
    single_word_terms = [
        'businesses', 'franchises', 'brokers', 'dashboard', 'listings',
        'established', 'internet', 'profitable', 'cash flow'
    ]
    
    if name in single_word_terms:
        return True
    
    # Check for duplicate/repetitive descriptions (indicates template/navigation content)
    if 'buy a business search for a business' in description:
        return True
    
    if 'cash flow, established, internet, profitable' in description:
        return True
    
    return False

def clean_csv():
    """Clean the CSV file by removing problematic entries"""
    print("🧹 CLEANING MAXIMIZED_WORKING_CACHE.csv...")
    
    # Load the CSV
    df = pd.read_csv('MAXIMIZED_WORKING_CACHE.csv')
    initial_count = len(df)
    
    print(f"Initial entries: {initial_count}")
    
    # Apply filtering
    clean_df = df[~df.apply(is_problematic_entry, axis=1)]
    final_count = len(clean_df)
    
    removed_count = initial_count - final_count
    
    print(f"Removed entries: {removed_count}")
    print(f"Clean entries: {final_count}")
    print(f"Removal rate: {removed_count/initial_count*100:.1f}%")
    
    # Show examples of what we removed
    removed_df = df[df.apply(is_problematic_entry, axis=1)]
    if len(removed_df) > 0:
        print(f"\n🗑️  EXAMPLES OF REMOVED ENTRIES:")
        for i, row in removed_df.head(10).iterrows():
            print(f"  • {row['source']}: {row['name'][:60]}...")
    
    # Save cleaned CSV
    clean_df.to_csv('CLEAN_BUSINESS_LISTINGS.csv', index=False)
    
    # Show breakdown by source
    print(f"\n📊 CLEAN BREAKDOWN BY SOURCE:")
    source_counts = clean_df['source'].value_counts()
    for source, count in source_counts.items():
        print(f"  {source}: {count:,} businesses")
    
    # Show breakdown by category
    print(f"\n📂 CLEAN BREAKDOWN BY CATEGORY:")
    category_counts = clean_df['category'].value_counts()
    for category, count in category_counts.items():
        print(f"  {category}: {count:,} businesses")
    
    # Financial data quality
    price_count = (clean_df['price'] != '').sum()
    revenue_count = (clean_df['revenue'] != '').sum()
    profit_count = (clean_df['profit'] != '').sum()
    
    print(f"\n💰 CLEAN FINANCIAL DATA:")
    print(f"  Prices: {price_count}/{len(clean_df)} ({price_count/len(clean_df)*100:.1f}%)")
    print(f"  Revenue: {revenue_count}/{len(clean_df)} ({revenue_count/len(clean_df)*100:.1f}%)")
    print(f"  Profit: {profit_count}/{len(clean_df)} ({profit_count/len(clean_df)*100:.1f}%)")
    
    print(f"\n💾 Exported to: CLEAN_BUSINESS_LISTINGS.csv")
    print("✅ Removed all navigation links, category pages, and non-business entries!")

if __name__ == "__main__":
    clean_csv() 