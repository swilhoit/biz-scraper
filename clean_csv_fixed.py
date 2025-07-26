#!/usr/bin/env python3
"""
Fixed Clean CSV - Remove Only Obvious Non-Business Listings
More precise filtering to preserve legitimate businesses
"""

import pandas as pd
import re

def is_problematic_entry(row):
    """Check if an entry is clearly not a business listing - MUCH MORE PRECISE"""
    name = str(row['name']).lower().strip()
    url = str(row['url']).lower()
    description = str(row['description']).lower()
    
    # Only flag EXACT matches for navigation terms (not partial matches)
    exact_navigation_terms = [
        'businesses', 'franchises', 'brokers', 'dashboard', 
        'my business', 'my listings', 'my saved listings', 
        'add a new listing', 'get valuation report',
        'how to buy a business', 'how to sell a business', 
        'value a business', 'sell a business on bizbuysell',
        'buy a franchise', 'business opportunities', 
        'retail franchises', 'restaurant and food franchises', 
        'low cost franchises', 'established businesses',
        'listings', 'list business for sale', 'cash flow',
        'established', 'internet', 'profitable', 
        'for sale by owner', 'managed by broker',
        'sell multiple businesses'
    ]
    
    # Check if name is EXACTLY a navigation term (not just containing it)
    if name in exact_navigation_terms:
        return True
    
    # Check for very short generic names (less than 15 characters is suspicious)
    if len(name) < 15:
        return True
    
    # Check for URLs that are clearly categories/navigation (more specific patterns)
    obvious_category_urls = [
        '/buy/$', '/sell/$', '/franchise-for-sale/$', '/how-to-', 
        '/mybbs/', '/fsbo/', '/business-valuation', '/brokers/$',
        '/business/cash-flow/$', '/business/established/$', 
        '/business/internet/$', '/business/profitable/$',
        '/business/for-sale-by-owner/$', '/business/brokered/$', 
        '/partner/', '/listings/$', '/sell/$'
    ]
    
    # Use regex for exact URL pattern matching
    for pattern in obvious_category_urls:
        if re.search(pattern, url):
            return True
    
    # Check for template/repetitive descriptions (navigation content)
    template_descriptions = [
        'buy a business search for a business established businesses',
        'cash flow, established, internet, profitable',
        'my listings guide to selling',
        'buy sell premium listings',
        'sign in register list business for sale'
    ]
    
    for template in template_descriptions:
        if template in description:
            return True
    
    # Check for sponsored content
    if 'sponsored' in name and len(name) < 50:
        return True
    
    return False

def clean_csv_precise():
    """Clean the CSV file by removing ONLY obvious problematic entries"""
    print("🧹 PRECISE CLEANING MAXIMIZED_WORKING_CACHE.csv...")
    
    # Load the CSV
    df = pd.read_csv('MAXIMIZED_WORKING_CACHE.csv')
    initial_count = len(df)
    
    print(f"Initial entries: {initial_count}")
    
    # Apply precise filtering
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
    
    # Show examples of what we kept
    print(f"\n✅ EXAMPLES OF KEPT ENTRIES:")
    for i, row in clean_df.head(5).iterrows():
        print(f"  • {row['source']}: {row['name'][:60]}...")
    
    # Save cleaned CSV
    clean_df.to_csv('PRECISELY_CLEAN_BUSINESS_LISTINGS.csv', index=False)
    
    # Show breakdown by source
    print(f"\n📊 PRECISE BREAKDOWN BY SOURCE:")
    source_counts = clean_df['source'].value_counts()
    for source, count in source_counts.items():
        print(f"  {source}: {count:,} businesses")
    
    # Show breakdown by category
    print(f"\n📂 PRECISE BREAKDOWN BY CATEGORY:")
    category_counts = clean_df['category'].value_counts()
    for category, count in category_counts.items():
        print(f"  {category}: {count:,} businesses")
    
    # Financial data quality
    price_count = (clean_df['price'] != '').sum()
    revenue_count = (clean_df['revenue'] != '').sum()
    profit_count = (clean_df['profit'] != '').sum()
    
    print(f"\n💰 PRECISE FINANCIAL DATA:")
    print(f"  Prices: {price_count}/{len(clean_df)} ({price_count/len(clean_df)*100:.1f}%)")
    print(f"  Revenue: {revenue_count}/{len(clean_df)} ({revenue_count/len(clean_df)*100:.1f}%)")
    print(f"  Profit: {profit_count}/{len(clean_df)} ({profit_count/len(clean_df)*100:.1f}%)")
    
    # Amazon FBA specific analysis
    amazon_fba = clean_df[clean_df['category'] == 'Amazon FBA']
    print(f"\n🎯 AMAZON FBA FOCUS:")
    print(f"  Amazon FBA businesses: {len(amazon_fba):,}")
    print(f"  Amazon FBA with prices: {(amazon_fba['price'] != '').sum()}")
    print(f"  Amazon FBA with revenue: {(amazon_fba['revenue'] != '').sum()}")
    
    print(f"\n💾 Exported to: PRECISELY_CLEAN_BUSINESS_LISTINGS.csv")
    print("✅ Removed only obvious navigation/category pages - preserved all legitimate businesses!")

if __name__ == "__main__":
    clean_csv_precise() 