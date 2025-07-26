#!/usr/bin/env python3
"""
Combine Final Results
Combines master script results with BizBuySell final results
"""

import pandas as pd
import re
from urllib.parse import urlparse

def clean_financial_value(value_str):
    """Clean and normalize financial values"""
    if not value_str or pd.isna(value_str):
        return ""
    
    # Remove common prefixes
    value_str = re.sub(r'^(Revenue|Sales|Profit|Cash Flow|SDE|EBITDA)[:\s]*', '', str(value_str), flags=re.IGNORECASE)
    
    # Extract the monetary value
    match = re.search(r'\$[\d,]+(?:\.\d+)?[KkMm]?', value_str)
    if match:
        return match.group()
    
    return ""

def calculate_multiple(price, revenue=None, profit=None):
    """Calculate business multiple"""
    try:
        if not price:
            return ""
            
        # Clean price
        price_clean = re.sub(r'[^\d.,]', '', price)
        if not price_clean:
            return ""
        price_val = float(price_clean.replace(',', ''))
        
        # Try revenue multiple first
        if revenue:
            revenue_clean = re.sub(r'[^\d.,KkMm]', '', revenue)
            if revenue_clean:
                revenue_val = float(revenue_clean.replace(',', '').replace('K', '000').replace('M', '000000'))
                if revenue_val > 0:
                    multiple = round(price_val / revenue_val, 2)
                    return f"{multiple}x"
        
        # Try profit multiple
        if profit:
            profit_clean = re.sub(r'[^\d.,KkMm]', '', profit)
            if profit_clean:
                profit_val = float(profit_clean.replace(',', '').replace('K', '000').replace('M', '000000'))
                if profit_val > 0:
                    multiple = round(price_val / profit_val, 2)
                    return f"{multiple}x (P/E)"
        
        return ""
    except:
        return ""

def main():
    print("🔄 COMBINING FINAL RESULTS...")
    
    # Read both datasets
    try:
        master_df = pd.read_csv('business_listings.csv')
        print(f"✅ Loaded {len(master_df)} listings from master script")
    except FileNotFoundError:
        print("❌ Master script results not found")
        master_df = pd.DataFrame()
    
    try:
        bizbuysell_df = pd.read_csv('bizbuysell_final_amazon_businesses.csv')
        print(f"✅ Loaded {len(bizbuysell_df)} listings from BizBuySell final scraper")
    except FileNotFoundError:
        print("❌ BizBuySell final results not found")
        bizbuysell_df = pd.DataFrame()
    
    if master_df.empty and bizbuysell_df.empty:
        print("❌ No data to combine!")
        return
    
    # Combine datasets
    if not master_df.empty and not bizbuysell_df.empty:
        combined_df = pd.concat([master_df, bizbuysell_df], ignore_index=True)
    elif not master_df.empty:
        combined_df = master_df.copy()
    else:
        combined_df = bizbuysell_df.copy()
    
    print(f"📊 Combined dataset: {len(combined_df)} total listings")
    
    # Remove duplicates based on URL
    combined_df['url_domain'] = combined_df['url'].apply(lambda x: urlparse(str(x)).netloc.lower() if pd.notna(x) else '')
    combined_df['url_path'] = combined_df['url'].apply(lambda x: urlparse(str(x)).path.lower() if pd.notna(x) else '')
    combined_df['url_signature'] = combined_df['url_domain'] + combined_df['url_path']
    
    # Also create name-based signature for additional deduplication
    combined_df['name_signature'] = combined_df['name'].apply(lambda x: re.sub(r'[^\w]', '', str(x).lower())[:50] if pd.notna(x) else '')
    
    # Remove duplicates
    before_dedup = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['url_signature'], keep='first')
    combined_df = combined_df.drop_duplicates(subset=['name_signature'], keep='first')
    after_dedup = len(combined_df)
    
    print(f"🧹 Removed {before_dedup - after_dedup} duplicates")
    
    # Clean up temporary columns
    combined_df = combined_df.drop(['url_domain', 'url_path', 'url_signature', 'name_signature'], axis=1)
    
    # Clean financial data
    combined_df['price'] = combined_df['price'].apply(clean_financial_value)
    combined_df['revenue'] = combined_df['revenue'].apply(clean_financial_value)
    combined_df['profit'] = combined_df['profit'].apply(clean_financial_value)
    
    # Recalculate multiples for all listings
    combined_df['multiple'] = combined_df.apply(
        lambda row: calculate_multiple(row['price'], row['revenue'], row['profit']), 
        axis=1
    )
    
    # Filter out navigation/junk listings
    valid_listings = []
    for _, row in combined_df.iterrows():
        name = str(row['name']).lower()
        
        # Skip obvious navigation/junk entries
        if any(skip in name for skip in ['log in', 'pricing', 'sellers', 'buyers', 'sponsored', 'instant listing']):
            continue
            
        # Must have either price, revenue, or profit data
        if not any([row['price'], row['revenue'], row['profit']]):
            continue
            
        # Name must be reasonable length
        if len(str(row['name'])) < 10:
            continue
            
        valid_listings.append(row)
    
    final_df = pd.DataFrame(valid_listings)
    
    if final_df.empty:
        print("❌ No valid listings after filtering!")
        return
    
    # Ensure proper column order
    column_order = ['source', 'name', 'price', 'revenue', 'profit', 'multiple', 'description', 'url']
    final_df = final_df[column_order]
    
    # Sort by source and then by price (highest first)
    final_df['price_numeric'] = final_df['price'].apply(lambda x: 
        float(re.sub(r'[^\d.]', '', str(x))) if x and re.search(r'[\d.]', str(x)) else 0
    )
    final_df = final_df.sort_values(['source', 'price_numeric'], ascending=[True, False])
    final_df = final_df.drop('price_numeric', axis=1)
    
    # Export final results
    filename = 'FINAL_COMPREHENSIVE_BUSINESS_LISTINGS.csv'
    final_df.to_csv(filename, index=False, quoting=1)
    
    # Generate summary
    print(f"\n{'='*70}")
    print("🏆 FINAL COMPREHENSIVE BUSINESS LISTINGS HARVESTED")
    print(f"{'='*70}")
    print(f"✅ Total unique businesses: {len(final_df)}")
    print(f"📊 Listings with prices: {final_df['price'].str.len().gt(0).sum()}")
    print(f"📊 Listings with revenue: {final_df['revenue'].str.len().gt(0).sum()}")
    print(f"📊 Listings with profit: {final_df['profit'].str.len().gt(0).sum()}")
    print(f"💾 Data exported to: {filename}")
    
    # Source breakdown
    print(f"\n📈 LISTINGS BY SOURCE:")
    source_counts = final_df['source'].value_counts()
    for source, count in source_counts.items():
        percentage = (count / len(final_df)) * 100
        print(f"  {source}: {count} listings ({percentage:.1f}%)")
    
    # Price analysis
    prices = final_df[final_df['price'].str.len() > 0]['price']
    if not prices.empty:
        price_values = prices.apply(lambda x: float(re.sub(r'[^\d.]', '', x)) if re.search(r'[\d.]', x) else 0)
        price_values = price_values[price_values > 0]
        if not price_values.empty:
            print(f"\n💰 PRICE ANALYSIS:")
            print(f"  Range: ${price_values.min():,.0f} - ${price_values.max():,.0f}")
            print(f"  Average: ${price_values.mean():,.0f}")
            print(f"  Median: ${price_values.median():,.0f}")
    
    # Show top 10 most valuable businesses
    valuable_businesses = final_df[final_df['price'].str.len() > 0].head(10)
    if not valuable_businesses.empty:
        print(f"\n🎯 TOP 10 HIGHEST VALUE OPPORTUNITIES:")
        for i, (_, row) in enumerate(valuable_businesses.iterrows()):
            print(f"  {i+1}. {row['name'][:80]}{'...' if len(row['name']) > 80 else ''}")
            print(f"     💰 Price: {row['price']}")
            if row['revenue']:
                print(f"     📈 Revenue: {row['revenue']}")
            if row['profit']:
                print(f"     💵 Profit: {row['profit']}")
            print(f"     🔗 Source: {row['source']}")
            print()

if __name__ == "__main__":
    main() 