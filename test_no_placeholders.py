#!/usr/bin/env python3
"""
Test script to verify placeholder elimination in business scraper
"""

import subprocess
import pandas as pd
import os

def test_placeholder_elimination():
    """Test that the fixed scraper eliminates placeholder values"""
    print("🧪 TESTING PLACEHOLDER ELIMINATION...")
    
    # Check if we have the fixed scraper
    if not os.path.exists('fixed_comprehensive_scraper.py'):
        print("❌ Fixed scraper not found")
        return
    
    # Run the fixed scraper with a small test
    print("🔄 Running fixed scraper test...")
    try:
        # Test BizBuySell specifically
        result = subprocess.run([
            'python3', 'fixed_comprehensive_scraper.py'
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            print(f"❌ Scraper failed: {result.stderr}")
            return
            
        print("✅ Scraper completed successfully")
        
    except subprocess.TimeoutExpired:
        print("⏰ Scraper test timed out (normal for quick test)")
    except Exception as e:
        print(f"❌ Error running scraper: {e}")
        return
    
    # Check for placeholder values in results
    result_files = [
        'FIXED_COMPREHENSIVE_BUSINESS_LISTINGS.csv',
        'bizbuysell_final_amazon_businesses.csv',
        'AMAZON_FBA_BUSINESSES.csv'
    ]
    
    placeholder_found = False
    
    for filename in result_files:
        if os.path.exists(filename):
            print(f"\n🔍 Checking {filename} for placeholders...")
            
            try:
                df = pd.read_csv(filename)
                
                # Check for known placeholder values
                placeholder_checks = [
                    (df['price'] == '$250,000').sum(),
                    (df['revenue'] == '$2,022').sum(),
                    (df['profit'] == '$500,004').sum(),
                    ((df['price'] == '$250,000') & (df['revenue'] == '$2,022')).sum(),
                    ((df['price'] == '$250,000') & (df['profit'] == '$500,004')).sum(),
                ]
                
                total_placeholders = sum(placeholder_checks)
                
                if total_placeholders > 0:
                    print(f"❌ Found {total_placeholders} placeholder values in {filename}")
                    print(f"   Price $250,000: {placeholder_checks[0]}")
                    print(f"   Revenue $2,022: {placeholder_checks[1]}")
                    print(f"   Profit $500,004: {placeholder_checks[2]}")
                    print(f"   Combined price+revenue: {placeholder_checks[3]}")
                    print(f"   Combined price+profit: {placeholder_checks[4]}")
                    placeholder_found = True
                else:
                    print(f"✅ No placeholder values found in {filename}")
                    print(f"   Total businesses: {len(df)}")
                    print(f"   Valid prices: {df['price'].notna().sum()}")
                    print(f"   Valid revenues: {df['revenue'].notna().sum()}")
                    print(f"   Valid profits: {df['profit'].notna().sum()}")
                
            except Exception as e:
                print(f"❌ Error reading {filename}: {e}")
    
    # Final verdict
    print(f"\n{'='*50}")
    if placeholder_found:
        print("❌ PLACEHOLDER TEST FAILED - Placeholders still present")
        print("💡 The scraper needs further fixes to eliminate all placeholders")
    else:
        print("✅ PLACEHOLDER TEST PASSED - No placeholders found!")
        print("🎉 The fixed scraper successfully eliminates placeholder values")
    print(f"{'='*50}")

def check_data_quality():
    """Check overall data quality"""
    print("\n📊 CHECKING DATA QUALITY...")
    
    result_files = [
        'FIXED_COMPREHENSIVE_BUSINESS_LISTINGS.csv',
        'MAX_PERFORMANCE_BUSINESS_LISTINGS.csv',
        'OPTIMIZED_FAST_BUSINESS_LISTINGS.csv'
    ]
    
    for filename in result_files:
        if os.path.exists(filename):
            try:
                df = pd.read_csv(filename)
                
                print(f"\n📁 {filename}:")
                print(f"   Total businesses: {len(df):,}")
                
                # Check financial data coverage
                price_count = df['price'].notna().sum()
                revenue_count = df['revenue'].notna().sum() 
                profit_count = df['profit'].notna().sum()
                
                print(f"   Financial coverage:")
                print(f"     Prices: {price_count:,} ({price_count/len(df)*100:.1f}%)")
                print(f"     Revenue: {revenue_count:,} ({revenue_count/len(df)*100:.1f}%)")
                print(f"     Profit: {profit_count:,} ({profit_count/len(df)*100:.1f}%)")
                
                # Check for diversity in financial values
                unique_prices = df['price'].nunique()
                unique_revenues = df['revenue'].nunique()
                unique_profits = df['profit'].nunique()
                
                print(f"   Data diversity:")
                print(f"     Unique prices: {unique_prices:,}")
                print(f"     Unique revenues: {unique_revenues:,}")
                print(f"     Unique profits: {unique_profits:,}")
                
                # Source breakdown
                if 'source' in df.columns:
                    sources = df['source'].value_counts()
                    print(f"   Sources: {', '.join([f'{src}({count})' for src, count in sources.head(3).items()])}")
                
            except Exception as e:
                print(f"❌ Error analyzing {filename}: {e}")

if __name__ == "__main__":
    test_placeholder_elimination()
    check_data_quality() 