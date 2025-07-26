#!/usr/bin/env python3

import pandas as pd
import numpy as np

def analyze_site_breakdown(df):
    """Comprehensive analysis of each marketplace/site"""
    
    print("=" * 80)
    print("🔍 COMPREHENSIVE SITE-BY-SITE BREAKDOWN")
    print("=" * 80)
    
    # Overall summary
    print(f"\n📊 OVERALL SUMMARY:")
    print(f"Total unique businesses: {len(df):,}")
    print(f"Total marketplaces: {df['source'].nunique()}")
    
    # Site breakdown
    source_counts = df['source'].value_counts()
    print(f"\n📈 MARKETPLACE DISTRIBUTION:")
    for source, count in source_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {source}: {count:,} businesses ({percentage:.1f}%)")
    
    print("\n" + "=" * 80)
    
    # Detailed analysis per site
    for source in df['source'].unique():
        site_df = df[df['source'] == source]
        
        print(f"\n🏢 {source.upper()} - DETAILED ANALYSIS")
        print("-" * 50)
        
        # Basic metrics
        print(f"📊 Basic Metrics:")
        print(f"  Total listings: {len(site_df):,}")
        print(f"  Market share: {len(site_df)/len(df)*100:.1f}%")
        
        # Financial coverage
        print(f"\n💰 Financial Data Coverage:")
        price_coverage = site_df['price_numeric'].notna().sum()
        revenue_coverage = site_df['revenue_numeric'].notna().sum()
        profit_coverage = site_df['profit_numeric'].notna().sum()
        
        print(f"  Price data: {price_coverage}/{len(site_df)} ({price_coverage/len(site_df)*100:.1f}%)")
        print(f"  Revenue data: {revenue_coverage}/{len(site_df)} ({revenue_coverage/len(site_df)*100:.1f}%)")
        print(f"  Profit data: {profit_coverage}/{len(site_df)} ({profit_coverage/len(site_df)*100:.1f}%)")
        
        # Price analysis
        valid_prices = site_df[site_df['price_numeric'].notna() & (site_df['price_numeric'] > 0)]
        if len(valid_prices) > 0:
            print(f"\n📈 Price Analysis:")
            print(f"  Price range: ${valid_prices['price_numeric'].min():,.0f} - ${valid_prices['price_numeric'].max():,.0f}")
            print(f"  Average price: ${valid_prices['price_numeric'].mean():,.0f}")
            print(f"  Median price: ${valid_prices['price_numeric'].median():,.0f}")
            
            # Price categories
            high_value = valid_prices[valid_prices['price_numeric'] >= 1000000]
            medium_value = valid_prices[(valid_prices['price_numeric'] >= 100000) & (valid_prices['price_numeric'] < 1000000)]
            small_value = valid_prices[valid_prices['price_numeric'] < 100000]
            
            print(f"\n🎯 Investment Categories:")
            print(f"  High-value (>$1M): {len(high_value):,} businesses")
            print(f"  Medium ($100K-$1M): {len(medium_value):,} businesses")
            print(f"  Small (<$100K): {len(small_value):,} businesses")
        
        # Revenue analysis
        valid_revenue = site_df[site_df['revenue_numeric'].notna() & (site_df['revenue_numeric'] > 0)]
        if len(valid_revenue) > 0:
            print(f"\n📊 Revenue Analysis:")
            print(f"  Revenue range: ${valid_revenue['revenue_numeric'].min():,.0f} - ${valid_revenue['revenue_numeric'].max():,.0f}")
            print(f"  Average revenue: ${valid_revenue['revenue_numeric'].mean():,.0f}")
            print(f"  Median revenue: ${valid_revenue['revenue_numeric'].median():,.0f}")
        
        # Top opportunities from this site
        print(f"\n🏆 TOP 5 OPPORTUNITIES FROM {source.upper()}:")
        top_5 = site_df.nlargest(5, 'price_numeric')
        
        for i, (_, row) in enumerate(top_5.iterrows(), 1):
            price = f"${row['price_numeric']:,.0f}" if pd.notna(row['price_numeric']) else "N/A"
            revenue = f"${row['revenue_numeric']:,.0f}" if pd.notna(row['revenue_numeric']) else "N/A"
            name_short = str(row['name'])[:50] + "..." if len(str(row['name'])) > 50 else str(row['name'])
            
            print(f"  {i}. {price} | Rev: {revenue}")
            print(f"     {name_short}")
        
        # Quality assessment
        print(f"\n⭐ QUALITY ASSESSMENT:")
        complete_data = site_df[(site_df['price_numeric'].notna()) & (site_df['revenue_numeric'].notna())]
        quality_score = len(complete_data) / len(site_df) * 100
        
        if quality_score >= 80:
            quality_rating = "🟢 Excellent"
        elif quality_score >= 60:
            quality_rating = "🟡 Good"
        elif quality_score >= 40:
            quality_rating = "🟠 Fair"
        else:
            quality_rating = "🔴 Poor"
            
        print(f"  Data completeness: {quality_score:.1f}% {quality_rating}")
        
        # Sample business types (if available in descriptions)
        if 'description' in site_df.columns:
            descriptions = site_df['description'].str.lower().fillna('')
            amazon_count = descriptions.str.contains('amazon|fba|fbm').sum()
            ecommerce_count = descriptions.str.contains('ecommerce|e-commerce').sum()
            saas_count = descriptions.str.contains('saas|software|subscription').sum()
            
            print(f"\n🏷️ Business Types (from descriptions):")
            print(f"  Amazon/FBA businesses: {amazon_count}")
            print(f"  E-commerce businesses: {ecommerce_count}")
            print(f"  SaaS/Software businesses: {saas_count}")
        
        print("\n" + "=" * 80)
    
    # Cross-site comparison
    print(f"\n📊 CROSS-SITE COMPARISON")
    print("-" * 50)
    
    comparison_data = []
    for source in df['source'].unique():
        site_df = df[df['source'] == source]
        
        # Calculate metrics
        avg_price = site_df['price_numeric'].mean()
        median_price = site_df['price_numeric'].median()
        max_price = site_df['price_numeric'].max()
        price_coverage = site_df['price_numeric'].notna().sum() / len(site_df) * 100
        revenue_coverage = site_df['revenue_numeric'].notna().sum() / len(site_df) * 100
        
        comparison_data.append({
            'Source': source,
            'Count': len(site_df),
            'Avg_Price': avg_price,
            'Median_Price': median_price,
            'Max_Price': max_price,
            'Price_Coverage': price_coverage,
            'Revenue_Coverage': revenue_coverage
        })
    
    comp_df = pd.DataFrame(comparison_data)
    
    print(f"\n🏆 BEST MARKETPLACES BY CRITERIA:")
    print(f"  Highest volume: {comp_df.loc[comp_df['Count'].idxmax(), 'Source']} ({comp_df['Count'].max()} businesses)")
    print(f"  Highest avg price: {comp_df.loc[comp_df['Avg_Price'].idxmax(), 'Source']} (${comp_df['Avg_Price'].max():,.0f})")
    print(f"  Best data quality: {comp_df.loc[comp_df['Price_Coverage'].idxmax(), 'Source']} ({comp_df['Price_Coverage'].max():.1f}% coverage)")
    
    return comp_df

def main():
    # Load the cleaned results
    df = pd.read_csv('FINAL_COMPREHENSIVE_BUSINESS_LISTINGS.csv')
    
    # Perform comprehensive analysis
    comparison_df = analyze_site_breakdown(df)
    
    # Save comparison data
    comparison_df.to_csv('SITE_COMPARISON_ANALYSIS.csv', index=False)
    print(f"\n✅ Site comparison data saved to: SITE_COMPARISON_ANALYSIS.csv")
    
    return comparison_df

if __name__ == "__main__":
    comparison_df = main() 