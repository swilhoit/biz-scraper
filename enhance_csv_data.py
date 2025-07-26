#!/usr/bin/env python3
"""
Enhance the business details CSV with calculated fields and data cleaning
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

def clean_currency_value(value):
    """Convert currency strings to numeric values"""
    if pd.isna(value) or str(value).strip() == '':
        return np.nan
    
    value = str(value)
    # Remove currency symbols and text
    value = re.sub(r'[^\d.,KkMmBb]+', '', value)
    
    if not value:
        return np.nan
    
    # Handle K, M, B suffixes
    multiplier = 1
    if value[-1].upper() == 'K':
        multiplier = 1000
        value = value[:-1]
    elif value[-1].upper() == 'M':
        multiplier = 1000000
        value = value[:-1]
    elif value[-1].upper() == 'B':
        multiplier = 1000000000
        value = value[:-1]
    
    # Remove commas and convert to float
    try:
        value = float(value.replace(',', ''))
        return value * multiplier
    except:
        return np.nan

def standardize_location(location):
    """Standardize location format"""
    if pd.isna(location) or str(location).strip() == '':
        return ''
    
    location = str(location).strip()
    
    # Clean up common patterns
    location = re.sub(r'\s*\(.*?\)', '', location)  # Remove parentheses
    location = re.sub(r'Select State.*', '', location)  # Remove "Select State"
    location = re.sub(r'Select County.*', '', location)  # Remove "Select County"
    
    # Extract country if present
    if 'USA' in location or 'United States' in location:
        return 'United States'
    elif 'Australia' in location:
        return 'Australia'
    elif 'Canada' in location:
        return 'Canada'
    elif 'UK' in location or 'United Kingdom' in location:
        return 'United Kingdom'
    
    # Clean up specific formats
    parts = location.split(',')
    if len(parts) > 1:
        # Try to extract state/country
        last_part = parts[-1].strip()
        if len(last_part) == 2:  # Likely a US state code
            return f'United States - {last_part}'
        else:
            return last_part
    
    return location

def calculate_data_quality_score(row):
    """Calculate a data quality score for each listing"""
    key_fields = [
        'asking_price_numeric', 'annual_revenue_numeric', 'annual_profit_numeric',
        'ebitda_numeric', 'multiple_numeric', 'industry_standardized',
        'location_standardized', 'employees', 'established_date'
    ]
    
    score = 0
    for field in key_fields:
        if field in row and pd.notna(row[field]) and str(row[field]).strip() != '':
            score += 1
    
    return (score / len(key_fields)) * 100

def extract_numeric_from_text(text, pattern):
    """Extract numeric value from text using regex pattern"""
    if pd.isna(text):
        return np.nan
    
    match = re.search(pattern, str(text), re.IGNORECASE)
    if match:
        return clean_currency_value(match.group())
    return np.nan

def main():
    # Load the original CSV
    df = pd.read_csv('enhanced_business_details.csv')
    print(f"Loaded {len(df)} records")
    
    # 1. Create numeric versions of financial fields
    print("\n1. Creating numeric financial fields...")
    df['asking_price_numeric'] = df['asking_price'].apply(clean_currency_value)
    df['annual_revenue_numeric'] = df['annual_revenue'].apply(clean_currency_value)
    df['annual_profit_numeric'] = df['annual_profit'].apply(clean_currency_value)
    df['monthly_revenue_numeric'] = df['monthly_revenue'].apply(clean_currency_value)
    df['monthly_profit_numeric'] = df['monthly_profit'].apply(clean_currency_value)
    df['ebitda_numeric'] = df['ebitda'].apply(clean_currency_value)
    df['sde_numeric'] = df['sde'].apply(clean_currency_value)
    df['inventory_value_numeric'] = df['inventory_value'].apply(clean_currency_value)
    
    # Extract multiple as numeric
    df['multiple_numeric'] = df['multiple'].apply(lambda x: extract_numeric_from_text(x, r'[\d.]+'))
    
    # 2. Calculate financial ratios
    print("2. Calculating financial ratios...")
    # Price to revenue ratio
    df['price_to_revenue_ratio'] = np.where(
        (df['asking_price_numeric'] > 0) & (df['annual_revenue_numeric'] > 0),
        df['asking_price_numeric'] / df['annual_revenue_numeric'],
        np.nan
    )
    
    # Price to profit ratio
    df['price_to_profit_ratio'] = np.where(
        (df['asking_price_numeric'] > 0) & (df['annual_profit_numeric'] > 0),
        df['asking_price_numeric'] / df['annual_profit_numeric'],
        np.nan
    )
    
    # Profit margin
    df['profit_margin_calculated'] = np.where(
        (df['annual_profit_numeric'] > 0) & (df['annual_revenue_numeric'] > 0),
        (df['annual_profit_numeric'] / df['annual_revenue_numeric']) * 100,
        np.nan
    )
    
    # Monthly revenue calculated (if annual is available but monthly isn't)
    df['monthly_revenue_calculated'] = np.where(
        (df['annual_revenue_numeric'] > 0) & pd.isna(df['monthly_revenue_numeric']),
        df['annual_revenue_numeric'] / 12,
        df['monthly_revenue_numeric']
    )
    
    # 3. Standardize locations
    print("3. Standardizing locations...")
    df['location_standardized'] = df['location'].apply(standardize_location)
    
    # Extract state from location
    df['state_extracted'] = df['location'].apply(lambda x: 
        re.search(r'\b([A-Z]{2})\b', str(x)).group(1) if pd.notna(x) and re.search(r'\b([A-Z]{2})\b', str(x)) else ''
    )
    
    # 4. Standardize industries
    print("4. Standardizing industries...")
    industry_mapping = {
        'Terms Reference': 'Other',
        'Insights in your Mailbox': 'Other',
        'AIAutomotive': 'Automotive',
        'that is profitable': 'Other',
        'In Consumer Goods': 'Consumer Goods',
        'In Sporting Goods': 'Sporting Goods',
        'In Medical Devices': 'Healthcare',
        'In Retail Outdoor Equipment': 'Outdoor & Recreation'
    }
    
    df['industry_standardized'] = df['industry'].replace(industry_mapping)
    
    # Clean up industry values
    df['industry_standardized'] = df['industry_standardized'].apply(lambda x: 
        str(x).strip() if pd.notna(x) and len(str(x)) < 50 else 'Other'
    )
    
    # 5. Extract years in business as numeric
    print("5. Extracting numeric business age...")
    df['years_in_business_numeric'] = pd.to_numeric(df['years_in_business'], errors='coerce')
    
    # Calculate from established date if not available
    current_year = datetime.now().year
    df['established_year'] = pd.to_numeric(df['established_date'], errors='coerce')
    df['years_in_business_calculated'] = np.where(
        pd.notna(df['established_year']),
        current_year - df['established_year'],
        df['years_in_business_numeric']
    )
    
    # 6. Clean employee counts
    print("6. Cleaning employee counts...")
    df['employees_numeric'] = pd.to_numeric(df['employees'], errors='coerce')
    
    # 7. Extract Amazon-specific metrics from descriptions
    print("7. Extracting Amazon-specific metrics...")
    
    # Extract review counts
    df['review_count'] = df['full_description'].apply(lambda x: 
        extract_numeric_from_text(x, r'(\d+(?:,\d+)?)\s*reviews?')
    )
    
    # Extract star ratings
    df['star_rating'] = df['full_description'].apply(lambda x: 
        extract_numeric_from_text(x, r'(\d+(?:\.\d+)?)\s*star')
    )
    
    # Extract subscriber counts
    df['subscriber_count'] = df['full_description'].apply(lambda x: 
        extract_numeric_from_text(x, r'(\d+(?:,\d+)?)\s*(?:subscribers?|subs)')
    )
    
    # 8. Add data quality score
    print("8. Calculating data quality scores...")
    df['data_quality_score'] = df.apply(calculate_data_quality_score, axis=1)
    
    # 9. Add scrape metadata
    df['data_enhanced_date'] = datetime.now().isoformat()
    
    # 10. Create listing value categories
    print("9. Creating value categories...")
    df['listing_value_category'] = pd.cut(
        df['asking_price_numeric'],
        bins=[0, 100000, 500000, 1000000, 5000000, np.inf],
        labels=['Under $100K', '$100K-$500K', '$500K-$1M', '$1M-$5M', 'Over $5M']
    )
    
    # 11. Flag high-quality listings
    df['high_quality_listing'] = (
        (df['data_quality_score'] >= 60) & 
        (pd.notna(df['asking_price_numeric'])) &
        (pd.notna(df['annual_revenue_numeric']) | pd.notna(df['annual_profit_numeric']))
    )
    
    # Select columns for the enhanced CSV (excluding empty columns)
    columns_to_keep = []
    for col in df.columns:
        if df[col].notna().sum() > 0:  # Keep columns with at least one non-null value
            columns_to_keep.append(col)
    
    # Reorder columns for better readability
    priority_columns = [
        'source', 'name', 'page_title', 
        # Original values
        'asking_price', 'annual_revenue', 'annual_profit', 'ebitda', 'multiple',
        # Numeric versions
        'asking_price_numeric', 'annual_revenue_numeric', 'annual_profit_numeric',
        'ebitda_numeric', 'multiple_numeric',
        # Calculated metrics
        'price_to_revenue_ratio', 'price_to_profit_ratio', 'profit_margin_calculated',
        'monthly_revenue_calculated',
        # Business details
        'industry', 'industry_standardized', 'location', 'location_standardized',
        'state_extracted', 'established_date', 'years_in_business_calculated',
        'employees_numeric',
        # Amazon metrics
        'review_count', 'star_rating', 'subscriber_count',
        # Quality metrics
        'data_quality_score', 'high_quality_listing', 'listing_value_category',
        # URLs and timestamps
        'url', 'scrape_timestamp', 'data_enhanced_date'
    ]
    
    # Add remaining columns
    other_columns = [col for col in columns_to_keep if col not in priority_columns]
    final_columns = [col for col in priority_columns if col in df.columns] + other_columns
    
    # Save enhanced CSV
    enhanced_df = df[final_columns]
    enhanced_df.to_csv('enhanced_business_details_v2.csv', index=False)
    print(f"\nSaved enhanced CSV with {len(enhanced_df)} rows and {len(final_columns)} columns")
    
    # Print summary statistics
    print("\n=== ENHANCEMENT SUMMARY ===")
    print(f"Records with numeric asking price: {enhanced_df['asking_price_numeric'].notna().sum()}")
    print(f"Records with numeric revenue: {enhanced_df['annual_revenue_numeric'].notna().sum()}")
    print(f"Records with calculated ratios: {enhanced_df['price_to_revenue_ratio'].notna().sum()}")
    print(f"High quality listings: {enhanced_df['high_quality_listing'].sum()}")
    print(f"Average data quality score: {enhanced_df['data_quality_score'].mean():.1f}%")
    
    # Show value distribution
    print("\nListing Value Distribution:")
    print(enhanced_df['listing_value_category'].value_counts().sort_index())

if __name__ == "__main__":
    main()