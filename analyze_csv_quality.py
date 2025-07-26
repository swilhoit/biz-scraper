#!/usr/bin/env python3
"""
Analyze the quality of the enhanced business details CSV
"""

import pandas as pd
import numpy as np
import re

# Load the CSV
df = pd.read_csv('enhanced_business_details.csv')

print('=== DATA QUALITY ANALYSIS ===')
print(f'Total rows: {len(df)}')
print(f'Total columns: {len(df.columns)}')

# Check data completeness for key fields
key_fields = ['asking_price', 'annual_revenue', 'annual_profit', 'ebitda', 'multiple', 
              'industry', 'location', 'employees', 'established_date', 'monthly_visitors']

print('\n=== KEY FIELD COMPLETENESS ===')
for field in key_fields:
    if field in df.columns:
        non_empty = df[field].notna() & (df[field].astype(str).str.strip() != '')
        count = non_empty.sum()
        pct = (count / len(df)) * 100
        print(f'{field}: {count}/{len(df)} ({pct:.1f}%)')

# Check for fields that might need cleaning
print('\n=== FIELDS NEEDING CLEANING ===')
for col in ['asking_price', 'annual_revenue', 'annual_profit']:
    if col in df.columns:
        sample_values = df[col].dropna().astype(str).head(5).tolist()
        print(f'\n{col} samples:')
        for val in sample_values[:5]:
            print(f'  - {val}')

# Check for missing important fields that could be added
print('\n=== POTENTIAL IMPROVEMENTS ===')

# 1. Calculated fields that could be added
print('\n1. CALCULATED FIELDS TO ADD:')
calculated_suggestions = [
    'asking_price_numeric (cleaned numeric version)',
    'revenue_numeric (cleaned numeric version)', 
    'profit_numeric (cleaned numeric version)',
    'price_to_revenue_ratio',
    'price_to_profit_ratio',
    'profit_margin_calculated',
    'monthly_revenue_calculated',
    'days_on_market',
    'listing_age'
]
for field in calculated_suggestions:
    print(f'  - {field}')

# 2. Additional data points to extract
print('\n2. ADDITIONAL DATA TO EXTRACT:')
additional_fields = [
    'cash_flow_multiple',
    'working_capital_included',
    'financing_terms',
    'earnout_terms',
    'amazon_account_health',
    'amazon_seller_rating',
    'number_of_reviews',
    'best_seller_rank',
    'subscription_revenue_percentage',
    'customer_lifetime_value',
    'monthly_unique_customers',
    'top_traffic_sources',
    'top_performing_products',
    'seasonal_trends',
    'warranty_claims_rate',
    'return_rate',
    'supplier_terms',
    'minimum_order_quantities',
    'lead_time_by_supplier',
    'competitive_landscape',
    'market_size',
    'growth_projections'
]
for field in additional_fields:
    print(f'  - {field}')

# 3. Data quality issues
print('\n3. DATA QUALITY OBSERVATIONS:')

# Check for inconsistent formats
price_formats = set()
if 'asking_price' in df.columns:
    for price in df['asking_price'].dropna().astype(str).head(20):
        if 'Price' in price:
            price_formats.add('Contains "Price"')
        if '$' in price:
            price_formats.add('Contains $')
        if ',' in price:
            price_formats.add('Contains commas')
    print(f'  - Price format variations: {price_formats}')

# Check industry standardization
if 'industry' in df.columns:
    unique_industries = df['industry'].nunique()
    print(f'  - Unique industries: {unique_industries} (consider standardizing)')

# Check location formats
if 'location' in df.columns:
    location_samples = df['location'].dropna().head(5).tolist()
    print(f'  - Location format samples: {location_samples[:3]}')

# 4. Enrichment opportunities
print('\n4. DATA ENRICHMENT OPPORTUNITIES:')
enrichment_ideas = [
    'Geocode locations to get coordinates',
    'Standardize industry categories',
    'Calculate year-over-year growth rates',
    'Add market comparison data',
    'Include economic indicators for locations',
    'Add competitor analysis',
    'Include market trending data'
]
for idea in enrichment_ideas:
    print(f'  - {idea}')

# Check for completely empty columns
print('\n5. EMPTY COLUMNS (consider removing):')
empty_cols = []
for col in df.columns:
    if df[col].notna().sum() == 0 or (df[col].astype(str).str.strip() == '').all():
        empty_cols.append(col)
        
if empty_cols:
    for col in empty_cols[:10]:  # Show first 10
        print(f'  - {col}')
    if len(empty_cols) > 10:
        print(f'  ... and {len(empty_cols) - 10} more')
else:
    print('  None found')

print('\n=== RECOMMENDATIONS ===')
print('1. Add numeric versions of financial fields for easier analysis')
print('2. Standardize location and industry formats')
print('3. Calculate additional financial ratios')
print('4. Extract more Amazon-specific metrics')
print('5. Add data validation and cleaning functions')
print('6. Consider adding a "data_quality_score" field')
print('7. Add fields for tracking listing updates/changes')