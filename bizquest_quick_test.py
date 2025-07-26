#!/usr/bin/env python3
"""Quick test of BizQuest with revenue data"""

import pandas as pd

# Check if we have any results from the partial run
import os
if os.path.exists('bizquest_full_results.csv'):
    df = pd.read_csv('bizquest_full_results.csv')
    print(f"Found {len(df)} listings")
    
    # Show listings with revenue
    revenue_df = df[df['revenue'].notna() & (df['revenue'] != '')]
    print(f"\nListings with revenue data: {len(revenue_df)}")
    
    if len(revenue_df) > 0:
        print("\nSample listings with revenue:")
        for idx, row in revenue_df.head(5).iterrows():
            print(f"\n{row['name']}")
            print(f"  Location: {row['location']}")
            print(f"  Price: {row['price']}")
            print(f"  Revenue: {row['revenue']}")
            print(f"  Cash Flow: {row['profit']}")
            print(f"  Years: {row.get('years_in_operation', 'N/A')}")
else:
    print("No results file found. Let's check the partial data from the HTML we saved...")
    
    # We know from the test that the first listing has:
    # Gross Revenue: $350,000
    # Cash Flow: $130,000
    # Price: $100,000
    
    sample_data = [{
        'source': 'BizQuest',
        'name': 'Established Online Household Goods Store',
        'location': 'Piscataway, NJ',
        'price': '$100,000',
        'revenue': '$350,000',
        'profit': '$130,000',
        'years_in_operation': '5',
        'employees': '3',
        'description': 'High-Demand Niche. This turnkey e-commerce business specializes in household essentials.',
        'url': 'https://www.bizquest.com/business-for-sale/established-online-household-goods-store/BW2388457/'
    }]
    
    df = pd.DataFrame(sample_data)
    print("\nSample BizQuest listing with revenue data:")
    print(df.to_string(index=False))