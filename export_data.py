#!/usr/bin/env python3
"""
Export scraped data to CSV format
"""

import argparse
import pandas as pd
from datetime import datetime
from database import get_session, Business

def export_to_csv(filename: str = None, source_site: str = None, amazon_only: bool = False):
    """
    Export business listings to CSV
    
    Args:
        filename: Output filename (defaults to timestamp)
        source_site: Filter by source site
        amazon_only: Only export Amazon FBA businesses
    """
    session = get_session()
    
    # Build query
    query = session.query(Business)
    
    if source_site:
        query = query.filter(Business.source_site == source_site)
    
    if amazon_only:
        query = query.filter(Business.is_amazon_fba == True)
    
    # Get all businesses
    businesses = query.all()
    
    if not businesses:
        print("No businesses found matching criteria")
        return
    
    # Convert to list of dicts
    data = []
    for business in businesses:
        data.append({
            'id': business.id,
            'source_site': business.source_site,
            'listing_url': business.listing_url,
            'title': business.title,
            'price': business.price,
            'revenue': business.revenue,
            'cash_flow': business.cash_flow,
            'multiple': business.multiple,
            'location': business.location,
            'industry': business.industry,
            'description': business.description[:500] if business.description else None,
            'seller_financing': business.seller_financing,
            'established_year': business.established_year,
            'employees': business.employees,
            'ebitda': business.ebitda,
            'inventory_value': business.inventory_value,
            'reason_for_selling': business.reason_for_selling,
            'website': business.website,
            'is_amazon_fba': business.is_amazon_fba,
            'amazon_business_type': business.amazon_business_type,
            'scraped_at': business.scraped_at,
            'enhanced_at': business.enhanced_at
        })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Generate filename if not provided
    if not filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filters = []
        if source_site:
            filters.append(source_site.lower())
        if amazon_only:
            filters.append('amazon_fba')
        
        filter_str = '_' + '_'.join(filters) if filters else ''
        filename = f'business_listings{filter_str}_{timestamp}.csv'
    
    # Export to CSV
    df.to_csv(filename, index=False)
    print(f"Exported {len(df)} listings to {filename}")
    
    # Print summary
    print("\nSummary:")
    print(f"Total listings: {len(df)}")
    print(f"Listings with price: {df['price'].notna().sum()}")
    print(f"Amazon FBA listings: {df['is_amazon_fba'].sum()}")
    print(f"Sources: {', '.join(df['source_site'].value_counts().index.tolist())}")
    
    if df['price'].notna().any():
        print(f"\nPrice range: ${df['price'].min():,.0f} - ${df['price'].max():,.0f}")
        print(f"Average price: ${df['price'].mean():,.0f}")

def main():
    parser = argparse.ArgumentParser(description='Export business listings to CSV')
    parser.add_argument('-o', '--output', help='Output filename')
    parser.add_argument('--source', help='Filter by source site')
    parser.add_argument('--amazon', action='store_true', help='Only export Amazon FBA businesses')
    
    args = parser.parse_args()
    
    export_to_csv(
        filename=args.output,
        source_site=args.source,
        amazon_only=args.amazon
    )

if __name__ == '__main__':
    main()