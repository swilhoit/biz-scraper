#!/usr/bin/env python3
"""
Data Analysis Tool for Business Listings
Provides insights and quality metrics for scraped data
"""

import argparse
from datetime import datetime
from sqlalchemy import func
from database import get_session, Business
from utils.amazon_detector import AmazonFBADetector

class DataAnalyzer:
    def __init__(self):
        self.session = get_session()
    
    def analyze_by_source(self):
        """Analyze listings by source site"""
        print("\n=== Listings by Source Site ===")
        results = self.session.query(
            Business.source_site,
            func.count(Business.id).label('count'),
            func.avg(Business.price).label('avg_price'),
            func.min(Business.price).label('min_price'),
            func.max(Business.price).label('max_price')
        ).group_by(Business.source_site).all()
        
        print(f"{'Site':<20} {'Count':<10} {'Avg Price':<15} {'Min Price':<15} {'Max Price':<15}")
        print("-" * 75)
        for row in results:
            avg_price = f"${row.avg_price:,.0f}" if row.avg_price else "N/A"
            min_price = f"${row.min_price:,.0f}" if row.min_price else "N/A"
            max_price = f"${row.max_price:,.0f}" if row.max_price else "N/A"
            print(f"{row.source_site:<20} {row.count:<10} {avg_price:<15} {min_price:<15} {max_price:<15}")
    
    def analyze_data_quality(self):
        """Analyze data completeness and quality"""
        print("\n=== Data Quality Analysis ===")
        
        total = self.session.query(func.count(Business.id)).scalar()
        
        # Count non-null fields
        metrics = {
            'Has Title': self.session.query(func.count(Business.id)).filter(Business.title.isnot(None)).scalar(),
            'Has Price': self.session.query(func.count(Business.id)).filter(Business.price.isnot(None)).scalar(),
            'Has Revenue': self.session.query(func.count(Business.id)).filter(Business.revenue.isnot(None)).scalar(),
            'Has Cash Flow': self.session.query(func.count(Business.id)).filter(Business.cash_flow.isnot(None)).scalar(),
            'Has Location': self.session.query(func.count(Business.id)).filter(Business.location.isnot(None)).scalar(),
            'Has Industry': self.session.query(func.count(Business.id)).filter(Business.industry.isnot(None)).scalar(),
            'Has Description': self.session.query(func.count(Business.id)).filter(Business.description.isnot(None)).scalar(),
        }
        
        print(f"Total Listings: {total}")
        print("\nField Completeness:")
        for field, count in metrics.items():
            percentage = (count / total * 100) if total > 0 else 0
            print(f"  {field:<20} {count:>6} ({percentage:>5.1f}%)")
    
    def analyze_price_ranges(self):
        """Analyze price distribution"""
        print("\n=== Price Range Analysis ===")
        
        ranges = [
            (0, 50000, "Under $50K"),
            (50000, 100000, "$50K - $100K"),
            (100000, 250000, "$100K - $250K"),
            (250000, 500000, "$250K - $500K"),
            (500000, 1000000, "$500K - $1M"),
            (1000000, 5000000, "$1M - $5M"),
            (5000000, float('inf'), "Over $5M")
        ]
        
        print(f"{'Price Range':<20} {'Count':<10} {'Percentage':<10}")
        print("-" * 40)
        
        total_with_price = self.session.query(func.count(Business.id)).filter(Business.price.isnot(None)).scalar()
        
        for min_price, max_price, label in ranges:
            query = self.session.query(func.count(Business.id)).filter(
                Business.price >= min_price,
                Business.price < max_price
            )
            count = query.scalar()
            percentage = (count / total_with_price * 100) if total_with_price > 0 else 0
            print(f"{label:<20} {count:<10} {percentage:>5.1f}%")
    
    def analyze_amazon_fba(self):
        """Analyze Amazon FBA businesses"""
        print("\n=== Amazon FBA Analysis ===")
        
        # Get all listings
        listings = self.session.query(Business).all()
        
        amazon_fba_count = 0
        amazon_other_count = 0
        by_type = {}
        
        for listing in listings:
            listing_dict = {
                'title': listing.title,
                'description': listing.description,
                'industry': listing.industry,
                'listing_url': listing.listing_url
            }
            
            if AmazonFBADetector.is_amazon_fba(listing_dict):
                amazon_fba_count += 1
                
            amazon_type = AmazonFBADetector.get_amazon_type(listing_dict)
            by_type[amazon_type] = by_type.get(amazon_type, 0) + 1
        
        print(f"Total Amazon FBA Businesses: {amazon_fba_count}")
        print(f"Non-Amazon Businesses: {by_type.get('non_amazon', 0)}")
        print("\nBreakdown by Type:")
        for business_type, count in sorted(by_type.items()):
            if business_type != 'non_amazon':
                print(f"  {business_type}: {count}")
    
    def find_high_value_listings(self, min_price=1000000):
        """Find high-value listings"""
        print(f"\n=== High-Value Listings (>=${min_price:,.0f}) ===")
        
        listings = self.session.query(Business).filter(
            Business.price >= min_price
        ).order_by(Business.price.desc()).limit(10).all()
        
        if listings:
            print(f"{'Title':<50} {'Price':<15} {'Source':<15}")
            print("-" * 80)
            for listing in listings:
                title = (listing.title[:47] + '...') if listing.title and len(listing.title) > 50 else listing.title
                price = f"${listing.price:,.0f}"
                print(f"{title:<50} {price:<15} {listing.source_site:<15}")
        else:
            print("No high-value listings found")
    
    def analyze_duplicates(self):
        """Find potential duplicate listings"""
        print("\n=== Duplicate Analysis ===")
        
        # Find listings with same title
        duplicates = self.session.query(
            Business.title,
            func.count(Business.id).label('count')
        ).filter(
            Business.title.isnot(None)
        ).group_by(Business.title).having(
            func.count(Business.id) > 1
        ).order_by(func.count(Business.id).desc()).limit(10).all()
        
        if duplicates:
            print("Potential duplicates (by title):")
            for title, count in duplicates:
                print(f"  '{title[:60]}...' appears {count} times")
        else:
            print("No duplicate titles found")
    
    def run_full_analysis(self):
        """Run all analysis functions"""
        print("=" * 80)
        print("Business Listings Data Analysis Report")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        self.analyze_by_source()
        self.analyze_data_quality()
        self.analyze_price_ranges()
        self.analyze_amazon_fba()
        self.find_high_value_listings()
        self.analyze_duplicates()
        
        print("\n" + "=" * 80)

def main():
    parser = argparse.ArgumentParser(description='Analyze scraped business listings data')
    parser.add_argument('--source', help='Analyze specific source only')
    parser.add_argument('--amazon', action='store_true', help='Run Amazon FBA analysis only')
    parser.add_argument('--quality', action='store_true', help='Run data quality analysis only')
    parser.add_argument('--high-value', type=int, help='Find listings above this price')
    
    args = parser.parse_args()
    
    analyzer = DataAnalyzer()
    
    if args.amazon:
        analyzer.analyze_amazon_fba()
    elif args.quality:
        analyzer.analyze_data_quality()
    elif args.high_value:
        analyzer.find_high_value_listings(args.high_value)
    else:
        analyzer.run_full_analysis()

if __name__ == '__main__':
    main()