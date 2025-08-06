#!/usr/bin/env python3
"""
Comprehensive test suite for all business listing scrapers
Tests each site individually and reports results
"""

import logging
import time
from datetime import datetime
from database import init_database, get_session, Business
from scrapers import BizBuySellScraper, BizQuestScraper, FlippaScraper
from config import SITES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ScraperTester:
    def __init__(self):
        self.results = {}
        init_database()
        
    def test_site(self, site_name, scraper_class):
        """Test a single site scraper"""
        print(f"\n{'='*60}")
        print(f"Testing: {site_name}")
        print(f"{'='*60}")
        
        results = {
            'site': site_name,
            'search_page': False,
            'listings_found': 0,
            'listing_page': False,
            'data_extracted': False,
            'saved_to_db': False,
            'errors': []
        }
        
        try:
            # Get site config
            site_config = next(site for site in SITES if site['name'] == site_name)
            scraper = scraper_class(site_config)
            
            # Test 1: Get listings
            print(f"\n1. Testing search page access...")
            start = time.time()
            listing_urls = scraper.get_listing_urls(max_pages=1)
            elapsed = time.time() - start
            
            if listing_urls:
                results['search_page'] = True
                results['listings_found'] = len(listing_urls)
                print(f"   ✓ Found {len(listing_urls)} listings in {elapsed:.1f}s")
                print(f"   Sample URL: {listing_urls[0]}")
            else:
                print(f"   ✗ No listings found")
                results['errors'].append("No listings found on search page")
                
            # Test 2: Scrape a listing
            if listing_urls:
                print(f"\n2. Testing listing page scraping...")
                start = time.time()
                listing_data = scraper.scrape_listing(listing_urls[0])
                elapsed = time.time() - start
                
                if listing_data:
                    results['listing_page'] = True
                    print(f"   ✓ Scraped listing in {elapsed:.1f}s")
                    
                    # Check data quality
                    filled_fields = sum(1 for v in listing_data.values() if v)
                    total_fields = len(listing_data)
                    print(f"   Data completeness: {filled_fields}/{total_fields} fields")
                    
                    if listing_data.get('title'):
                        results['data_extracted'] = True
                        print(f"   Title: {listing_data['title'][:50]}...")
                        if listing_data.get('price'):
                            print(f"   Price: ${listing_data['price']:,.0f}")
                        if listing_data.get('location'):
                            print(f"   Location: {listing_data['location']}")
                    
                    # Test 3: Save to database
                    print(f"\n3. Testing database save...")
                    if scraper.save_business(listing_data):
                        results['saved_to_db'] = True
                        print(f"   ✓ Saved to database")
                    else:
                        print(f"   ✗ Failed to save")
                        results['errors'].append("Database save failed")
                else:
                    print(f"   ✗ Failed to scrape listing")
                    results['errors'].append("Listing scrape failed")
                    
            scraper.db_session.close()
            
        except Exception as e:
            print(f"\n   ERROR: {str(e)}")
            results['errors'].append(str(e))
            
        self.results[site_name] = results
        return results
    
    def print_summary(self):
        """Print summary of all tests"""
        print(f"\n\n{'='*60}")
        print("SUMMARY REPORT")
        print(f"{'='*60}")
        print(f"{'Site':<15} {'Search':<8} {'Listings':<10} {'Scrape':<8} {'Data':<8} {'Save':<8}")
        print(f"{'-'*15} {'-'*8} {'-'*10} {'-'*8} {'-'*8} {'-'*8}")
        
        for site, result in self.results.items():
            print(f"{site:<15} "
                  f"{'✓' if result['search_page'] else '✗':<8} "
                  f"{result['listings_found']:<10} "
                  f"{'✓' if result['listing_page'] else '✗':<8} "
                  f"{'✓' if result['data_extracted'] else '✗':<8} "
                  f"{'✓' if result['saved_to_db'] else '✗':<8}")
            
            if result['errors']:
                for error in result['errors']:
                    print(f"  └─ Error: {error[:60]}...")
        
        # Database summary
        print(f"\n\nDatabase Summary:")
        session = get_session()
        
        for site in self.results.keys():
            count = session.query(Business).filter_by(source_site=site).count()
            print(f"  {site}: {count} listings")
        
        total = session.query(Business).count()
        print(f"  Total: {total} listings")
        session.close()

def main():
    tester = ScraperTester()
    
    # Test each scraper
    scrapers = [
        ('BizBuySell', BizBuySellScraper),
        ('BizQuest', BizQuestScraper),
        ('Flippa', FlippaScraper)
    ]
    
    for site_name, scraper_class in scrapers:
        tester.test_site(site_name, scraper_class)
        time.sleep(2)  # Be respectful between sites
    
    # Print summary
    tester.print_summary()
    
    print(f"\n\nTest completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    main()