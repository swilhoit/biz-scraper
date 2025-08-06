from scrapers.bizbuysell_scraper import BizBuySellScraper
from config.settings import SITES

# Get BizBuySell config
bbs_config = next(site for site in SITES if site['name'] == 'BizBuySell')
scraper = BizBuySellScraper(bbs_config)

# Get listing URLs
search_url = bbs_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

if listing_urls:
    print(f"Testing {min(3, len(listing_urls))} listings...\n")
    
    for i, url in enumerate(listing_urls[:3]):
        print(f"\n{'='*60}")
        print(f"Listing {i+1}: {url}")
        print('='*60)
        
        result = scraper.scrape_listing(url)
        if result:
            for key, value in result.items():
                if key not in ['description', 'listing_url']:
                    print(f"  {key}: {value}")
        else:
            print("  Failed to scrape listing")
else:
    print("No listings found")