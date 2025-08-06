from scrapers import BizQuestScraper
from config.settings import SITES

# Get BizQuest config
bizquest_config = next(site for site in SITES if site['name'] == 'BizQuest')

print("Testing updated BizQuest scraper...")
print("="*60)

scraper = BizQuestScraper(bizquest_config)

# Get listing URLs (this will also cache data)
search_url = bizquest_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

print(f"\nFound {len(listing_urls)} listings")

# Test scraping first 3 listings
for i, url in enumerate(listing_urls[:3], 1):
    print(f"\n--- Listing {i} ---")
    data = scraper.scrape_listing(url)
    
    if data:
        print(f"URL: {url}")
        print(f"Title: {data.get('title', 'MISSING')}")
        print(f"Price: ${data.get('price', 0):,.0f}" if data.get('price') else "Price: MISSING")
        print(f"Cash Flow: ${data.get('cash_flow', 0):,.0f}" if data.get('cash_flow') else "Cash Flow: MISSING")
        print(f"Location: {data.get('location', 'MISSING')}")
        print(f"Description: {data.get('description', 'MISSING')[:100]}...")
    else:
        print(f"Failed to scrape: {url}")