from scrapers.acquire_scraper import AcquireScraper
from config.settings import SITES

# Find Acquire config
acquire_config = next(site for site in SITES if site['name'] == 'Acquire')
print(f"Testing Acquire with URL: {acquire_config['search_url']}")

scraper = AcquireScraper(acquire_config)

# Get the page with render=True to ensure JavaScript executes
soup = scraper.get_page(acquire_config['search_url'], render=True)
if soup:
    print("Page fetched successfully with rendering")
    
    # Look for listing elements - common patterns
    # Try different selectors that might contain listings
    selectors_to_try = [
        'div[class*="listing"]',
        'div[class*="card"]',
        'article',
        'a[href*="/listing"]',
        'a[href*="/marketplace/"]',
        'div[class*="business"]',
        'div[class*="item"]',
        'div[class*="result"]'
    ]
    
    for selector in selectors_to_try:
        elements = soup.select(selector)
        if elements:
            print(f"\nFound {len(elements)} elements with selector: {selector}")
            # Show first few
            for i, elem in enumerate(elements[:3]):
                print(f"  Element {i}: {str(elem)[:200]}")
            break
    
    # Also check for any links that might be listings
    all_links = soup.find_all('a', href=True)
    listing_links = [a for a in all_links if '/listing/' in a['href'] or '/business/' in a['href']]
    if listing_links:
        print(f"\nFound {len(listing_links)} potential listing links:")
        for link in listing_links[:5]:
            print(f"  - {link['href']}")
    
else:
    print("Failed to fetch page")