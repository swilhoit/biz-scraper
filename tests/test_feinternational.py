from scrapers.feinternational_scraper import FEInternationalScraper
from config.settings import SITES

# Find FEInternational config
fe_config = next(site for site in SITES if site['name'] == 'FEInternational')
print(f"Testing FEInternational with URL: {fe_config['search_url']}")

scraper = FEInternationalScraper(fe_config)

# Test the page
soup = scraper.get_page(fe_config['search_url'])
if soup:
    print("Page fetched successfully")
    
    # Check current selector
    listing_cards = soup.select('a.card_businesses_item')
    print(f"Found {len(listing_cards)} listings with selector 'a.card_businesses_item'")
    
    # Try alternative selectors
    selectors = [
        'div.listing-card a',
        'article a',
        'a[href*="/businesses/"]',
        'a[href*="/listing"]',
        'div[class*="card"] a',
        'div[class*="listing"] a',
        'div.business-card a',
        'a.listing-link'
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        if elements:
            print(f"\nFound {len(elements)} elements with selector: '{selector}'")
            # Check if they're actual listing links
            listing_count = 0
            for elem in elements[:5]:
                href = elem.get('href', '')
                if '/businesses/' in href or '/listing' in href or 'business-for-sale' in href:
                    listing_count += 1
                    print(f"  - {href}")
            if listing_count > 0:
                print(f"  {listing_count} appear to be listing links")
    
    # Save HTML sample
    with open('feinternational_sample.html', 'w', encoding='utf-8') as f:
        f.write(str(soup.prettify()[:20000]))
    print("\nSaved first 20000 chars to feinternational_sample.html")
    
else:
    print("Failed to fetch page")