from scrapers.bizquest_scraper import BizQuestScraper
from config.settings import SITES

# Find BizQuest config
bizquest_config = next(site for site in SITES if site['name'] == 'BizQuest')
print(f"Testing BizQuest with URL: {bizquest_config['amazon_url']}")

scraper = BizQuestScraper(bizquest_config)

# Test the page with rendering
url = f"{bizquest_config['amazon_url']}?page=1"
print(f"Fetching: {url}")

soup = scraper.get_page(url, render=True)
if soup:
    print("Page fetched successfully")
    
    # Check current selector
    listings = soup.select('div.search-result-card-container a')
    print(f"Found {len(listings)} listings with current selector 'div.search-result-card-container a'")
    
    # Try alternative selectors
    selectors = [
        'a[href*="/business-for-sale/"]',
        'div.listing-card a',
        'div.result a[href*="/business"]',
        'div[class*="listing"] a',
        'div[class*="result"] a',
        'article a',
        'a.listing-link',
        'div.businesses-list a'
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        if elements:
            print(f"\nFound {len(elements)} elements with selector: '{selector}'")
            # Show first few hrefs
            for i, elem in enumerate(elements[:3]):
                href = elem.get('href', '')
                if '/business-for-sale/' in href:
                    print(f"  - {href}")
    
    # Save HTML for inspection
    with open('bizquest_sample.html', 'w', encoding='utf-8') as f:
        f.write(str(soup.prettify()[:20000]))
    print("\nSaved first 20000 chars to bizquest_sample.html")
    
else:
    print("Failed to fetch page")