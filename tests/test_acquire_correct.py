from scrapers.acquire_scraper import AcquireScraper
from config.settings import SITES

# Find Acquire config
acquire_config = next(site for site in SITES if site['name'] == 'Acquire')
print(f"Testing Acquire with URL: {acquire_config['search_url']}")

scraper = AcquireScraper(acquire_config)

# Get the page
soup = scraper.get_page(acquire_config['search_url'])
if soup:
    print("Page fetched successfully")
    
    # Check for __NEXT_DATA__ (original)
    next_data = soup.find('script', {'id': '__NEXT_DATA__'})
    if next_data:
        print("Found __NEXT_DATA__ script")
        print(f"Script content length: {len(next_data.string)}")
    else:
        print("No __NEXT_DATA__ found")
    
    # Check for __NUXT_DATA__
    nuxt_data = soup.find('script', {'id': '__NUXT_DATA__'})
    if nuxt_data:
        print("Found __NUXT_DATA__ script")
    
    # Check what scripts are available
    scripts = soup.find_all('script')
    print(f"\nFound {len(scripts)} script tags")
    for i, script in enumerate(scripts[:10]):
        if script.get('id'):
            print(f"Script {i}: id='{script.get('id')}'")
        elif script.get('src'):
            src = script.get('src')
            if 'acquire' in src.lower() or 'next' in src.lower():
                print(f"Script {i}: src='{src}'")
            
    # Look for listing elements
    print("\nLooking for listing elements...")
    selectors = [
        'a[href*="/startups/"]',
        'div[class*="listing"]',
        'div[class*="startup"]',
        'article',
        'div[data-testid*="listing"]'
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        if elements:
            print(f"Found {len(elements)} elements with selector: {selector}")
            for elem in elements[:3]:
                href = elem.get('href') or elem.find('a', href=True)
                if href:
                    print(f"  - {href if isinstance(href, str) else href.get('href')}")
            
else:
    print("Failed to fetch page")