from scrapers.acquire_scraper import AcquireScraper
from config.settings import SITES

# Find Acquire config
acquire_config = next(site for site in SITES if site['name'] == 'Acquire')
print(f"Testing Acquire with URL: {acquire_config['search_url']}")

scraper = AcquireScraper(acquire_config)

# Get the page and check if __NEXT_DATA__ exists
soup = scraper.get_page(acquire_config['search_url'])
if soup:
    print("Page fetched successfully")
    
    # Check for __NEXT_DATA__
    next_data = soup.find('script', {'id': '__NEXT_DATA__'})
    if next_data:
        print("Found __NEXT_DATA__ script")
        print(f"Script content length: {len(next_data.string)}")
        # Print first 500 chars to see structure
        print("First 500 chars:", next_data.string[:500])
    else:
        print("No __NEXT_DATA__ found")
        
        # Check what scripts are available
        scripts = soup.find_all('script')
        print(f"\nFound {len(scripts)} script tags")
        for i, script in enumerate(scripts[:5]):
            if script.get('id'):
                print(f"Script {i}: id='{script.get('id')}'")
            elif script.get('src'):
                print(f"Script {i}: src='{script.get('src')}'")
            else:
                content = str(script.string)[:100] if script.string else "No content"
                print(f"Script {i}: {content}")
else:
    print("Failed to fetch page")