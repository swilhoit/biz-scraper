from scrapers.acquire_scraper import AcquireScraper
from config.settings import SITES
import json

# Find Acquire config
acquire_config = next(site for site in SITES if site['name'] == 'Acquire')
print(f"Testing Acquire with URL: {acquire_config['search_url']}")

scraper = AcquireScraper(acquire_config)

# Get the page and check if __NUXT_DATA__ exists
soup = scraper.get_page(acquire_config['search_url'])
if soup:
    print("Page fetched successfully")
    
    # Check for __NUXT_DATA__
    nuxt_data = soup.find('script', {'id': '__NUXT_DATA__'})
    if nuxt_data:
        print("Found __NUXT_DATA__ script")
        content = nuxt_data.string
        print(f"Script content length: {len(content)}")
        
        # Nuxt data is often in a special format, let's see what it looks like
        print("\nFirst 1000 chars:", content[:1000])
        
        # Try to parse it - Nuxt data is often an array
        try:
            # Remove any trailing/leading whitespace
            content = content.strip()
            # Nuxt often uses a special format, let's try to parse it
            data = json.loads(content)
            print(f"\nParsed data type: {type(data)}")
            if isinstance(data, list):
                print(f"Array length: {len(data)}")
                # Print first few items
                for i, item in enumerate(data[:10]):
                    print(f"Item {i}: {str(item)[:100]}")
        except Exception as e:
            print(f"\nCould not parse as JSON: {e}")
    else:
        print("No __NUXT_DATA__ found")
else:
    print("Failed to fetch page")