from scrapers.flippa_scraper import FlippaScraper
from config.settings import SITES
import json

# Find Flippa config
flippa_config = next(site for site in SITES if site['name'] == 'Flippa')
print(f"Testing Flippa with URL: {flippa_config['amazon_url']}")

scraper = FlippaScraper(flippa_config)

# Test the page
url = f"{flippa_config['amazon_url']}&page=1"
print(f"Fetching: {url}")

soup = scraper.get_page(url)
if soup:
    print("Page fetched successfully")
    
    # Check for __NEXT_DATA__
    json_script = soup.find('script', {'id': '__NEXT_DATA__'})
    if json_script:
        print("Found __NEXT_DATA__")
        try:
            data = json.loads(json_script.string)
            print(f"JSON parsed successfully")
            # Navigate the structure
            props = data.get('props', {})
            pageProps = props.get('pageProps', {})
            print(f"Keys in pageProps: {list(pageProps.keys())}")
            
            # Check different possible paths
            if 'data' in pageProps:
                data_keys = list(pageProps['data'].keys())
                print(f"Keys in pageProps.data: {data_keys}")
            
        except Exception as e:
            print(f"Error parsing JSON: {e}")
    else:
        print("No __NEXT_DATA__ found")
    
    # Try with rendering
    print("\nTrying with rendering enabled...")
    soup = scraper.get_page(url, render=True)
    if soup:
        # Look for listing elements
        selectors = [
            'a[href*="/buy/"]',
            'div[class*="listing"] a',
            'div[class*="card"] a', 
            'article a',
            'a[class*="listing"]'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                print(f"\nFound {len(elements)} elements with selector: '{selector}'")
                # Show first few
                for i, elem in enumerate(elements[:3]):
                    href = elem.get('href', '')
                    if '/buy/' in href and len(href) > 10:
                        print(f"  - {href}")
                        
        # Save HTML sample
        with open('flippa_sample.html', 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()[:20000]))
        print("\nSaved first 20000 chars to flippa_sample.html")
else:
    print("Failed to fetch page")