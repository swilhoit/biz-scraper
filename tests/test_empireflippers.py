from scrapers.empireflippers_scraper import EmpireFlippersScraper
from config.settings import SITES

# Get EmpireFlippers config
ef_config = next(site for site in SITES if site['name'] == 'EmpireFlippers')
scraper = EmpireFlippersScraper(ef_config)

# Get a listing URL from search
print("Getting listing URLs from search page...")
search_url = ef_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

if listing_urls:
    print(f"Found {len(listing_urls)} listings")
    
    # Test first listing
    test_url = listing_urls[0]
    print(f"\nTesting listing: {test_url}")
    
    soup = scraper.get_page(test_url)
    if soup:
        print("Page fetched successfully")
        
        # Check for title
        h1 = soup.find('h1')
        if h1:
            print(f"\nTitle (h1): {h1.text.strip()}")
        
        # Look for metrics section
        metrics = soup.select_one('div.metrics-wrapper')
        if metrics:
            print("\nFound metrics-wrapper")
            items = metrics.select('div.metric-item')
            print(f"Found {len(items)} metric items")
        else:
            print("\nNo metrics-wrapper found")
            
            # Look for any divs with financial data
            print("\nSearching for financial data patterns...")
            
            # Method 1: Look for text containing price/revenue
            price_elements = soup.find_all(text=lambda t: t and '$' in t)
            if price_elements:
                print(f"\nFound {len(price_elements)} elements with '$':")
                for elem in price_elements[:5]:
                    parent = elem.parent
                    if parent:
                        print(f"  {parent.name}: {elem.strip()[:60]}")
            
            # Method 2: Look for specific class patterns
            selectors = [
                'div[class*="price"]',
                'div[class*="metric"]',
                'div[class*="revenue"]',
                'div[class*="profit"]',
                'span[class*="price"]',
                'div.listing-stats',
                'div.listing-details',
                'dl.stats'
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    print(f"\nFound {len(elements)} elements with selector '{selector}'")
                    elem = elements[0]
                    print(f"  Content: {elem.text.strip()[:100]}")
                    break
        
        # Save HTML sample
        with open('empireflippers_listing.html', 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()[:30000]))
        print("\nSaved first 30000 chars to empireflippers_listing.html")
        
else:
    print("No listing URLs found")