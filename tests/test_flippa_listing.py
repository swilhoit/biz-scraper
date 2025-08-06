from scrapers.flippa_scraper import FlippaScraper
from config.settings import SITES
import re

# Get Flippa config
flippa_config = next(site for site in SITES if site['name'] == 'Flippa')
scraper = FlippaScraper(flippa_config)

# Get a listing URL
search_url = flippa_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

if listing_urls:
    test_url = listing_urls[0]
    print(f"Testing: {test_url}")
    
    # Get the page with rendering
    soup = scraper.get_page(test_url, render=True)
    if soup:
        print("Page fetched successfully")
        
        # Look for financial data
        page_text = soup.get_text()
        
        # Find all price values
        prices = re.findall(r'\$[\d,]+(?:\.\d{2})?', page_text)
        if prices:
            print(f"\nFound {len(prices)} price values:")
            for i, price in enumerate(prices[:10]):
                print(f"  {i+1}. {price}")
        
        # Look for specific patterns
        patterns = [
            (r'(?:Buy It Now|Price|Asking Price)[:\s]*(\$[\d,]+)', 'Price'),
            (r'(?:Monthly Revenue|Revenue)[:\s]*(\$[\d,]+)', 'Revenue'),
            (r'(?:Monthly Profit|Net Profit|Profit)[:\s]*(\$[\d,]+)', 'Profit'),
            (r'(?:Annual Revenue)[:\s]*(\$[\d,]+)', 'Annual Revenue'),
        ]
        
        print("\nPattern matches:")
        for pattern, name in patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                print(f"  {name}: {match.group(1)}")
        
        # Look for data in specific elements
        selectors = [
            'span[class*="price"]',
            'div[class*="price"]',
            'div[class*="metric"]',
            'dl.metrics',
            'div.listing-stats',
            'table.stats'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                print(f"\nFound {len(elements)} elements with selector '{selector}'")
                elem = elements[0]
                print(f"Content: {elem.text.strip()[:100]}")
                break
        
        # Save HTML
        with open('flippa_listing.html', 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()[:30000]))
        print("\nSaved HTML to flippa_listing.html")
        
else:
    print("No listing URLs found")