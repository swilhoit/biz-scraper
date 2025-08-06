from scrapers.bizbuysell_scraper import BizBuySellScraper
from config.settings import SITES
import re

# Get BizBuySell config
bbs_config = next(site for site in SITES if site['name'] == 'BizBuySell')
scraper = BizBuySellScraper(bbs_config)

# Get search page
search_url = f"{bbs_config['amazon_url']}1/"
print(f"Testing search page: {search_url}")

soup = scraper.get_page(search_url)
if soup:
    print("Search page fetched successfully")
    
    # Look for listing containers
    listings = soup.select('div.search-result-card')
    print(f"\nFound {len(listings)} listing cards")
    
    if listings:
        # Analyze first listing
        listing = listings[0]
        text = listing.get_text(separator=' ', strip=True)
        print(f"\nFirst listing text:\n{text[:500]}")
        
        # Look for financial data in listing
        if '$' in text:
            prices = re.findall(r'\$[\d,]+', text)
            print(f"\nFound prices in listing: {prices}")
        
        # Check if revenue/cash flow are shown
        if 'revenue' in text.lower() or 'cash flow' in text.lower():
            print("\nFinancial keywords found in search results")
        else:
            print("\nNo financial keywords in search results")
    
    # Check JSON-LD on search page
    json_ld = soup.find('script', {'type': 'application/ld+json'})
    if json_ld:
        print("\nFound JSON-LD on search page")
        try:
            import json
            data = json.loads(json_ld.string)
            if 'about' in data and data['about']:
                item = data['about'][0]
                print(f"First item keys: {list(item.keys())}")
                if 'item' in item:
                    print(f"Item data: {item['item']}")
        except Exception as e:
            print(f"Error parsing JSON-LD: {e}")
            
else:
    print("Failed to fetch search page")