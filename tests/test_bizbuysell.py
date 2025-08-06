from scrapers.bizbuysell_scraper import BizBuySellScraper
from config.settings import SITES
import re

# Get BizBuySell config
bbs_config = next(site for site in SITES if site['name'] == 'BizBuySell')
scraper = BizBuySellScraper(bbs_config)

# Get a listing
search_url = bbs_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

if listing_urls:
    test_url = listing_urls[0]
    print(f"Testing: {test_url}")
    
    soup = scraper.get_page(test_url)
    if soup:
        print("Page fetched successfully")
        
        # Check JSON-LD
        json_ld = soup.find('script', {'type': 'application/ld+json'})
        if json_ld:
            print("\nFound JSON-LD data")
            import json
            try:
                data = json.loads(json_ld.string)
                print(f"JSON-LD keys: {list(data.keys())}")
                if 'offers' in data:
                    print(f"Offers: {data['offers']}")
            except:
                pass
        
        # Check for financials section
        financials = soup.select('div.financials-desktop__wrapper--item')
        print(f"\nFound {len(financials)} financial items with current selector")
        
        if not financials:
            # Try alternative selectors
            selectors = [
                'div[class*="financial"]',
                'div[class*="revenue"]',
                'div[class*="cash-flow"]',
                'dl.financials',
                'div.listing-financials',
                'table.financials'
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    print(f"\nFound {len(elements)} elements with selector '{selector}'")
                    elem = elements[0]
                    print(f"Content: {elem.text.strip()[:200]}")
                    break
        
        # Look for text patterns
        page_text = soup.get_text()
        
        # Revenue patterns
        revenue_patterns = [
            r'(?:Gross Revenue|Revenue|Annual Revenue)[:\s]*\$?([\d,]+)',
            r'(?:Sales|Annual Sales)[:\s]*\$?([\d,]+)',
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                print(f"\nRevenue found with pattern '{pattern}': ${match.group(1)}")
                break
        
        # Cash flow patterns
        cf_patterns = [
            r'(?:Cash Flow|Net Income|EBITDA|Profit)[:\s]*\$?([\d,]+)',
            r'(?:Annual Cash Flow|Annual Profit)[:\s]*\$?([\d,]+)',
        ]
        
        for pattern in cf_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                print(f"Cash Flow found with pattern '{pattern}': ${match.group(1)}")
                break
        
        # Save HTML sample
        with open('bizbuysell_listing.html', 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()[:50000]))
        print("\nSaved first 50000 chars to bizbuysell_listing.html")