from scrapers.bizbuysell_scraper import BizBuySellScraper
from config.settings import SITES
import re

# Get BizBuySell config
bbs_config = next(site for site in SITES if site['name'] == 'BizBuySell')
scraper = BizBuySellScraper(bbs_config)

# Get a listing URL
search_url = bbs_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

if listing_urls:
    test_url = listing_urls[0]
    print(f"Testing: {test_url}")
    
    # Get the page with rendering
    soup = scraper.get_page(test_url, render=True)
    if soup:
        print("Page fetched successfully\n")
        
        # Debug: Look for financial data structure
        print("=== LOOKING FOR FINANCIAL DATA ===")
        
        # Check various selectors
        selectors_to_check = [
            ('div.asking-price', 'Asking Price Div'),
            ('span.asking-price', 'Asking Price Span'),
            ('dl.financials', 'Financials DL'),
            ('div.financials', 'Financials Div'),
            ('div.financial-info', 'Financial Info'),
            ('div[class*="financ"]', 'Any Financ* class'),
            ('dl[class*="financ"]', 'DL Financ* class'),
            ('div.listing-financials', 'Listing Financials'),
            ('section.financials', 'Section Financials')
        ]
        
        for selector, name in selectors_to_check:
            elements = soup.select(selector)
            if elements:
                print(f"\nFound {len(elements)} {name}:")
                for elem in elements[:2]:
                    print(f"  Content: {elem.text.strip()[:200]}")
                    if elem.get('class'):
                        print(f"  Classes: {elem.get('class')}")
        
        # Look for specific text patterns
        print("\n=== SEARCHING FOR FINANCIAL PATTERNS ===")
        page_text = soup.get_text()
        
        # Search for the values from the screenshot
        patterns = [
            (r'Asking Price[:\s]*\$?([\d,]+)', 'Asking Price'),
            (r'Cash Flow.*?\$?([\d,]+)', 'Cash Flow'),
            (r'Gross Revenue[:\s]*\$?([\d,]+)', 'Gross Revenue'),
            (r'EBITDA[:\s]*([^\n]+)', 'EBITDA'),
            (r'Established[:\s]*(\d{4})', 'Established Year'),
            (r'SDE.*?\$?([\d,]+)', 'SDE')
        ]
        
        for pattern, name in patterns:
            match = re.search(pattern, page_text, re.I | re.S)
            if match:
                print(f"\n{name}: {match.group(0)}")
                context_start = max(0, match.start() - 30)
                context_end = min(len(page_text), match.end() + 30)
                context = page_text[context_start:context_end].replace('\n', ' ')
                print(f"  Context: ...{context}...")
        
        # Look for dt/dd patterns which are common in BizBuySell
        print("\n=== CHECKING DT/DD PATTERNS ===")
        dts = soup.find_all('dt')
        for dt in dts[:10]:
            dd = dt.find_next_sibling('dd')
            if dd:
                label = dt.text.strip()
                value = dd.text.strip()
                if any(keyword in label.lower() for keyword in ['price', 'revenue', 'cash', 'flow', 'ebitda']):
                    print(f"{label}: {value}")
        
        # Save HTML for inspection
        with open('bizbuysell_debug.html', 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()))
        print("\n\nSaved HTML to bizbuysell_debug.html")
        
        # Try the scraper method
        print("\n=== SCRAPER RESULT ===")
        result = scraper.scrape_listing(test_url)
        if result:
            for key, value in result.items():
                if key != 'description':
                    print(f"{key}: {value}")
else:
    print("No listing URLs found")