from scrapers.bizquest_scraper import BizQuestScraper
from config.settings import SITES

# Get BizQuest config
bizquest_config = next(site for site in SITES if site['name'] == 'BizQuest')
scraper = BizQuestScraper(bizquest_config)

# Test URL from the data
test_url = "https://www.bizquest.com/business-for-sale/thriving-amazon-cleaning-products-store-886k-sales-124k-profit/BW2384670/"
print(f"Testing BizQuest listing: {test_url}")
print("="*80)

# Get the page
soup = scraper.get_page(test_url, render=True)
if soup:
    print("Page fetched successfully")
    
    # Test current selectors
    print("\nTesting current selectors:")
    
    # Title
    title_tag = soup.select_one('h1.font-h1-new')
    if title_tag:
        print(f"Title (h1.font-h1-new): {title_tag.text.strip()}")
    else:
        print("Title (h1.font-h1-new): NOT FOUND")
        # Try alternative selectors
        h1 = soup.find('h1')
        if h1:
            print(f"Title (h1): {h1.text.strip()}")
    
    # Description
    desc_tag = soup.select_one('div.business-description')
    if desc_tag:
        print(f"\nDescription found: {desc_tag.text.strip()[:100]}...")
    else:
        print("\nDescription (div.business-description): NOT FOUND")
    
    # Details table
    details_table = soup.select_one('table.table-striped')
    if details_table:
        print("\nDetails table found")
    else:
        print("\nDetails table (table.table-striped): NOT FOUND")
        # Look for any tables
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables on page")
    
    # Look for price/financial info in different ways
    print("\nSearching for financial information...")
    
    # Method 1: Look for text patterns
    page_text = soup.get_text()
    
    import re
    price_patterns = [
        r'asking\s*price[:\s]*\$?([\d,]+)',
        r'price[:\s]*\$?([\d,]+)',
        r'\$?([\d,]+)\s*asking'
    ]
    
    for pattern in price_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            print(f"Price found with pattern '{pattern}': ${match.group(1)}")
            break
    
    # Method 2: Look for specific elements
    print("\nLooking for listing details sections...")
    
    # Common section classes/ids
    sections = soup.find_all(['div', 'section'], class_=re.compile('details|info|summary|overview', re.I))
    print(f"Found {len(sections)} potential detail sections")
    
    # Save HTML for manual inspection
    with open('bizquest_listing_sample.html', 'w', encoding='utf-8') as f:
        f.write(str(soup.prettify()))
    print("\nSaved full HTML to bizquest_listing_sample.html for inspection")
    
else:
    print("Failed to fetch page")