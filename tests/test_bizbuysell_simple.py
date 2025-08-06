from scrapers.bizbuysell_scraper import BizBuySellScraper
from config.settings import SITES
import re

# Get BizBuySell config
bbs_config = next(site for site in SITES if site['name'] == 'BizBuySell')
scraper = BizBuySellScraper(bbs_config)

# Test a specific listing - using one that might have the data shown in screenshot
test_url = "https://www.bizbuysell.com/Business-Opportunity/established-liquor-store-in-broward-county/2507424/"
print(f"Testing: {test_url}")

# Get the page WITHOUT rendering first to see raw HTML
soup = scraper.get_page(test_url, render=False)
if soup:
    print("Page fetched successfully (no render)\n")
    
    # Look for financial data patterns in the HTML
    html_str = str(soup)
    
    # Check if we can find the financial values
    if "$135,000" in html_str:
        print("✓ Found asking price $135,000 in HTML")
    if "$88,000" in html_str:
        print("✓ Found cash flow $88,000 in HTML")
    if "$285,000" in html_str:
        print("✓ Found revenue $285,000 in HTML")
    
    # Look for the structure
    print("\n=== CHECKING STRUCTURE ===")
    
    # Check for dt/dd pairs
    dts = soup.find_all('dt')
    print(f"Found {len(dts)} dt elements")
    
    for dt in dts[:20]:
        dd = dt.find_next_sibling('dd')
        if dd:
            label = dt.text.strip()
            value = dd.text.strip()[:100]  # Truncate long values
            print(f"  {label}: {value}")
    
    # Save a snippet
    with open('bizbuysell_snippet.html', 'w', encoding='utf-8') as f:
        # Find the financial section
        financial_section = soup.find('dl', class_='financials') or soup.find('section', class_='financials')
        if financial_section:
            f.write(str(financial_section.prettify()))
            print("\nSaved financial section to bizbuysell_snippet.html")
        else:
            # Try to find any dl element
            any_dl = soup.find('dl')
            if any_dl:
                f.write(str(any_dl.prettify()))
                print("\nSaved first dl element to bizbuysell_snippet.html")
else:
    print("Failed to fetch page")