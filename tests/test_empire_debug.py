from scrapers.empireflippers_scraper import EmpireFlippersScraper
from config.settings import SITES
import re

# Get EmpireFlippers config
empire_config = next(site for site in SITES if site['name'] == 'EmpireFlippers')
scraper = EmpireFlippersScraper(empire_config)

# Test the specific listing from the screenshot
test_url = "https://empireflippers.com/listing/85705/"
print(f"Testing: {test_url}")

# Get the page with rendering
soup = scraper.get_page(test_url, render=True)
if soup:
    print("Page fetched successfully\n")
    
    # Debug: Look for price in various elements
    print("=== LOOKING FOR PRICE DATA ===")
    
    # Check for price in divs with class containing 'price'
    price_divs = soup.find_all('div', class_=lambda x: x and 'price' in x.lower() if x else False)
    for div in price_divs[:5]:
        print(f"Found price div: {div.get('class')} -> {div.text.strip()[:100]}")
    
    # Check spans
    price_spans = soup.find_all('span', class_=lambda x: x and ('price' in x.lower() or 'value' in x.lower()) if x else False)
    for span in price_spans[:5]:
        print(f"Found price span: {span.get('class')} -> {span.text.strip()[:100]}")
    
    # Look for any element containing the exact price from screenshot
    target_price = "649,354"
    elements_with_price = soup.find_all(string=lambda text: target_price in text if text else False)
    print(f"\n=== Elements containing '{target_price}' ===")
    for elem in elements_with_price[:3]:
        parent = elem.parent
        print(f"Found in {parent.name}: {elem.strip()}")
        if parent.parent:
            print(f"  Parent class: {parent.parent.get('class')}")
    
    # Look for financial metrics
    print("\n=== LOOKING FOR FINANCIAL METRICS ===")
    
    # Search for common metric labels
    metric_keywords = ['revenue', 'profit', 'monthly', 'listing price', 'avg']
    page_text = soup.get_text()
    
    for keyword in metric_keywords:
        matches = re.finditer(rf'{keyword}[:\s]*\$?([\d,]+)', page_text, re.I)
        for match in matches:
            context_start = max(0, match.start() - 50)
            context_end = min(len(page_text), match.end() + 50)
            context = page_text[context_start:context_end].replace('\n', ' ')
            print(f"\n{keyword.upper()}: Found '{match.group(0)}' in context:")
            print(f"  ...{context}...")
    
    # Look for structured data
    print("\n=== LOOKING FOR STRUCTURED DATA ===")
    
    # Check for dl/dt/dd patterns
    dls = soup.find_all('dl')
    for dl in dls[:3]:
        print(f"Found dl with {len(dl.find_all('dt'))} items")
        for dt, dd in zip(dl.find_all('dt'), dl.find_all('dd')):
            print(f"  {dt.text.strip()}: {dd.text.strip()}")
    
    # Check for table patterns
    tables = soup.find_all('table')
    for i, table in enumerate(tables[:2]):
        print(f"\nTable {i+1}:")
        rows = table.find_all('tr')[:5]
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if cells:
                print(f"  {' | '.join(cell.text.strip() for cell in cells)}")
    
    # Save HTML for manual inspection
    with open('empire_debug.html', 'w', encoding='utf-8') as f:
        f.write(str(soup.prettify()))
    print("\n\nSaved full HTML to empire_debug.html")
    
    # Also save just the main content area
    main_content = soup.find('main') or soup.find('div', class_='listing-detail')
    if main_content:
        with open('empire_main_content.html', 'w', encoding='utf-8') as f:
            f.write(str(main_content.prettify()))
        print("Saved main content to empire_main_content.html")
else:
    print("Failed to fetch page")