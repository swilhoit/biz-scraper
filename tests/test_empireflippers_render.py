from scrapers.empireflippers_scraper import EmpireFlippersScraper
from config.settings import SITES
import re

# Get EmpireFlippers config
ef_config = next(site for site in SITES if site['name'] == 'EmpireFlippers')
scraper = EmpireFlippersScraper(ef_config)

# Get a listing URL
search_url = ef_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

if listing_urls:
    test_url = listing_urls[0]
    print(f"Testing: {test_url}")
    
    # Try with rendering
    print("\nFetching with render=True...")
    soup = scraper.get_page(test_url, render=True)
    
    if soup:
        print("Page fetched with rendering")
        
        # Extract all text and look for patterns
        page_text = soup.get_text()
        
        # Look for price patterns
        price_matches = re.findall(r'\$[\d,]+(?:\.\d{2})?', page_text)
        if price_matches:
            print(f"\nFound {len(price_matches)} price values:")
            for i, price in enumerate(price_matches[:10]):
                print(f"  {i+1}. {price}")
        
        # Look for labeled values
        patterns = [
            r'(?:Listing Price|Price)[:\s]*(\$[\d,]+)',
            r'(?:Monthly Revenue|Revenue)[:\s]*(\$[\d,]+)',
            r'(?:Monthly Net Profit|Net Profit|Profit)[:\s]*(\$[\d,]+)',
            r'(?:Multiple)[:\s]*([\d.]+x?)',
        ]
        
        print("\nLooking for labeled financial data:")
        for pattern in patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                print(f"  Pattern '{pattern.split('(')[0]}': {matches}")
        
        # Look for specific containers
        containers = soup.find_all('div', class_=re.compile('stat|metric|price|listing-detail', re.I))
        if containers:
            print(f"\nFound {len(containers)} potential data containers")
            for container in containers[:5]:
                classes = ' '.join(container.get('class', []))
                text = container.text.strip()[:100]
                if '$' in text or any(word in text.lower() for word in ['price', 'revenue', 'profit']):
                    print(f"\nContainer class: {classes}")
                    print(f"Text: {text}")
        
        # Save rendered HTML
        with open('empireflippers_rendered.html', 'w', encoding='utf-8') as f:
            f.write(str(soup))
        print("\nSaved rendered HTML to empireflippers_rendered.html")
        
    else:
        print("Failed to fetch page with rendering")