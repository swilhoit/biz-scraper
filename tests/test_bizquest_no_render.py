import requests
from bs4 import BeautifulSoup
from config import SCRAPER_API_URL, SCRAPER_API_PARAMS

# Test URL
url = "https://www.bizquest.com/business-for-sale/thriving-amazon-cleaning-products-store-886k-sales-124k-profit/BW2384670/"

print(f"Fetching without render: {url}")

# Use ScraperAPI without render
params = SCRAPER_API_PARAMS.copy()
params['url'] = url
# No render parameter

try:
    response = requests.get(SCRAPER_API_URL, params=params, timeout=60)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'lxml')
    print("Page fetched successfully")
    
    # Look for the main content
    # Try different selectors
    selectors_to_test = {
        'h1': 'Main heading',
        'h2.business-title': 'Business title',
        '.listing-header': 'Listing header',
        '.price': 'Price',
        '.asking-price': 'Asking price',
        '[class*="price"]': 'Any price class',
        '.business-details': 'Business details',
        '.listing-details': 'Listing details',
        'table': 'Tables',
        '.description': 'Description'
    }
    
    for selector, description in selectors_to_test.items():
        elements = soup.select(selector)
        if elements:
            print(f"\n{description} ({selector}): Found {len(elements)}")
            elem = elements[0]
            text = elem.text.strip()[:100] if elem.text else "No text"
            print(f"  Sample: {text}")
    
    # Look for meta tags which often have structured data
    print("\nMeta tags:")
    meta_tags = soup.find_all('meta', attrs={'property': True})
    for meta in meta_tags[:10]:
        prop = meta.get('property')
        content = meta.get('content', '')[:100]
        if any(keyword in prop.lower() for keyword in ['price', 'title', 'description']):
            print(f"  {prop}: {content}")
    
    # Save HTML
    with open('bizquest_no_render.html', 'w', encoding='utf-8') as f:
        f.write(str(soup))
    print("\nSaved HTML to bizquest_no_render.html")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()