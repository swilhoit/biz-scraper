import requests
from bs4 import BeautifulSoup
from config import SCRAPER_API_URL, SCRAPER_API_PARAMS

# Test URL
url = "https://www.bizquest.com/business-for-sale/thriving-amazon-cleaning-products-store-886k-sales-124k-profit/BW2384670/"

print(f"Fetching: {url}")

# Use ScraperAPI
params = SCRAPER_API_PARAMS.copy()
params['url'] = url
params['render'] = 'true'

try:
    response = requests.get(SCRAPER_API_URL, params=params, timeout=120)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'lxml')
    
    # Look for the main content area
    print("\nSearching for title...")
    title_candidates = [
        soup.find('h1'),
        soup.find('h2', class_='listing-title'),
        soup.find(class_='business-title'),
        soup.find('title')
    ]
    
    for candidate in title_candidates:
        if candidate:
            print(f"Found: {candidate.name} = {candidate.text.strip()[:100]}")
    
    # Look for price/financial data
    print("\nSearching for financial data...")
    
    # Look for any element containing "$"
    price_elements = soup.find_all(text=lambda text: text and '$' in text)
    print(f"Found {len(price_elements)} elements with '$'")
    
    for elem in price_elements[:5]:
        parent = elem.parent
        if parent:
            print(f"  {parent.name}: {elem.strip()[:80]}")
    
    # Look for description
    print("\nSearching for description...")
    desc_candidates = [
        soup.find('div', class_='description'),
        soup.find('div', class_='business-description'),
        soup.find('section', class_='description'),
        soup.find('div', {'id': 'description'})
    ]
    
    for candidate in desc_candidates:
        if candidate:
            print(f"Found description in: {candidate.name}.{candidate.get('class')}")
            print(f"Text: {candidate.text.strip()[:200]}...")
            
    # Save a portion of HTML
    with open('bizquest_test.html', 'w') as f:
        f.write(str(soup.prettify()[:50000]))
    print("\nSaved first 50000 chars to bizquest_test.html")
    
except Exception as e:
    print(f"Error: {e}")