from scrapers.flippa_scraper import FlippaScraper
from config.settings import SITES

# Find Flippa config
flippa_config = next(site for site in SITES if site['name'] == 'Flippa')
scraper = FlippaScraper(flippa_config)

# Test with rendering to see actual listing structure
url = flippa_config['amazon_url']
print(f"Fetching {url} with rendering...")

soup = scraper.get_page(url, render=True)
if soup:
    # Look for listing cards/items
    print("Searching for listing containers...")
    
    # Common patterns for listing containers
    containers = [
        'div.ListingCard',
        'div[class*="listing-card"]',
        'article.listing',
        'div.listing-item',
        'a.ListingCard__anchor',
        'div[data-testid*="listing"]'
    ]
    
    for selector in containers:
        elements = soup.select(selector)
        if elements:
            print(f"\nFound {len(elements)} elements with selector: '{selector}'")
            elem = elements[0]
            
            # Try to find the link within the element
            link = elem if elem.name == 'a' else elem.find('a')
            if link and link.get('href'):
                print(f"Sample link: {link['href']}")
                
            # Look for title/price
            title = elem.find(text=True, recursive=True)
            print(f"Sample text: {str(title)[:100] if title else 'No text'}")
            break
    
    # Also check for any data attributes that might contain listing info
    data_attrs = soup.find_all(attrs={"data-listing-id": True})
    if data_attrs:
        print(f"\nFound {len(data_attrs)} elements with data-listing-id")
        
else:
    print("Failed to fetch page")