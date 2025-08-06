from scrapers.flippa_scraper import FlippaScraper
from config.settings import SITES
import re

# Find Flippa config
flippa_config = next(site for site in SITES if site['name'] == 'Flippa')
scraper = FlippaScraper(flippa_config)

# Use the general search URL instead
url = flippa_config['search_url']
print(f"Fetching {url} with rendering...")

soup = scraper.get_page(url, render=True)
if soup:
    print("Page fetched successfully")
    
    # Look for any links that seem like listings
    all_links = soup.find_all('a', href=True)
    listing_links = []
    
    for link in all_links:
        href = link['href']
        # Flippa listing URLs typically contain a numeric ID
        if re.search(r'/\d{6,}', href) or '/listings/' in href:
            if href.startswith('/'):
                href = 'https://flippa.com' + href
            if href not in listing_links and 'flippa.com' in href:
                listing_links.append(href)
    
    if listing_links:
        print(f"\nFound {len(listing_links)} potential listing links:")
        for link in listing_links[:10]:
            print(f"  - {link}")
    else:
        print("No listing links found")
        
    # Try to find listing containers by looking for price elements
    price_elements = soup.find_all(text=re.compile(r'\$[\d,]+'))
    if price_elements:
        print(f"\nFound {len(price_elements)} price elements")
        # Get parent containers
        containers = set()
        for price in price_elements[:5]:
            parent = price.parent
            while parent and parent.name not in ['body', 'html']:
                if parent.find('a', href=True):
                    containers.add(parent)
                    break
                parent = parent.parent
        
        if containers:
            print(f"Found {len(containers)} unique containers with prices and links")
            
else:
    print("Failed to fetch page")