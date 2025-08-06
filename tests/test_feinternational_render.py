from scrapers.feinternational_scraper import FEInternationalScraper
from config.settings import SITES

# Find FEInternational config
fe_config = next(site for site in SITES if site['name'] == 'FEInternational')
print(f"Testing FEInternational with URL: {fe_config['search_url']}")

scraper = FEInternationalScraper(fe_config)

# Test with rendering
print("Fetching page with rendering...")
soup = scraper.get_page(fe_config['search_url'], render=True)
if soup:
    print("Page fetched successfully with rendering")
    
    # Look for any links that might be listings
    all_links = soup.find_all('a', href=True)
    listing_links = []
    
    for link in all_links:
        href = link['href']
        # Common patterns for business listing URLs
        if any(pattern in href.lower() for pattern in ['/business/', '/listing', 'for-sale', '/portfolio/']):
            if href not in listing_links:
                listing_links.append(href)
    
    if listing_links:
        print(f"\nFound {len(listing_links)} potential listing links:")
        for link in listing_links[:10]:
            print(f"  - {link}")
    else:
        print("\nNo listing links found")
    
    # Look for business cards or portfolio items
    selectors = [
        'div.portfolio-item',
        'div.business-listing',
        'div[class*="portfolio"]',
        'div[class*="business"]',
        'article',
        'div.card',
        'a[href*="portfolio"]'
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        if elements:
            print(f"\nFound {len(elements)} elements with selector: '{selector}'")
            break
    
    # Check if there's a "View Portfolio" or similar button
    buttons = soup.find_all(['a', 'button'], string=lambda text: text and any(word in text.lower() for word in ['portfolio', 'listings', 'businesses', 'view all']))
    if buttons:
        print(f"\nFound {len(buttons)} relevant buttons/links:")
        for btn in buttons[:3]:
            text = btn.get_text(strip=True)
            href = btn.get('href', 'No href')
            print(f"  - '{text}' -> {href}")
    
else:
    print("Failed to fetch page")