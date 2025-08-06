from scrapers.acquire_scraper import AcquireScraper
from config.settings import SITES

# Find Acquire config
acquire_config = next(site for site in SITES if site['name'] == 'Acquire')
print(f"Testing Acquire with URL: {acquire_config['search_url']}")

scraper = AcquireScraper(acquire_config)

# Get the page with render=True
print("Fetching page with JavaScript rendering...")
soup = scraper.get_page(acquire_config['search_url'], render=True)
if soup:
    print("Page fetched successfully with rendering")
    
    # Look for any links that might be startup listings
    all_links = soup.find_all('a', href=True)
    startup_links = []
    
    for link in all_links:
        href = link['href']
        # Common patterns for startup/business listing URLs
        if any(pattern in href for pattern in ['/startups/', '/startup/', '/listing/', '/business/', '/company/']):
            startup_links.append(href)
    
    if startup_links:
        print(f"\nFound {len(startup_links)} potential startup listing links:")
        # Remove duplicates and show first 10
        unique_links = list(set(startup_links))
        for link in unique_links[:10]:
            print(f"  - {link}")
    else:
        print("\nNo startup listing links found")
    
    # Also try to find listing containers
    print("\nSearching for listing containers...")
    
    # Try finding by text content
    elements_with_revenue = soup.find_all(text=lambda text: text and ('revenue' in text.lower() or 'mrr' in text.lower() or '$' in text))
    if elements_with_revenue:
        print(f"Found {len(elements_with_revenue)} elements mentioning revenue/pricing")
        
    # Save a sample of the HTML for inspection
    with open('acquire_sample.html', 'w', encoding='utf-8') as f:
        f.write(str(soup.prettify()[:10000]))
    print("\nSaved first 10000 chars of HTML to acquire_sample.html for inspection")
    
else:
    print("Failed to fetch page")