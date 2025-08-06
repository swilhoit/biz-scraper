from scrapers.feinternational_scraper import FEInternationalScraper
from config.settings import SITES

# Find FEInternational config
fe_config = next(site for site in SITES if site['name'] == 'FEInternational')
scraper = FEInternationalScraper(fe_config)

# Try different potential URLs
test_urls = [
    'https://feinternational.com/portfolio/',
    'https://feinternational.com/businesses-for-sale/',
    'https://feinternational.com/current-listings/',
    'https://feinternational.com/buy-online-business/'
]

for url in test_urls:
    print(f"\nTrying: {url}")
    soup = scraper.get_page(url, render=True)
    
    if soup:
        # Check if it's a 404 or redirect
        title = soup.find('title')
        if title and ('404' in title.text or 'not found' in title.text.lower()):
            print("  -> 404 page")
            continue
            
        # Look for business listings
        all_links = soup.find_all('a', href=True)
        business_links = []
        
        for link in all_links:
            href = link['href']
            text = link.get_text(strip=True)
            
            # Look for patterns that indicate a business listing
            if any(pattern in href.lower() for pattern in ['/portfolio/', '/business/', 'listing']) and len(text) > 10:
                if href not in business_links and not any(skip in href for skip in ['#', 'javascript:', 'mailto:']):
                    business_links.append((href, text[:50]))
        
        if business_links:
            print(f"  -> Found {len(business_links)} potential listings:")
            for href, text in business_links[:5]:
                print(f"     - {text}: {href}")
        else:
            print("  -> No listings found")
    else:
        print("  -> Failed to fetch")