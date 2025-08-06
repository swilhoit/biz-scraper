from scrapers.empireflippers_scraper import EmpireFlippersScraper
from config.settings import SITES
from bs4 import BeautifulSoup

# Get EmpireFlippers config
ef_config = next(site for site in SITES if site['name'] == 'EmpireFlippers')
scraper = EmpireFlippersScraper(ef_config)

# Get a listing URL
search_url = ef_config['amazon_url']
listing_urls = scraper.get_listing_urls(search_url, max_pages=1)

if listing_urls:
    test_url = listing_urls[0]
    print(f"Testing: {test_url}")
    
    soup = scraper.get_page(test_url)
    if soup:
        # Look for the data structure
        # Try to find the container with price
        price_divs = soup.find_all('div', string=lambda t: t and '$273,960' in t if t else False)
        if price_divs:
            print(f"\nFound price in {len(price_divs)} divs")
            for div in price_divs:
                parent = div.parent
                print(f"\nPrice div parent: {parent.name}")
                if parent.get('class'):
                    print(f"Parent classes: {' '.join(parent.get('class'))}")
                
                # Look for siblings
                siblings = parent.find_all(['div', 'span'])
                print(f"Siblings: {len(siblings)}")
                for sib in siblings[:5]:
                    print(f"  - {sib.name}: {sib.text.strip()[:50]}")
        
        # Look for stat blocks
        stat_blocks = soup.select('div.stats-block, div.stat-block, div.listing-stats')
        if stat_blocks:
            print(f"\nFound {len(stat_blocks)} stat blocks")
            for block in stat_blocks[:2]:
                print(f"\nBlock content:")
                items = block.find_all(['div', 'span', 'p'])
                for item in items[:10]:
                    text = item.text.strip()
                    if text:
                        print(f"  {item.name}: {text[:60]}")
        
        # Look for any structured data
        scripts = soup.find_all('script', type='application/ld+json')
        if scripts:
            print(f"\nFound {len(scripts)} structured data scripts")
            
        # Try different patterns
        patterns = [
            ('div', {'class': lambda c: c and any(x in str(c).lower() for x in ['stat', 'metric', 'price', 'revenue'])}),
            ('dl', {}),
            ('table', {}),
        ]
        
        for tag, attrs in patterns:
            elements = soup.find_all(tag, attrs)
            if elements:
                print(f"\nFound {len(elements)} {tag} elements")
                elem = elements[0]
                print(f"First element: {elem.text.strip()[:200]}")
                break