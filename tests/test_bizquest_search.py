from scrapers.bizquest_scraper import BizQuestScraper
from config.settings import SITES
import re

# Get BizQuest config
bizquest_config = next(site for site in SITES if site['name'] == 'BizQuest')
scraper = BizQuestScraper(bizquest_config)

# Get search page
search_url = f"{bizquest_config['amazon_url']}?page=1"
print(f"Fetching search page: {search_url}")

soup = scraper.get_page(search_url, render=True)
if soup:
    print("Page fetched successfully\n")
    
    # Find all listing links
    listing_links = soup.select('a[href*="/business-for-sale/"]')
    print(f"Found {len(listing_links)} listing links")
    
    if listing_links:
        # Check the first listing link
        first_link = listing_links[0]
        print(f"\nAnalyzing first listing:")
        print(f"Link: {first_link.get('href')}")
        
        # Get the parent container of the link
        parent = first_link.parent
        while parent and parent.name not in ['div', 'article', 'section']:
            parent = parent.parent
            
        if parent:
            print(f"Parent container: {parent.name}")
            
            # Extract all text from the container
            container_text = parent.get_text(separator=' ', strip=True)
            print(f"\nContainer text preview:\n{container_text[:500]}")
            
            # Look for price patterns in the container
            price_match = re.search(r'\$[\d,]+(?:\.\d+)?[KkMm]?', container_text)
            if price_match:
                print(f"\nPrice found: {price_match.group()}")
            
            # Look for revenue/sales
            revenue_match = re.search(r'(?:revenue|sales)[:\s]*\$?[\d,]+[KkMm]?', container_text, re.I)
            if revenue_match:
                print(f"Revenue found: {revenue_match.group()}")
                
            # Look for other financial data
            cash_flow_match = re.search(r'(?:cash flow|profit|income)[:\s]*\$?[\d,]+[KkMm]?', container_text, re.I)
            if cash_flow_match:
                print(f"Cash flow found: {cash_flow_match.group()}")
    
    # Look for listing containers more broadly
    print("\n\nLooking for listing containers...")
    
    # Common patterns for listing containers
    container_selectors = [
        'div.listing-item',
        'div.search-result',
        'div.business-listing',
        'article.listing',
        'div[class*="result"]',
        'div[class*="listing"]'
    ]
    
    for selector in container_selectors:
        containers = soup.select(selector)
        if containers:
            print(f"\nFound {len(containers)} containers with selector: {selector}")
            # Analyze first container
            container = containers[0]
            text = container.get_text(separator=' ', strip=True)[:200]
            print(f"Sample text: {text}")
            break
            
else:
    print("Failed to fetch page")