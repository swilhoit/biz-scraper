import asyncio
from scrapers import (
    AcquireScraper, BizBuySellScraper, BizQuestScraper,
    EmpireFlippersScraper, FEInternationalScraper, FlippaScraper,
    QuietLightScraper, WebsiteClosersScraper, WebsitePropertiesScraper
)
from config.settings import SITES

async def test_scrapers():
    # Map scraper names to their classes
    scraper_classes = {
        'Acquire': AcquireScraper,
        'BizBuySell': BizBuySellScraper,
        'BizQuest': BizQuestScraper,
        'EmpireFlippers': EmpireFlippersScraper,
        'FEInternational': FEInternationalScraper,
        'Flippa': FlippaScraper,
        'QuietLight': QuietLightScraper,
        'WebsiteClosers': WebsiteClosersScraper,
        'WebsiteProperties': WebsitePropertiesScraper
    }
    
    results = []
    
    for site_config in SITES:
        if not site_config.get('enabled', True):
            continue
            
        name = site_config['name']
        scraper_class = scraper_classes.get(name)
        
        if not scraper_class:
            print(f"No scraper class found for {name}")
            continue
            
        try:
            print(f"\nTesting {name}...")
            scraper = scraper_class(site_config)
            
            # Get listing URLs without scraping them
            listing_urls = scraper._get_all_listing_urls(max_pages=1)
            
            print(f"{name}: Found {len(listing_urls)} listings")
            if len(listing_urls) == 0:
                print(f"  ⚠️  WARNING: {name} found no listings!")
                results.append((name, 0, "No listings found"))
            else:
                # Show first few URLs
                print(f"  Sample URLs:")
                for url in listing_urls[:3]:
                    print(f"    - {url}")
                results.append((name, len(listing_urls), "OK"))
                
        except Exception as e:
            print(f"{name}: ❌ ERROR - {str(e)}")
            results.append((name, 0, f"Error: {str(e)}"))
    
    print("\n" + "="*60)
    print("SUMMARY:")
    print("="*60)
    for name, count, status in results:
        if count == 0:
            print(f"❌ {name}: {status}")
        else:
            print(f"✓ {name}: {count} listings found")

if __name__ == "__main__":
    asyncio.run(test_scrapers())