from scrapers.empireflippers_scraper import EmpireFlippersScraper
from config.settings import SITES

# Get EmpireFlippers config
empire_config = next(site for site in SITES if site['name'] == 'EmpireFlippers')
scraper = EmpireFlippersScraper(empire_config)

# Test the specific listing from the screenshot
test_url = "https://empireflippers.com/listing/85705/"
print(f"Testing: {test_url}")

# Scrape the listing
result = scraper.scrape_listing(test_url)

if result:
    print("\nExtracted data:")
    for key, value in result.items():
        if key != 'description':
            print(f"  {key}: {value}")
    
    # Verify the values match the screenshot
    print("\nValidation against screenshot:")
    expected = {
        'price': 649354,
        'monthly_profit': 16234,
        'monthly_revenue': 86589,
        'annual_profit': 16234 * 12,
        'annual_revenue': 86589 * 12
    }
    
    print(f"  Price: ${result.get('price', 0):,} (expected: ${expected['price']:,})")
    print(f"  Annual Revenue: ${result.get('revenue', 0):,} (expected: ${expected['annual_revenue']:,})")
    print(f"  Annual Cash Flow: ${result.get('cash_flow', 0):,} (expected: ${expected['annual_profit']:,})")
else:
    print("Failed to scrape listing")