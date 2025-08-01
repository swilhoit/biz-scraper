import asyncio
from scrapers.feinternational_scraper import FEInternationalScraper
from config.settings import SITES

def get_first_url():
    feinternational_config = next((site for site in SITES if site['name'] == 'FEInternational'), None)
    if not feinternational_config:
        print("FEInternational config not found")
        return

    scraper = FEInternationalScraper(feinternational_config)
    # Get just one page to get a sample URL quickly
    urls = scraper.get_listing_urls(max_pages=1)
    if urls:
        print(urls[0])
    else:
        print("No URLs found")

if __name__ == '__main__':
    get_first_url()
