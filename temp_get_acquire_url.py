import asyncio
from scrapers.acquire_scraper import AcquireScraper
from config.settings import SITES

def get_first_url():
    acquire_config = next((site for site in SITES if site['name'] == 'Acquire'), None)
    if not acquire_config:
        print("Acquire config not found")
        return

    scraper = AcquireScraper(acquire_config)
    # Get just one page to get a sample URL quickly
    urls = scraper.get_listing_urls(max_pages=1)
    if urls:
        print(urls[0])
    else:
        print("No URLs found")

if __name__ == '__main__':
    get_first_url()
