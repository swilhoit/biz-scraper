import asyncio
from scrapers.acquire_scraper import AcquireScraper
from config.settings import SITES

def get_home_page_html():
    acquire_config = next((site for site in SITES if site['name'] == 'Acquire'), None)
    if not acquire_config:
        print("Acquire config not found")
        return

    scraper = AcquireScraper(acquire_config)
    url = scraper.site_config['base_url']
    soup = scraper.get_page(url)
    
    if soup:
        print(soup.prettify())
    else:
        print("No content found")

if __name__ == '__main__':
    get_home_page_html()
