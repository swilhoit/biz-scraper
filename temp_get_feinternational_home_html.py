import asyncio
from scrapers.feinternational_scraper import FEInternationalScraper
from config.settings import SITES

def get_home_page_html():
    feinternational_config = next((site for site in SITES if site['name'] == 'FEInternational'), None)
    if not feinternational_config:
        print("FEInternational config not found")
        return

    scraper = FEInternationalScraper(feinternational_config)
    url = scraper.site_config['base_url']
    soup = scraper.get_page(url)
    
    if soup:
        print(soup.prettify())
    else:
        print("No content found")

if __name__ == '__main__':
    get_home_page_html()
