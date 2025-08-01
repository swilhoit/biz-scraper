import asyncio
from scrapers.empireflippers_scraper import EmpireFlippersScraper
from config.settings import SITES

def get_listing_page_html():
    empireflippers_config = next((site for site in SITES if site['name'] == 'EmpireFlippers'), None)
    if not empireflippers_config:
        print("EmpireFlippers config not found")
        return

    scraper = EmpireFlippersScraper(empireflippers_config)
    url = "https://empireflippers.com/listing/85717/"
    soup = scraper.get_page(url)
    
    if soup:
        print(soup.prettify())
    else:
        print("No content found")

if __name__ == '__main__':
    get_listing_page_html()
