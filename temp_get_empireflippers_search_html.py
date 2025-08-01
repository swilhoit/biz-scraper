import asyncio
from scrapers.empireflippers_scraper import EmpireFlippersScraper
from config.settings import SITES

def get_search_page_html():
    empireflippers_config = next((site for site in SITES if site['name'] == 'EmpireFlippers'), None)
    if not empireflippers_config:
        print("EmpireFlippers config not found")
        return

    scraper = EmpireFlippersScraper(empireflippers_config)
    # The search URL can be paginated by appending the page number
    url = scraper.site_config['search_url']
    soup = scraper.get_page(url)
    
    if soup:
        print(soup.prettify())
    else:
        print("No content found")

if __name__ == '__main__':
    get_search_page_html()
