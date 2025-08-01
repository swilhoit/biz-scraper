
import requests
from config import SCRAPER_API_PARAMS

def get_websiteproperties_listing_html():
    """Fetches the HTML of a WebsiteProperties listing page."""
    listing_url = 'https://websiteproperties.com/websites/13786-web-hosting-and-ecommerce-platform-with-proprietary-website-builder/'
    params = SCRAPER_API_PARAMS.copy()
    params['url'] = listing_url
    response = requests.get('http://api.scraperapi.com', params=params)
    if response.status_code == 200:
        print(response.text)
    else:
        print(f"Error: {response.status_code}")

if __name__ == '__main__':
    get_websiteproperties_listing_html()
