
import requests
from config import SCRAPER_API_PARAMS

def get_quietlight_listing_html():
    """Fetches the HTML of a QuietLight listing page."""
    listing_url = 'https://quietlight.com/listings/16065383/'
    params = SCRAPER_API_PARAMS.copy()
    params['url'] = listing_url
    response = requests.get('http://api.scraperapi.com', params=params)
    if response.status_code == 200:
        print(response.text)
    else:
        print(f"Error: {response.status_code}")

if __name__ == '__main__':
    get_quietlight_listing_html()
