import os
from dotenv import load_dotenv

load_dotenv()

SCRAPER_API_KEY = os.getenv('SCRAPER_API_KEY')

if not SCRAPER_API_KEY:
    raise ValueError("SCRAPER_API_KEY not found in .env file")

# ScraperAPI base URL
SCRAPER_API_URL = 'http://api.scraperapi.com'

# ScraperAPI default parameters - basic config that works
SCRAPER_API_PARAMS = {
    'api_key': SCRAPER_API_KEY,
    'country_code': 'us'
}

# List of business marketplace sites to scrape
SITES = [
    {
        'name': 'QuietLight',
        'base_url': 'https://quietlight.com',
        'search_urls': [
            'https://quietlight.com/amazon-fba-businesses-for-sale/',
            'https://quietlight.com/ecommerce-businesses-for-sale/'
        ],
        'enabled': True
    },
    {
        'name': 'BizBuySell',
        'base_url': 'https://www.bizbuysell.com',
        'amazon_url': 'https://www.bizbuysell.com/amazon-stores-for-sale/',
        'enabled': True
    },
    {
        'name': 'BizQuest',
        'base_url': 'https://www.bizquest.com',
        'search_url': 'https://www.bizquest.com/dynamic/search/businesses-for-sale/',
        'amazon_url': 'https://www.bizquest.com/amazon-business-for-sale/',
        'enabled': True
    },
    {
        'name': 'WebsiteProperties',
        'base_url': 'https://websiteproperties.com',
        'search_url': 'https://websiteproperties.com/websites-for-sale/',
        'enabled': True
    },
    {
        'name': 'Flippa',
        'base_url': 'https://flippa.com',
        'search_url': 'https://flippa.com/search?filter[property_type][]=website',
        'amazon_url': 'https://flippa.com/buy/monetization/amazon-fba',
        'enabled': True
    },
    {
        'name': 'EmpireFlippers',
        'base_url': 'https://empireflippers.com',
        'search_url': 'https://empireflippers.com/marketplace/',
        'amazon_url': 'https://empireflippers.com/marketplace/amazon-fba-businesses-for-sale/',
        'enabled': True
    },
    {
        'name': 'Acquire',
        'base_url': 'https://acquire.com',
        'search_url': 'https://acquire.com/buyers/',
        'enabled': True
    },
    {
        'name': 'FEInternational',
        'base_url': 'https://feinternational.com',
        'search_url': 'https://feinternational.com/buy-a-business/',
        'enabled': True
    },
    {
        'name': 'WebsiteClosers',
        'base_url': 'https://www.websiteclosers.com',
        'search_url': 'https://www.websiteclosers.com/businesses-for-sale/',
        'enabled': True
    }
]