# Business Listings Scraper

A comprehensive web scraper that extracts business listings from multiple Amazon FBA marketplace websites and exports the data to CSV format.

## Features

- Scrapes 9 different business marketplace websites
- Extracts key business metrics: name, price, revenue, profit, description
- Calculates price multiples where possible
- Removes duplicate listings
- Exports to CSV format
- Uses Scraper API for reliable data extraction
- Comprehensive logging and error handling

## Websites Scraped

1. QuietLight.com - Amazon FBA businesses
2. BizBuySell.com - Amazon stores
3. Flippa.com - Amazon FBA monetization
4. LoopNet.com - Amazon stores
5. EmpireFlippers.com - Amazon FBA businesses
6. Investors.club - Amazon FBA tech stack
7. WebsiteProperties.com - Amazon FBA businesses
8. BizQuest.com - Amazon businesses
9. Acquire.com - Amazon FBA businesses

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Scraper API**
   Create a `.env` file in the project root:
   ```
   SCRAPER_API_KEY=your_scraper_api_key_here
   ```
   
   Get your API key from [Scraper API](https://www.scraperapi.com/)

3. **Run the Scraper**
   ```bash
   python business_scraper.py
   ```

## Output

The scraper generates:
- `business_listings.csv` - CSV file with all scraped business data
- Console output with scraping progress and summary statistics

## CSV Columns

- **source**: Website where the listing was found
- **name**: Business name/title
- **price**: Asking price
- **revenue**: Annual revenue (if available)
- **profit**: Annual profit (if available)
- **multiple**: Price-to-revenue multiple (calculated when possible)
- **description**: Business description
- **url**: Source URL

## Features

### Smart Data Extraction
- Automatically detects different website structures
- Extracts financial data using pattern matching
- Handles various price formats ($1M, $1,000,000, etc.)

### Data Quality
- Removes duplicate listings based on name similarity
- Filters out navigation and non-content elements
- Validates data before export

### Error Handling
- Retry logic for failed requests
- Comprehensive logging
- Graceful handling of parsing errors

## Example Usage

```python
from business_scraper import BusinessScraper

# Initialize scraper
scraper = BusinessScraper()

# Scrape all websites
scraper.scrape_all()

# Export to CSV
scraper.export_to_csv('my_listings.csv')

# Print summary
scraper.print_summary()
```

## Requirements

- Python 3.7+
- Scraper API account and key
- Internet connection

## Rate Limiting

The scraper includes built-in delays between requests to be respectful to target websites and comply with Scraper API guidelines.

## Troubleshooting

1. **API Key Issues**: Ensure your Scraper API key is correctly set in the `.env` file
2. **No Data Found**: Some websites may have changed their structure; check logs for parsing errors
3. **Rate Limiting**: If you encounter rate limits, the scraper will automatically retry with exponential backoff

## Legal Notice

This scraper is for educational and research purposes. Always respect website terms of service and robots.txt files. Consider reaching out to website owners for permission when scraping large amounts of data. 