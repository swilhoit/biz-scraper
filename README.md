# Business Listing Scraper

A clean, modular web scraper for business listings from multiple marketplace websites using ScraperAPI.

## 📋 Features

- **9 Marketplace Scrapers**: QuietLight, BizBuySell, BizQuest, WebsiteProperties, Flippa, EmpireFlippers, Acquire, FE International, WebsiteClosers
- **Database Storage**: SQLite with SQLAlchemy ORM (no CSV files)
- **ScraperAPI Integration**: Bypass anti-scraping measures
- **Amazon FBA Detection**: Automatic identification of Amazon FBA businesses
- **Enhanced Detail Scraping**: Fetch additional details from listing pages
- **Data Analysis Tools**: Comprehensive analytics and reporting
- **Modular Architecture**: Easy to add new sites
- **Comprehensive Data**: Price, revenue, cash flow, EBITDA, location, industry, and more
- **Automatic Deduplication**: Based on listing URL

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd biz-scraper-clean

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy environment file
cp .env.example .env

# Edit .env and add your ScraperAPI key
# Get your key at: https://www.scraperapi.com/
```

### 3. Run the Scraper

```bash
# Run all sites
python main.py

# Run specific sites
python main.py --sites QuietLight BizQuest

# Limit listings per site
python main.py --max-listings 50

# Or use the runner script
./run_scraper.sh --sites QuietLight --max-listings 10
```

## 📊 Supported Sites

| Site | Success Rate | Notes |
|------|-------------|-------|
| **QuietLight** | ⭐⭐⭐⭐⭐ | Best performer, high-value listings ($1M-$17M) |
| **BizQuest** | ⭐⭐⭐⭐ | Good for small-medium businesses |
| **WebsiteProperties** | ⭐⭐⭐⭐ | Digital businesses, SaaS focused |
| **Acquire.com** | ⭐⭐⭐ | Startups and SaaS acquisitions |
| **FE International** | ⭐⭐⭐ | Premium online business broker |
| **WebsiteClosers** | ⭐⭐⭐ | Established online businesses |
| **BizBuySell** | ⭐⭐ | Individual pages may fail (anti-scraping) |
| **Flippa** | ⭐⭐ | JavaScript-heavy, limited success |
| **EmpireFlippers** | ⭐⭐ | JavaScript-heavy, needs different approach |

## 🗄️ Database Schema

The SQLite database (`businesses.db`) stores:

- **Basic Info**: title, source_site, listing_url
- **Financials**: price, revenue, cash_flow, multiple, ebitda, inventory_value
- **Details**: location, industry, established_year, employees, website
- **Features**: seller_financing, reason_for_selling, monthly_traffic
- **Amazon FBA**: is_amazon_fba, amazon_business_type
- **Metadata**: scraped_at, updated_at, enhanced_at

## 🏗️ Project Structure

```
biz-scraper-clean/
├── config/
│   ├── __init__.py
│   └── settings.py          # ScraperAPI config & site definitions
├── database/
│   ├── __init__.py
│   └── schema.py            # SQLAlchemy models
├── scrapers/
│   ├── __init__.py
│   ├── base_scraper.py      # Abstract base class
│   ├── bizbuysell_scraper.py
│   ├── bizquest_scraper.py
│   ├── flippa_scraper.py
│   ├── quietlight_scraper.py
│   ├── websiteproperties_scraper.py
│   ├── empireflippers_scraper.py
│   ├── acquire_scraper.py
│   ├── feinternational_scraper.py
│   ├── websiteclosers_scraper.py
│   └── detail_scraper.py    # Enhanced detail fetcher
├── utils/
│   ├── __init__.py
│   └── amazon_detector.py   # Amazon FBA detection
├── .env.example             # Environment template
├── .gitignore              # Git ignore rules
├── main.py                 # Entry point
├── analyze_data.py         # Data analysis tool
├── requirements.txt        # Python dependencies
├── run_scraper.sh         # Convenience runner
├── test_all_sites.py      # Test suite
└── README.md              # This file
```

## 🔍 Additional Features

### Amazon FBA Detection
The scraper automatically detects Amazon FBA businesses using keyword analysis:
```bash
# Results include is_amazon_fba and amazon_business_type fields
```

### Enhanced Detail Scraping
Fetch additional details from individual listing pages:
```bash
# Enhance all listings
python -m scrapers.detail_scraper

# Enhance specific site
python -m scrapers.detail_scraper --source QuietLight --limit 10
```

### Data Analysis
Analyze scraped data with comprehensive reports:
```bash
# Full analysis report
python analyze_data.py

# Specific analyses
python analyze_data.py --amazon      # Amazon FBA analysis
python analyze_data.py --quality     # Data quality report
python analyze_data.py --high-value 1000000  # Listings over $1M
```

### Data Export
Export data to CSV for external analysis:
```bash
# Export all data
python export_data.py

# Export with filters
python export_data.py --source QuietLight
python export_data.py --amazon              # Only Amazon FBA
python export_data.py -o my_export.csv      # Custom filename
```

## 🧪 Testing

```bash
# Test all sites individually
python test_all_sites.py

# This will:
# - Test search page access
# - Attempt to scrape one listing
# - Save to database
# - Report success/failure for each site
```

## 🔧 Adding a New Site

1. Create a new scraper in `scrapers/` inheriting from `BaseScraper`
2. Implement `get_listing_urls()` and `scrape_listing()` methods
3. Add site config to `config/settings.py`
4. Add scraper class to `main.py`

Example:
```python
from scrapers.base_scraper import BaseScraper

class NewSiteScraper(BaseScraper):
    def get_listing_urls(self, max_pages=None):
        # Implementation
        pass
    
    def scrape_listing(self, url):
        # Implementation
        pass
```

## ⚠️ Important Notes

- **API Limits**: ScraperAPI has request limits based on your plan
- **Rate Limiting**: The scraper includes delays to be respectful
- **Anti-Scraping**: Some sites (BizBuySell) have strong protections
- **Data Quality**: Not all fields will be available for every listing

## 📈 Performance Tips

1. **Start with successful sites**: QuietLight, BizQuest, WebsiteProperties
2. **Use `--max-listings`**: Test with small batches first
3. **Monitor logs**: Check for 500 errors or timeouts
4. **Database queries**: Use SQLite browser to explore data

## 🐛 Troubleshooting

- **500 Errors**: Site is blocking ScraperAPI, try different site
- **No listings found**: Check site structure may have changed
- **Import errors**: Ensure all dependencies are installed
- **Database locked**: Close other connections to SQLite

## 📝 License

This project is for educational purposes. Respect website terms of service and robots.txt files.