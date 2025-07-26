#!/usr/bin/env python3
"""
Business Listing Detail Scraper
Scrapes detailed information from individual listing pages using URLs from the first scraper
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import time
import re
import json
from urllib.parse import urlparse, urljoin
from typing import List, Dict, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DetailScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.detailed_data = []
        self.lock = threading.Lock()
        
        # Load the CSV file from the first scraper
        self.listings = self.load_listings()
        
    def load_listings(self, filename: str = 'business_listings.csv') -> pd.DataFrame:
        """Load the business listings from CSV"""
        try:
            df = pd.read_csv(filename)
            logger.info(f"Loaded {len(df)} listings from {filename}")
            return df
        except FileNotFoundError:
            logger.error(f"File {filename} not found. Please run the first scraper first.")
            raise
    
    def make_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """Make a request using Scraper API with retries"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'render': 'true'  # Enable JavaScript rendering
        }
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=90)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
        return None
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace and newlines
        text = re.sub(r'\s+', ' ', text.strip())
        return text.strip()
    
    def extract_financial_metrics(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract financial metrics from the page"""
        metrics = {
            'detailed_price': '',
            'annual_revenue': '',
            'annual_profit': '',
            'monthly_revenue': '',
            'monthly_profit': '',
            'ebitda': '',
            'multiple': '',
            'inventory_included': '',
            'asking_price_includes': ''
        }
        
        # Common patterns for financial data
        financial_patterns = {
            'price': [r'asking\s*price[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?', r'price[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'],
            'revenue': [r'annual\s*revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?', r'revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'],
            'profit': [r'annual\s*profit[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?', r'net\s*profit[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'],
            'ebitda': [r'ebitda[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?', r'sde[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'],
            'multiple': [r'multiple[:\s]*[\d.]+x?', r'[\d.]+x?\s*multiple'],
        }
        
        # Search in common containers
        text_containers = soup.find_all(['div', 'section', 'article', 'dl', 'table'])
        full_text = ' '.join([container.get_text() for container in text_containers])
        
        for metric, patterns in financial_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    if metric == 'price':
                        metrics['detailed_price'] = self.clean_text(match.group())
                    elif metric == 'revenue':
                        metrics['annual_revenue'] = self.clean_text(match.group())
                    elif metric == 'profit':
                        metrics['annual_profit'] = self.clean_text(match.group())
                    elif metric == 'ebitda':
                        metrics['ebitda'] = self.clean_text(match.group())
                    elif metric == 'multiple':
                        metrics['multiple'] = self.clean_text(match.group())
                    break
        
        return metrics
    
    def extract_business_details(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract business details from the page"""
        details = {
            'established_date': '',
            'employees': '',
            'location': '',
            'industry': '',
            'business_type': '',
            'reason_for_selling': '',
            'growth_opportunities': '',
            'competition': ''
        }
        
        # Common patterns for business details
        detail_patterns = {
            'established': [r'established[:\s]*(\d{4})', r'founded[:\s]*(\d{4})', r'since[:\s]*(\d{4})'],
            'employees': [r'employees?[:\s]*(\d+)', r'staff[:\s]*(\d+)', r'team\s*size[:\s]*(\d+)'],
            'location': [r'location[:\s]*([^,\n]+)', r'based\s*in[:\s]*([^,\n]+)'],
            'industry': [r'industry[:\s]*([^,\n]+)', r'sector[:\s]*([^,\n]+)'],
            'reason': [r'reason\s*for\s*selling[:\s]*([^.]+)', r'why\s*selling[:\s]*([^.]+)']
        }
        
        text_containers = soup.find_all(['div', 'section', 'article', 'dl', 'p'])
        full_text = ' '.join([container.get_text() for container in text_containers])
        
        for detail, patterns in detail_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    if detail == 'established':
                        details['established_date'] = match.group(1)
                    elif detail == 'employees':
                        details['employees'] = match.group(1)
                    elif detail == 'location':
                        details['location'] = self.clean_text(match.group(1))
                    elif detail == 'industry':
                        details['industry'] = self.clean_text(match.group(1))
                    elif detail == 'reason':
                        details['reason_for_selling'] = self.clean_text(match.group(1))
                    break
        
        return details
    
    def extract_assets_included(self, soup: BeautifulSoup) -> List[str]:
        """Extract list of assets included in the sale"""
        assets = []
        
        # Look for common asset sections
        asset_keywords = ['assets included', 'included in sale', 'what\'s included', 'sale includes']
        
        for keyword in asset_keywords:
            # Search for sections containing these keywords
            elements = soup.find_all(text=re.compile(keyword, re.IGNORECASE))
            for element in elements:
                parent = element.parent
                if parent:
                    # Look for lists nearby
                    lists = parent.find_all(['ul', 'ol'])
                    for lst in lists:
                        items = lst.find_all('li')
                        assets.extend([self.clean_text(item.get_text()) for item in items])
        
        # Remove duplicates and empty items
        assets = list(set(filter(None, assets)))
        return assets[:10]  # Limit to 10 items
    
    def scrape_quietlight_detail(self, soup: BeautifulSoup) -> Dict[str, any]:
        """Scrape QuietLight listing detail page"""
        data = {}
        
        # QuietLight specific selectors
        data.update(self.extract_financial_metrics(soup))
        data.update(self.extract_business_details(soup))
        
        # Look for specific QuietLight data structure
        metrics_section = soup.find('div', class_=['listing-metrics', 'business-metrics', 'financial-data'])
        if metrics_section:
            metrics_text = metrics_section.get_text()
            data['additional_metrics'] = self.clean_text(metrics_text)[:500]
        
        # Extract detailed description
        desc_elem = soup.find('div', class_=['listing-description', 'business-description', 'content'])
        if desc_elem:
            data['detailed_description'] = self.clean_text(desc_elem.get_text())[:1000]
        
        data['assets_included'] = self.extract_assets_included(soup)
        
        return data
    
    def scrape_bizbuysell_detail(self, soup: BeautifulSoup) -> Dict[str, any]:
        """Scrape BizBuySell listing detail page"""
        data = {}
        
        data.update(self.extract_financial_metrics(soup))
        data.update(self.extract_business_details(soup))
        
        # BizBuySell specific data
        # Look for the business details table
        details_table = soup.find('table', class_='business-details')
        if not details_table:
            details_table = soup.find('div', class_='listing-details')
        
        if details_table:
            rows = details_table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = self.clean_text(cells[0].get_text()).lower()
                    value = self.clean_text(cells[1].get_text())
                    
                    if 'cash flow' in key:
                        data['annual_profit'] = value
                    elif 'gross revenue' in key:
                        data['annual_revenue'] = value
                    elif 'inventory' in key:
                        data['inventory_included'] = value
                    elif 'established' in key:
                        data['established_date'] = value
        
        data['assets_included'] = self.extract_assets_included(soup)
        
        return data
    
    def scrape_empireflippers_detail(self, soup: BeautifulSoup) -> Dict[str, any]:
        """Scrape EmpireFlippers listing detail page"""
        data = {}
        
        data.update(self.extract_financial_metrics(soup))
        data.update(self.extract_business_details(soup))
        
        # EmpireFlippers specific data structure
        # They often use data attributes or specific class names
        listing_data = soup.find('div', {'data-listing': True})
        if listing_data:
            try:
                json_data = json.loads(listing_data.get('data-listing', '{}'))
                data['monthly_revenue'] = json_data.get('monthly_revenue', '')
                data['monthly_profit'] = json_data.get('monthly_profit', '')
                data['multiple'] = json_data.get('multiple', '')
            except:
                pass
        
        # Look for monetization methods
        monetization = soup.find('div', class_=['monetization', 'revenue-streams'])
        if monetization:
            data['monetization_methods'] = self.clean_text(monetization.get_text())[:300]
        
        data['assets_included'] = self.extract_assets_included(soup)
        
        return data
    
    def scrape_flippa_detail(self, soup: BeautifulSoup) -> Dict[str, any]:
        """Scrape Flippa listing detail page"""
        data = {}
        
        data.update(self.extract_financial_metrics(soup))
        data.update(self.extract_business_details(soup))
        
        # Flippa often shows monthly metrics prominently
        monthly_section = soup.find('div', class_=['monthly-metrics', 'financial-summary'])
        if monthly_section:
            monthly_text = monthly_section.get_text()
            
            # Extract monthly revenue
            monthly_rev_match = re.search(r'monthly.*revenue[:\s]*\$?[\d,]+', monthly_text, re.IGNORECASE)
            if monthly_rev_match:
                data['monthly_revenue'] = self.clean_text(monthly_rev_match.group())
            
            # Extract monthly profit
            monthly_profit_match = re.search(r'monthly.*profit[:\s]*\$?[\d,]+', monthly_text, re.IGNORECASE)
            if monthly_profit_match:
                data['monthly_profit'] = self.clean_text(monthly_profit_match.group())
        
        # Traffic data (common on Flippa)
        traffic_section = soup.find('div', class_=['traffic-data', 'analytics'])
        if traffic_section:
            data['traffic_info'] = self.clean_text(traffic_section.get_text())[:300]
        
        data['assets_included'] = self.extract_assets_included(soup)
        
        return data
    
    def scrape_generic_detail(self, soup: BeautifulSoup) -> Dict[str, any]:
        """Generic detail scraper for other sites"""
        data = {}
        
        data.update(self.extract_financial_metrics(soup))
        data.update(self.extract_business_details(soup))
        
        # Try to find any structured data
        schema_data = soup.find('script', type='application/ld+json')
        if schema_data:
            try:
                json_data = json.loads(schema_data.string)
                if isinstance(json_data, dict):
                    data['structured_data'] = str(json_data)[:500]
            except:
                pass
        
        # Extract main content
        main_content = soup.find(['main', 'article', 'div'], class_=re.compile('content|main|detail'))
        if main_content:
            data['detailed_description'] = self.clean_text(main_content.get_text())[:1000]
        
        data['assets_included'] = self.extract_assets_included(soup)
        
        return data
    
    def scrape_detail_page(self, row: pd.Series) -> Dict[str, any]:
        """Scrape a single detail page"""
        url = row['url']
        
        # Skip invalid URLs
        if not url or url == 'nan' or not url.startswith('http'):
            logger.warning(f"Skipping invalid URL: {url}")
            return None
        
        # Skip generic pages
        if any(skip in url.lower() for skip in ['pricing', 'sellers', 'buyers', 'partner', 'listing/']):
            logger.info(f"Skipping generic page: {url}")
            return None
        
        response = self.make_request(url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        domain = urlparse(url).netloc.lower()
        
        # Base data from original scrape
        detail_data = {
            'source': row['source'],
            'name': row['name'],
            'original_price': row['price'],
            'original_revenue': row['revenue'],
            'original_profit': row['profit'],
            'url': url,
            'scrape_timestamp': datetime.now().isoformat()
        }
        
        # Scrape based on source
        try:
            if 'quietlight' in domain:
                detail_data.update(self.scrape_quietlight_detail(soup))
            elif 'bizbuysell' in domain:
                detail_data.update(self.scrape_bizbuysell_detail(soup))
            elif 'empireflippers' in domain:
                detail_data.update(self.scrape_empireflippers_detail(soup))
            elif 'flippa' in domain:
                detail_data.update(self.scrape_flippa_detail(soup))
            else:
                detail_data.update(self.scrape_generic_detail(soup))
            
            logger.info(f"Successfully scraped details for: {row['name'][:50]}...")
            return detail_data
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return detail_data
    
    def scrape_all_details(self, max_workers: int = 10, limit: Optional[int] = None):
        """Scrape all detail pages in parallel"""
        logger.info("Starting detail page scraping...")
        
        # Filter out rows with invalid URLs
        valid_listings = self.listings[
            self.listings['url'].notna() & 
            (self.listings['url'] != '') &
            self.listings['url'].str.startswith('http')
        ]
        
        if limit:
            valid_listings = valid_listings.head(limit)
        
        logger.info(f"Scraping details for {len(valid_listings)} valid listings...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks
            futures = []
            for idx, row in valid_listings.iterrows():
                future = executor.submit(self.scrape_detail_page, row)
                futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        with self.lock:
                            self.detailed_data.append(result)
                except Exception as e:
                    logger.error(f"Error processing future: {e}")
        
        logger.info(f"Completed scraping {len(self.detailed_data)} detail pages")
    
    def export_to_csv(self, filename: str = 'business_details.csv'):
        """Export detailed data to CSV"""
        if not self.detailed_data:
            logger.warning("No detailed data to export")
            return
        
        df = pd.DataFrame(self.detailed_data)
        
        # Convert lists to strings for CSV export
        if 'assets_included' in df.columns:
            df['assets_included'] = df['assets_included'].apply(lambda x: '; '.join(x) if isinstance(x, list) else x)
        
        # Reorder columns for better readability
        priority_columns = [
            'source', 'name', 'original_price', 'detailed_price', 
            'annual_revenue', 'annual_profit', 'monthly_revenue', 'monthly_profit',
            'ebitda', 'multiple', 'established_date', 'employees', 'location',
            'industry', 'reason_for_selling', 'assets_included', 'url'
        ]
        
        # Only include columns that exist
        columns = [col for col in priority_columns if col in df.columns]
        # Add any remaining columns
        remaining_columns = [col for col in df.columns if col not in columns]
        columns.extend(remaining_columns)
        
        df = df[columns]
        df.to_csv(filename, index=False)
        logger.info(f"Detailed data exported to {filename}")
    
    def export_to_json(self, filename: str = 'business_details.json'):
        """Export detailed data to JSON for better structure preservation"""
        if not self.detailed_data:
            logger.warning("No detailed data to export")
            return
        
        with open(filename, 'w') as f:
            json.dump(self.detailed_data, f, indent=2)
        logger.info(f"Detailed data exported to {filename}")
    
    def print_summary(self):
        """Print summary of scraped data"""
        if not self.detailed_data:
            logger.info("No detailed data scraped")
            return
        
        df = pd.DataFrame(self.detailed_data)
        
        print("\n" + "="*50)
        print("DETAIL SCRAPING SUMMARY")
        print("="*50)
        print(f"Total detail pages scraped: {len(df)}")
        print(f"Sources: {df['source'].value_counts().to_dict()}")
        
        # Count fields with data
        field_counts = {}
        for col in df.columns:
            non_empty = df[col].astype(str).str.strip().str.len().gt(0).sum()
            if non_empty > 0:
                field_counts[col] = non_empty
        
        print("\nData completeness:")
        for field, count in sorted(field_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {field}: {count} ({count/len(df)*100:.1f}%)")
        
        # Show sample detailed listing
        print("\nSample detailed listing:")
        if len(df) > 0:
            sample = df.iloc[0]
            print(f"\nBusiness: {sample.get('name', 'N/A')}")
            print(f"Source: {sample.get('source', 'N/A')}")
            print(f"Price: {sample.get('detailed_price', sample.get('original_price', 'N/A'))}")
            print(f"Annual Revenue: {sample.get('annual_revenue', 'N/A')}")
            print(f"Annual Profit: {sample.get('annual_profit', 'N/A')}")
            print(f"Multiple: {sample.get('multiple', 'N/A')}")
            print(f"Location: {sample.get('location', 'N/A')}")
            print(f"Established: {sample.get('established_date', 'N/A')}")


def main():
    """Main function to run the detail scraper"""
    try:
        scraper = DetailScraper()
        
        # You can limit the number of URLs to scrape for testing
        # scraper.scrape_all_details(max_workers=10, limit=5)  # Test with 5 URLs
        
        # Or scrape all URLs
        scraper.scrape_all_details(max_workers=10)
        
        # Export results
        scraper.export_to_csv()
        scraper.export_to_json()
        scraper.print_summary()
        
    except Exception as e:
        logger.error(f"Detail scraper failed: {e}")
        raise


if __name__ == "__main__":
    main()