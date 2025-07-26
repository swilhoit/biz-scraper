#!/usr/bin/env python3
"""BizQuest detail page scraper to extract revenue and additional data"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BizQuestDetailScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
    def make_request(self, url, retries=3):
        """Make a request using Scraper API"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'render': 'true'
        }
        
        for attempt in range(retries):
            try:
                response = requests.get("http://api.scraperapi.com", params=params, timeout=90)
                response.raise_for_status()
                return response
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None
    
    def clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text.strip())
        return text
    
    def extract_financial_value(self, text):
        """Extract financial value from text"""
        if not text:
            return ""
        
        # Look for currency patterns
        patterns = [
            r'\$[\d,]+(?:\.\d{2})?',
            r'\$\d+(?:\.\d+)?[KkMm]',
            r'[\d,]+(?:\.\d{2})?',  # Numbers without $
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                value = match.group()
                # Add $ if not present
                if not value.startswith('$'):
                    value = '$' + value
                return value
        return ""
    
    def scrape_detail_page(self, url):
        """Scrape a single BizQuest detail page"""
        logger.info(f"Scraping detail page: {url}")
        
        response = self.make_request(url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Initialize data
        data = {
            'url': url,
            'gross_revenue': '',
            'cash_flow': '',
            'asking_price': '',
            'inventory': '',
            'established': '',
            'employees': '',
            'business_type': '',
            'financing_available': False,
            'detailed_description': ''
        }
        
        try:
            # Look for financial metrics section
            # BizQuest often has a table or list with financial details
            
            # Method 1: Look for labeled financial data
            financial_labels = soup.find_all(['dt', 'td', 'span', 'div'], string=re.compile(r'(Gross Revenue|Revenue|Sales|Gross Income)', re.I))
            for label in financial_labels:
                # Look for the value in the next sibling or parent structure
                parent = label.parent
                value_text = parent.get_text()
                if 'revenue' in value_text.lower() or 'sales' in value_text.lower():
                    value = self.extract_financial_value(value_text)
                    if value and not data['gross_revenue']:
                        data['gross_revenue'] = value
            
            # Method 2: Look for structured data (JSON-LD)
            json_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_scripts:
                try:
                    json_data = json.loads(script.string)
                    if isinstance(json_data, dict):
                        # Check for offers/price
                        if 'offers' in json_data and 'price' in json_data['offers']:
                            data['asking_price'] = f"${json_data['offers']['price']:,}"
                except:
                    pass
            
            # Method 3: Look for specific metric containers
            metric_containers = soup.select('.metric-item, .financial-item, .listing-detail-item')
            for container in metric_containers:
                text = container.get_text().lower()
                value = self.extract_financial_value(container.get_text())
                
                if 'revenue' in text and value:
                    data['gross_revenue'] = value
                elif 'cash flow' in text and value:
                    data['cash_flow'] = value
                elif 'inventory' in text and value:
                    data['inventory'] = value
                elif 'established' in text:
                    year_match = re.search(r'\b(19|20)\d{2}\b', container.get_text())
                    if year_match:
                        data['established'] = year_match.group()
                elif 'employees' in text:
                    emp_match = re.search(r'\b\d+\b', container.get_text())
                    if emp_match:
                        data['employees'] = emp_match.group()
            
            # Look for financing available
            if soup.find(string=re.compile(r'financing available', re.I)):
                data['financing_available'] = True
            
            # Get detailed description
            desc_elem = soup.select_one('.listing-description, .business-description, .description-content')
            if desc_elem:
                data['detailed_description'] = self.clean_text(desc_elem.get_text())[:1000]
            
            # Business type/category
            category_elem = soup.select_one('.category, .business-type, .industry')
            if category_elem:
                data['business_type'] = self.clean_text(category_elem.get_text())
            
            return data
            
        except Exception as e:
            logger.error(f"Error parsing detail page {url}: {e}")
            return None
    
    def test_sample_listings(self):
        """Test the scraper on a few sample listings"""
        # Read the existing CSV to get some URLs
        df = pd.read_csv('bizquest_results.csv')
        sample_urls = df['url'].head(3).tolist()
        
        print("\nTesting detail scraper on sample listings...")
        for url in sample_urls:
            print(f"\n{'='*60}")
            print(f"Testing: {url}")
            data = self.scrape_detail_page(url)
            if data:
                print("Extracted data:")
                for key, value in data.items():
                    if value and key != 'url':
                        print(f"  {key}: {value}")
            else:
                print("Failed to extract data")
            print('='*60)
            
    def scrape_all_details(self, input_csv='bizquest_results.csv', output_csv='bizquest_detailed_results.csv'):
        """Scrape details for all listings in the input CSV"""
        df = pd.read_csv(input_csv)
        logger.info(f"Found {len(df)} listings to process")
        
        # Prepare results
        detailed_results = []
        
        # Process in parallel with thread pool
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all tasks
            future_to_row = {}
            for idx, row in df.iterrows():
                future = executor.submit(self.scrape_detail_page, row['url'])
                future_to_row[future] = row
            
            # Process completed tasks
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    detail_data = future.result()
                    if detail_data:
                        # Merge with existing data
                        result = row.to_dict()
                        result.update(detail_data)
                        
                        # Update revenue if found
                        if detail_data['gross_revenue'] and not result.get('revenue'):
                            result['revenue'] = detail_data['gross_revenue']
                        
                        detailed_results.append(result)
                        logger.info(f"Processed: {row['name'][:50]}...")
                    else:
                        # Keep original data even if detail scraping failed
                        detailed_results.append(row.to_dict())
                        logger.warning(f"No details extracted for: {row['name'][:50]}...")
                        
                except Exception as e:
                    logger.error(f"Error processing {row['url']}: {e}")
                    detailed_results.append(row.to_dict())
        
        # Save results
        detailed_df = pd.DataFrame(detailed_results)
        detailed_df.to_csv(output_csv, index=False)
        logger.info(f"Saved {len(detailed_df)} listings to {output_csv}")
        
        # Print summary
        print(f"\nDetail Scraping Summary:")
        print(f"Total listings processed: {len(detailed_df)}")
        print(f"Listings with revenue data: {detailed_df['revenue'].str.len().gt(0).sum()}")
        print(f"Listings with established year: {detailed_df['established'].str.len().gt(0).sum()}")
        print(f"Listings with financing available: {detailed_df['financing_available'].sum()}")

def main():
    """Run the BizQuest detail scraper"""
    scraper = BizQuestDetailScraper()
    
    # First test on a few samples
    scraper.test_sample_listings()
    
    # Ask user if they want to proceed
    response = input("\nDo you want to scrape details for all listings? (y/n): ")
    if response.lower() == 'y':
        scraper.scrape_all_details()

if __name__ == "__main__":
    main()