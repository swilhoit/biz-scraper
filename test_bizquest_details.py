#!/usr/bin/env python3
"""Test BizQuest detail scraper on a single page first"""

import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import re

load_dotenv()

def test_single_page():
    api_key = os.getenv('SCRAPER_API_KEY')
    if not api_key:
        print("Error: SCRAPER_API_KEY not found")
        return
    
    # Test with the first listing
    url = "https://www.bizquest.com/business-for-sale/established-online-household-goods-store/BW2388457/"
    
    params = {
        'api_key': api_key,
        'url': url,
        'render': 'true'
    }
    
    print(f"Testing detail page: {url}")
    print("Making request...")
    
    try:
        response = requests.get("http://api.scraperapi.com", params=params, timeout=90)
        response.raise_for_status()
        
        print(f"Response status: {response.status_code}")
        print(f"Response length: {len(response.text)} characters")
        
        # Save for inspection
        with open('bizquest_detail_test.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("Saved to bizquest_detail_test.html")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for financial information
        print("\nSearching for financial data...")
        
        # Method 1: Look for text patterns
        text_content = soup.get_text()
        
        # Revenue patterns
        revenue_patterns = [
            r'(?:Gross Revenue|Revenue|Annual Revenue|Sales)[\s:]*\$?([\d,]+)',
            r'(?:Gross Income|Annual Sales)[\s:]*\$?([\d,]+)',
        ]
        
        for pattern in revenue_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                print(f"Revenue pattern matches: {matches}")
        
        # Look for specific sections
        print("\nLooking for financial sections...")
        
        # Check for tables
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables")
        
        # Check for definition lists
        dl_elements = soup.find_all(['dl', 'dt', 'dd'])
        print(f"Found {len(dl_elements)} definition list elements")
        
        # Look for any element containing financial keywords
        financial_elements = soup.find_all(string=re.compile(r'(revenue|sales|income|cash flow|inventory)', re.I))
        print(f"\nFound {len(financial_elements)} elements with financial keywords")
        for elem in financial_elements[:10]:
            if elem.strip():
                parent = elem.parent
                if parent:
                    text = parent.get_text().strip()
                    if len(text) < 200:
                        print(f"  - {text}")
        
        # Check JSON-LD
        json_scripts = soup.find_all('script', type='application/ld+json')
        print(f"\nFound {len(json_scripts)} JSON-LD scripts")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_page()