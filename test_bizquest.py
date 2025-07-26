#!/usr/bin/env python3
"""Test BizQuest scraping specifically"""

import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import json

load_dotenv()

def test_bizquest():
    api_key = os.getenv('SCRAPER_API_KEY')
    if not api_key:
        print("Error: SCRAPER_API_KEY not found")
        return
    
    url = "https://www.bizquest.com/amazon-business-for-sale/"
    
    # Make request with ScraperAPI
    params = {
        'api_key': api_key,
        'url': url,
        'render': 'true'
    }
    
    print(f"Testing BizQuest URL: {url}")
    print("Making request via ScraperAPI...")
    
    try:
        response = requests.get("http://api.scraperapi.com", params=params, timeout=90)
        response.raise_for_status()
        
        print(f"Response status: {response.status_code}")
        print(f"Response length: {len(response.text)} characters")
        
        # Save raw HTML for inspection
        with open('bizquest_raw.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("Saved raw HTML to bizquest_raw.html")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try various selectors
        print("\nTrying to find listings...")
        
        selectors = [
            'div.listing',
            'div.business-listing',
            'div.search-result',
            'div.result-item',
            'article.listing',
            'div[class*="listing"]',
            'div[class*="business"]',
            'div[class*="result"]',
            'tr.result-row',
            'table.results tr',
            'a[href*="/businesses-for-sale/"]',
            'h3.listing-title',
            'div.ad-listing'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                print(f"  {selector}: Found {len(elements)} elements")
                # Show first element
                if elements:
                    print(f"    First element: {str(elements[0])[:200]}...")
        
        # Look for any text containing business/listing info
        print("\nSearching for business-related text...")
        texts = soup.find_all(string=True)
        for text in texts:
            if any(keyword in text.lower() for keyword in ['amazon', 'fba', 'asking price', 'cash flow', 'revenue']):
                clean_text = text.strip()
                if len(clean_text) > 20:
                    print(f"  Found: {clean_text[:100]}")
        
        # Check if this is a search results page
        title = soup.find('title')
        if title:
            print(f"\nPage title: {title.text}")
        
        # Look for any forms or search elements
        forms = soup.find_all('form')
        if forms:
            print(f"\nFound {len(forms)} forms on page")
            
        # Check for "no results" messages
        no_results_indicators = soup.find_all(string=lambda text: text and any(
            phrase in text.lower() for phrase in ['no results', 'no listings', 'not found', '0 results']
        ))
        if no_results_indicators:
            print("\nPossible 'no results' indicators found:")
            for indicator in no_results_indicators[:3]:
                print(f"  {indicator.strip()}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bizquest()