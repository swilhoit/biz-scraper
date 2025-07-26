#!/usr/bin/env python3
"""Test Empire Flippers scraping specifically"""

import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import json

load_dotenv()

def test_empire_flippers():
    api_key = os.getenv('SCRAPER_API_KEY')
    if not api_key:
        print("Error: SCRAPER_API_KEY not found")
        return
    
    url = "https://empireflippers.com/marketplace/amazon-fba-businesses-for-sale/"
    
    # Make request with ScraperAPI
    params = {
        'api_key': api_key,
        'url': url,
        'render': 'true'
    }
    
    print(f"Testing Empire Flippers URL: {url}")
    print("Making request via ScraperAPI...")
    
    try:
        response = requests.get("http://api.scraperapi.com", params=params, timeout=90)
        response.raise_for_status()
        
        print(f"Response status: {response.status_code}")
        print(f"Response length: {len(response.text)} characters")
        
        # Save raw HTML for inspection
        with open('empire_flippers_raw.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("Saved raw HTML to empire_flippers_raw.html")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try various selectors
        print("\nTrying to find listings...")
        
        selectors = [
            'div.listing-card',
            'div.business-card', 
            'div.marketplace-listing',
            'div[data-listing-id]',
            'article.listing',
            'div[class*="listing"]',
            'div[class*="business"]',
            'div[class*="card"]',
            'a[href*="/listing/"]',
            'a[href*="unlock"]'
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
        texts = soup.find_all(text=True)
        for text in texts:
            if any(keyword in text.lower() for keyword in ['monetization', 'amazon fba', 'price', 'monthly net']):
                print(f"  Found: {text.strip()[:100]}")
        
        # Check if we're hitting a different page (login, etc)
        title = soup.find('title')
        if title:
            print(f"\nPage title: {title.text}")
        
        # Look for any JSON data
        scripts = soup.find_all('script', type='application/ld+json')
        if scripts:
            print(f"\nFound {len(scripts)} JSON-LD scripts")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_empire_flippers()