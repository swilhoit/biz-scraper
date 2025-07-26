#!/usr/bin/env python3

import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re
from urllib.parse import urljoin

load_dotenv()

class SiteSpecificAnalyzer:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()

    def fetch_page(self, url: str) -> BeautifulSoup:
        """Fetch page using ScraperAPI"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'country_code': 'us',
        }
        
        print(f"🔍 Fetching: {url}")
        response = self.session.get(self.base_url, params=params, timeout=90)
        response.raise_for_status()
        
        return BeautifulSoup(response.text, 'html.parser')

    def analyze_quietlight(self):
        """ANALYZE QUIETLIGHT: Find why we're getting 0 results"""
        print("\n" + "="*80)
        print("🔍 ANALYZING QUIETLIGHT (WORST PERFORMING)")
        print("="*80)
        
        url = "https://quietlight.com/amazon-fba-businesses-for-sale/"
        soup = self.fetch_page(url)
        
        print(f"\n📄 PAGE TITLE: {soup.title.string if soup.title else 'No title'}")
        print(f"📏 PAGE SIZE: {len(soup.get_text())} characters")
        
        # Check what we're currently looking for vs what exists
        print(f"\n🔍 CURRENT SELECTORS ANALYSIS:")
        
        current_selectors = [
            'div.listing-item',
            'article.post', 
            'div[class*="business"]',
            'div[class*="listing"]',
            'a[href*="/listings/"]'
        ]
        
        for selector in current_selectors:
            elements = soup.select(selector)
            print(f"  {selector}: {len(elements)} elements found")
            if elements:
                for i, elem in enumerate(elements[:3]):
                    text_preview = elem.get_text().strip()[:100]
                    print(f"    [{i+1}] {text_preview}...")
        
        # Analyze page structure
        print(f"\n🏗️  PAGE STRUCTURE ANALYSIS:")
        
        # Find all divs with classes
        divs_with_classes = soup.find_all('div', class_=True)
        class_patterns = {}
        
        for div in divs_with_classes:
            classes = ' '.join(div.get('class', []))
            if classes:
                class_patterns[classes] = class_patterns.get(classes, 0) + 1
        
        print(f"  Most common div classes:")
        sorted_classes = sorted(class_patterns.items(), key=lambda x: x[1], reverse=True)
        for class_name, count in sorted_classes[:15]:
            print(f"    {class_name}: {count} occurrences")
        
        # Look for business-related content
        print(f"\n💼 BUSINESS CONTENT ANALYSIS:")
        
        # Find text containing business/price indicators
        business_indicators = ['$', 'price', 'revenue', 'amazon', 'fba', 'business', 'sale']
        for indicator in business_indicators:
            elements = soup.find_all(text=re.compile(indicator, re.IGNORECASE))
            print(f"  Text containing '{indicator}': {len(elements)} instances")
        
        # Find all links
        all_links = soup.find_all('a', href=True)
        listing_links = [link for link in all_links if '/listing' in link.get('href', '')]
        print(f"  Links containing '/listing': {len(listing_links)}")
        
        if listing_links:
            print(f"  Sample listing links:")
            for link in listing_links[:5]:
                href = link.get('href')
                text = link.get_text().strip()[:50]
                print(f"    {href} -> {text}")
        
        # Check for JavaScript/dynamic content
        scripts = soup.find_all('script')
        print(f"\n⚡ JAVASCRIPT ANALYSIS:")
        print(f"  Script tags found: {len(scripts)}")
        
        for script in scripts:
            if script.string:
                script_content = script.string
                if any(keyword in script_content.lower() for keyword in ['listing', 'business', 'ajax', 'fetch']):
                    print(f"  Found relevant script content (length: {len(script_content)})")
                    break
        
        # Look for potential containers
        print(f"\n📦 POTENTIAL CONTAINERS:")
        container_selectors = [
            'main', 'section', 'article', 'div[id*="content"]', 
            'div[class*="content"]', 'div[class*="main"]', 'div[class*="container"]'
        ]
        
        for selector in container_selectors:
            elements = soup.select(selector)
            if elements:
                print(f"  {selector}: {len(elements)} found")
                for elem in elements[:2]:
                    child_divs = elem.find_all('div')
                    child_links = elem.find_all('a')
                    print(f"    - Contains {len(child_divs)} divs, {len(child_links)} links")

    def analyze_flippa(self):
        """ANALYZE FLIPPA: Find why we're getting 0 results"""
        print("\n" + "="*80)
        print("🔍 ANALYZING FLIPPA (SECOND WORST PERFORMING)")
        print("="*80)
        
        url = "https://flippa.com/buy/monetization/amazon-fba"
        soup = self.fetch_page(url)
        
        print(f"\n📄 PAGE TITLE: {soup.title.string if soup.title else 'No title'}")
        print(f"📏 PAGE SIZE: {len(soup.get_text())} characters")
        
        # Check current selectors
        current_selectors = [
            'div[data-testid="listing-card"]',
            'div.listing-card',
            'div.auction-card',
            'div[class*="ListingCard"]',
            'div[class*="listing"]'
        ]
        
        print(f"\n🔍 CURRENT SELECTORS ANALYSIS:")
        for selector in current_selectors:
            elements = soup.select(selector)
            print(f"  {selector}: {len(elements)} elements found")
        
        # Analyze div structure
        print(f"\n🏗️  DIV STRUCTURE ANALYSIS:")
        all_divs = soup.find_all('div')
        print(f"  Total divs: {len(all_divs)}")
        
        # Find divs with data attributes (common in React apps)
        data_divs = soup.find_all('div', attrs=lambda x: x and any(attr.startswith('data-') for attr in x))
        print(f"  Divs with data attributes: {len(data_divs)}")
        
        if data_divs:
            print(f"  Sample data attributes:")
            for div in data_divs[:10]:
                attrs = {k: v for k, v in div.attrs.items() if k.startswith('data-')}
                if attrs:
                    print(f"    {attrs}")
        
        # Check for React/Vue components
        print(f"\n⚛️  FRONTEND FRAMEWORK ANALYSIS:")
        react_indicators = soup.find_all(attrs=lambda x: x and any('react' in str(v).lower() for v in x.values()))
        vue_indicators = soup.find_all(attrs=lambda x: x and any('vue' in str(v).lower() for v in x.values()))
        
        print(f"  React indicators: {len(react_indicators)}")
        print(f"  Vue indicators: {len(vue_indicators)}")
        
        # Look for JSON data
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and ('listing' in script.string.lower() or 'auction' in script.string.lower()):
                print(f"  Found script with listing data (length: {len(script.string)})")
                if len(script.string) < 1000:
                    print(f"    Content preview: {script.string[:200]}...")
                break

    def analyze_websiteproperties(self):
        """ANALYZE WEBSITEPROPERTIES: Find why only 4 results"""
        print("\n" + "="*80)
        print("🔍 ANALYZING WEBSITEPROPERTIES (THIRD WORST)")  
        print("="*80)
        
        url = "https://websiteproperties.com/amazon-fba-business-for-sale/"
        soup = self.fetch_page(url)
        
        print(f"\n📄 PAGE TITLE: {soup.title.string if soup.title else 'No title'}")
        
        # Check what we found vs what might exist
        current_selectors = ['div.listing', 'div.business', 'div[class*="listing"]', 'article']
        
        print(f"\n🔍 CURRENT SELECTORS ANALYSIS:")
        for selector in current_selectors:
            elements = soup.select(selector)
            print(f"  {selector}: {len(elements)} elements found")
            if elements:
                for i, elem in enumerate(elements[:3]):
                    text = elem.get_text().strip()[:80]
                    print(f"    [{i+1}] {text}...")
        
        # Look for pagination or more content
        print(f"\n📄 PAGINATION ANALYSIS:")
        pagination_selectors = [
            'nav', 'div[class*="pag"]', 'a[class*="next"]', 'a[class*="more"]',
            'button[class*="load"]', 'div[class*="load"]'
        ]
        
        for selector in pagination_selectors:
            elements = soup.select(selector)
            if elements:
                print(f"  {selector}: {len(elements)} found")
                for elem in elements:
                    text = elem.get_text().strip()
                    if text:
                        print(f"    Text: {text}")

    def generate_fixes(self):
        """Generate specific fixes based on analysis"""
        print("\n" + "="*80)
        print("🔧 GENERATING SITE-SPECIFIC FIXES")
        print("="*80)
        
        print("""
Based on analysis, here are the fixes needed:

1. QUIETLIGHT FIXES:
   - Use JavaScript rendering (render=true)
   - Update selectors based on actual HTML structure
   - Look for AJAX endpoints or API calls
   - Check for infinite scroll implementation

2. FLIPPA FIXES:
   - Definitely needs JavaScript rendering
   - Look for React/Vue component data
   - Find proper data-testid attributes
   - May need API endpoint discovery

3. WEBSITEPROPERTIES FIXES:
   - Improve container detection
   - Add pagination handling
   - Better link extraction
   - Check for load-more functionality

4. INVESTORS.CLUB FIXES:
   - Verify working URLs (some 404s found)
   - Improve content extraction
   - Better categorization
        """)

def main():
    analyzer = SiteSpecificAnalyzer()
    
    print("🔍 STARTING SITE-SPECIFIC ANALYSIS")
    print("Will analyze each failing site to understand their structure...")
    
    try:
        # Analyze worst performers first
        analyzer.analyze_quietlight()
        analyzer.analyze_flippa() 
        analyzer.analyze_websiteproperties()
        analyzer.generate_fixes()
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")

if __name__ == "__main__":
    main() 