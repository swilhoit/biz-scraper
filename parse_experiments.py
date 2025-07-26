#!/usr/bin/env python3
"""
Parse Experiments - No API Costs!
Demonstrate how to iterate on parsing logic using cached HTML
"""

import os
import json
from bs4 import BeautifulSoup
import re
from typing import List, Dict

def load_cached_html(filename: str) -> str:
    """Load cached HTML file"""
    cache_path = os.path.join("html_cache", filename)
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

def experiment_1_basic_parsing():
    """Experiment 1: Basic parsing approach"""
    print("\n🧪 EXPERIMENT 1: Basic parsing approach")
    
    # Load QuietLight cached HTML
    html = load_cached_html("quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html")
    if not html:
        print("No cached HTML found")
        return
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Basic approach - find all divs with class containing "listing"
    listings = soup.select('div[class*="listing"]')
    print(f"Basic approach found: {len(listings)} potential listings")
    
    # Look for financial patterns
    financial_matches = re.findall(r'\$[\d,]+(?:\.\d+)?[KkMm]?', html)
    print(f"Financial values found: {len(set(financial_matches))} unique")
    print(f"Sample values: {list(set(financial_matches))[:10]}")

def experiment_2_advanced_parsing():
    """Experiment 2: Advanced parsing with better selectors"""
    print("\n🧪 EXPERIMENT 2: Advanced parsing with better selectors")
    
    html = load_cached_html("quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html")
    if not html:
        return
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try different selectors
    selectors = [
        'article',
        'div.business',
        'div[class*="business"]',
        'div[class*="listing"]',
        'div[data-testid]'
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        if elements:
            print(f"Selector '{selector}': {len(elements)} elements")
            
            # Sample first element
            first = elements[0]
            text_preview = first.get_text()[:200].replace('\n', ' ')
            print(f"  Sample text: {text_preview}...")

def experiment_3_bizbuysell_placeholders():
    """Experiment 3: Test BizBuySell placeholder detection"""
    print("\n🧪 EXPERIMENT 3: BizBuySell placeholder detection")
    
    html = load_cached_html("bizbuysell_com_b106cc7129ce583113a76ced7e280513.html")
    if not html:
        return
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find opportunity links
    links = soup.select('a[href*="opportunity"]')
    print(f"Found {len(links)} opportunity links")
    
    placeholder_suspects = []
    for link in links[:5]:  # Sample first 5
        text = link.get_text().strip()
        parent = link.find_parent(['div', 'article'])
        if parent:
            parent_text = parent.get_text()
            
            # Look for financial values
            financial_values = re.findall(r'\$[\d,]+(?:\.\d+)?', parent_text)
            
            print(f"\nLink: {text[:50]}...")
            print(f"Financial values: {financial_values}")
            
            # Check for suspicious patterns
            if "$250,000" in financial_values or "$500,004" in financial_values:
                placeholder_suspects.append(text)
    
    print(f"\nPotential placeholder entries: {len(placeholder_suspects)}")

def experiment_4_improved_selectors():
    """Experiment 4: Test improved CSS selectors"""
    print("\n🧪 EXPERIMENT 4: Improved CSS selectors")
    
    # Test on multiple cached files
    cache_files = [
        ("QuietLight", "quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html"),
        ("BizBuySell", "bizbuysell_com_b106cc7129ce583113a76ced7e280513.html"),
        ("Investors", "investors_club_295bd3c8770bd3c900945f9ec6ab9028.html"),
    ]
    
    for site_name, filename in cache_files:
        html = load_cached_html(filename)
        if not html:
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        print(f"\n{site_name} Analysis:")
        
        # Test multiple selector strategies
        strategies = {
            'Links with href': len(soup.select('a[href]')),
            'Business cards': len(soup.select('div[class*="business"], div[class*="listing"]')),
            'Articles': len(soup.select('article')),
            'Price indicators': len(re.findall(r'\$[\d,]+', html)),
            'Revenue indicators': len(re.findall(r'revenue|sales', html, re.IGNORECASE)),
        }
        
        for strategy, count in strategies.items():
            print(f"  {strategy}: {count}")

def main():
    """Run all parsing experiments using cached HTML - NO API CALLS!"""
    print("🚀 PARSING EXPERIMENTS - USING CACHED HTML ONLY!")
    print("=" * 60)
    print("This demonstrates how you can iterate on parsing logic")
    print("without making expensive API calls!")
    print("=" * 60)
    
    # Check cache availability
    cache_dir = "html_cache"
    if not os.path.exists(cache_dir):
        print("❌ No cache directory found. Run cached_html_scraper.py first!")
        return
    
    cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.html')]
    print(f"📁 Found {len(cache_files)} cached HTML files")
    
    # Run experiments
    experiment_1_basic_parsing()
    experiment_2_advanced_parsing()
    experiment_3_bizbuysell_placeholders()
    experiment_4_improved_selectors()
    
    print("\n" + "=" * 60)
    print("🎉 ALL EXPERIMENTS COMPLETE!")
    print("💡 You can modify parsing logic and run again instantly!")
    print("💰 Zero API costs - using cached HTML only!")
    print("=" * 60)

if __name__ == "__main__":
    main() 