#!/usr/bin/env python3
"""Test Empire Flippers data extraction"""

from bs4 import BeautifulSoup
import re

def extract_price(text):
    """Extract price from text"""
    price_patterns = [
        r'\$[\d,]+(?:\.\d{2})?',
        r'\$\d+(?:\.\d+)?[KkMm]',
    ]
    
    for pattern in price_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group()
    return ""

def extract_profit(text):
    """Extract profit information from text"""
    profit_patterns = [
        r'profit[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
        r'net[:\s]+\$?[\d,]+(?:\.\d+)?[KkMm]?',
        r'monthly net profit[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
    ]
    
    for pattern in profit_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group()
    return ""

# Load saved HTML
with open('empire_flippers_raw.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')

# Find all listing items
listings = soup.select('div.listing-item')
print(f"Found {len(listings)} listing items")

# Try to extract data from first few listings
for i, listing in enumerate(listings[:5]):
    print(f"\n--- Listing {i+1} ---")
    
    # Try to find business description or title
    desc_elem = listing.select_one('.description, .business-description, p')
    if desc_elem:
        desc_text = desc_elem.get_text().strip()
        print(f"Description: {desc_text[:100]}...")
    
    # Look for niches/categories
    niche_elem = listing.select_one('.top-info-niches span, .niches')
    if niche_elem:
        niches = niche_elem.get_text().strip()
        print(f"Niches: {niches}")
    
    # Look for price info
    full_text = listing.get_text()
    price = extract_price(full_text)
    if price:
        print(f"Price: {price}")
    
    profit = extract_profit(full_text)
    if profit:
        print(f"Profit: {profit}")
    
    # Look for links
    link = listing.select_one('a[href*="/listing/"]')
    if link:
        print(f"URL: https://empireflippers.com{link['href']}")
    
    # Look for monetization info
    if 'amazon fba' in full_text.lower():
        print("Monetization: Amazon FBA")

# Also check for the new structure with metrics
print("\n\nChecking for metric structures...")
metrics = soup.select('.metric-item')
print(f"Found {len(metrics)} metric items")

for metric in metrics[:10]:
    label = metric.select_one('.label')
    value = metric.select_one('.value')
    if label and value:
        print(f"{label.get_text()}: {value.get_text()}")