#!/usr/bin/env python3
"""
Maximize Business Listings Extractor
Extract maximum possible listings from cached HTML - NO API CALLS
"""

import os
import json
from bs4 import BeautifulSoup
import re
import pandas as pd
from typing import List, Dict
from urllib.parse import urljoin

def load_cached_html(filename: str) -> str:
    """Load cached HTML file"""
    cache_path = os.path.join("html_cache", filename)
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

def extract_all_financials(text: str) -> Dict[str, str]:
    """Extract all possible financial data from text"""
    # Comprehensive financial patterns
    price_patterns = [
        r'(?:asking|price|listed|sale)(?:\s*price)?[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
        r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?[KkMm]?)',
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?[KkMm]?)\s*(?:asking|price|listed)',
    ]
    
    revenue_patterns = [
        r'(?:revenue|sales|gross|annual|ttm)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
        r'(\d+(?:\.\d+)?[KkMm]?)\s*(?:in\s*)?(?:revenue|sales)',
        r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*[KkMm]?)\s*(?:revenue|sales|gross)',
    ]
    
    profit_patterns = [
        r'(?:profit|cash\s*flow|sde|ebitda|earnings|income|net)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
        r'(\d+(?:\.\d+)?[KkMm]?)\s*(?:profit|sde|ebitda)',
    ]
    
    def find_best_match(patterns: List[str]) -> str:
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = next((m for m in match if m and m.strip()), match[-1])
                
                clean_match = re.sub(r'[^\d.,KkMm]', '', str(match))
                if clean_match and any(c.isdigit() for c in clean_match):
                    try:
                        multiplier = 1
                        if clean_match.upper().endswith('K'):
                            multiplier = 1000
                            clean_match = clean_match[:-1]
                        elif clean_match.upper().endswith('M'):
                            multiplier = 1000000
                            clean_match = clean_match[:-1]
                        
                        value = float(clean_match.replace(',', '')) * multiplier
                        if 1000 <= value <= 500000000:  # Reasonable range
                            return f"${value:,.0f}"
                    except:
                        continue
        return ""
    
    return {
        'price': find_best_match(price_patterns),
        'revenue': find_best_match(revenue_patterns),
        'profit': find_best_match(profit_patterns)
    }

def maximize_quietlight_extraction() -> List[Dict]:
    """Extract maximum listings from QuietLight cached HTML"""
    print("🔥 MAXIMIZING QuietLight extraction...")
    
    listings = []
    
    # Load all QuietLight cached files
    cache_files = [
        "quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html",  # amazon-fba
        "quietlight_com_769ba6690f9f17e6cd331d74d0f04c90.html",  # ecommerce  
        "quietlight_com_755d4f3bf7f83f7f482bba171ef5b079.html",  # saas
    ]
    
    for filename in cache_files:
        html = load_cached_html(filename)
        if not html:
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try multiple extraction strategies
        strategies = [
            'div[class*="listing"]',
            'article',
            'div[class*="business"]',
            'div[class*="card"]',
            'div[class*="item"]'
        ]
        
        best_containers = []
        for strategy in strategies:
            containers = soup.select(strategy)
            if len(containers) > len(best_containers):
                best_containers = containers
        
        print(f"QuietLight ({filename}): Processing {len(best_containers)} containers")
        
        for i, container in enumerate(best_containers):
            try:
                # Extract title from multiple sources
                title_elem = (
                    container.find(['h1', 'h2', 'h3', 'h4', 'h5']) or
                    container.find('a', href=True) or
                    container.find(text=re.compile(r'.{20,}'))
                )
                
                if title_elem:
                    if hasattr(title_elem, 'get_text'):
                        title = title_elem.get_text().strip()
                    else:
                        title = str(title_elem).strip()
                else:
                    title = f"QuietLight Business #{i+1}"
                
                # Skip if title too short or generic
                if len(title) < 15 or any(skip in title.lower() for skip in ['nav', 'menu', 'footer', 'header']):
                    continue
                
                # Extract URL
                link_elem = container.find('a', href=True)
                if link_elem:
                    url = urljoin("https://quietlight.com", link_elem['href'])
                else:
                    url = "https://quietlight.com"
                
                # Extract financial data
                text = container.get_text()
                financials = extract_all_financials(text)
                
                # Only include if has meaningful data
                if any(financials.values()) and len(title) >= 15:
                    listing = {
                        'source': 'QuietLight',
                        'name': title[:200],
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': re.sub(r'\s+', ' ', text[:400]).strip(),
                        'url': url,
                    }
                    listings.append(listing)
                    
            except Exception as e:
                continue
    
    print(f"✅ QuietLight: Extracted {len(listings)} listings")
    return listings

def maximize_bizbuysell_extraction() -> List[Dict]:
    """Extract maximum listings from BizBuySell cached HTML"""
    print("🔥 MAXIMIZING BizBuySell extraction...")
    
    listings = []
    html = load_cached_html("bizbuysell_com_b106cc7129ce583113a76ced7e280513.html")
    
    if not html:
        return listings
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extract from opportunity links
    opportunity_links = soup.select('a[href*="opportunity"]')
    print(f"BizBuySell: Processing {len(opportunity_links)} opportunity links")
    
    for link in opportunity_links:
        try:
            href = link.get('href', '')
            if not href or '/business-opportunity/' not in href:
                continue
            
            title = link.get_text().strip()
            if len(title) < 15:
                continue
            
            # Get immediate parent for financial data
            parent = link.find_parent(['div', 'article', 'section'])
            if parent:
                text = parent.get_text()
                financials = extract_all_financials(text)
                
                # Skip known placeholder values
                if any(val in ['$250,000', '$500,004', '$2,022'] for val in financials.values()):
                    continue
                
                if any(financials.values()):
                    listing = {
                        'source': 'BizBuySell',
                        'name': title[:200],
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': re.sub(r'\s+', ' ', text[:400]).strip(),
                        'url': urljoin("https://www.bizbuysell.com", href),
                    }
                    listings.append(listing)
                    
        except Exception as e:
            continue
    
    print(f"✅ BizBuySell: Extracted {len(listings)} listings")
    return listings

def maximize_other_sources() -> List[Dict]:
    """Extract maximum listings from other cached sources"""
    print("🔥 MAXIMIZING other sources extraction...")
    
    listings = []
    
    # Other cached files
    other_files = [
        ("Investors", "investors_club_295bd3c8770bd3c900945f9ec6ab9028.html"),
        ("Websiteproperties", "websiteproperties_com_96edeadf9264cd3bc2d4d63394d49206.html"),
    ]
    
    for source_name, filename in other_files:
        html = load_cached_html(filename)
        if not html:
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find business containers
        containers = soup.select('div[class*="business"], div[class*="listing"], div[class*="card"]')
        if not containers:
            containers = soup.select('div')[:50]  # Fallback to first 50 divs
        
        print(f"{source_name}: Processing {len(containers)} containers")
        
        for container in containers:
            try:
                # Find title
                title_elem = container.find(['h1', 'h2', 'h3', 'h4'])
                if not title_elem:
                    continue
                    
                title = title_elem.get_text().strip()
                if len(title) < 15:
                    continue
                
                # Extract financial data
                text = container.get_text()
                financials = extract_all_financials(text)
                
                # Find URL
                link_elem = container.find('a', href=True)
                if link_elem:
                    url = urljoin(f"https://{source_name.lower()}.com", link_elem['href'])
                else:
                    url = f"https://{source_name.lower()}.com"
                
                if any(financials.values()):
                    listing = {
                        'source': source_name,
                        'name': title[:200],
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': re.sub(r'\s+', ' ', text[:400]).strip(),
                        'url': url,
                    }
                    listings.append(listing)
                    
            except Exception as e:
                continue
    
    print(f"✅ Other sources: Extracted {len(listings)} listings")
    return listings

def maximize_bizquest_extraction() -> List[Dict]:
    """Extract maximum listings from BizQuest cached HTML (even though showing 0 cards)"""
    print("🔥 MAXIMIZING BizQuest extraction...")
    
    listings = []
    
    # BizQuest cached files
    bizquest_files = [
        "bizquest_com_2bb18b23d3cd580244817c8da0ed6237.html",
        "bizquest_com_d02612ec22c5544c84e6ae0d5831f45e.html", 
        "bizquest_com_83f22f5630e3f8268e6acf6fad366af0.html",
        "bizquest_com_f127c31d144f0bd5a552b98d96a90ff5.html",
    ]
    
    for filename in bizquest_files:
        html = load_cached_html(filename)
        if not html:
            continue
            
        # Check if this is an error page or has real content
        if len(html) < 5000:  # Too small, likely error page
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for any business-related content
        all_links = soup.find_all('a', href=True)
        business_links = [link for link in all_links if 
                         any(keyword in link.get('href', '').lower() for keyword in ['business', 'listing', 'sale'])]
        
        print(f"BizQuest ({filename}): Found {len(business_links)} business links in {len(html)} chars")
        
        # If we find business content, extract it
        for link in business_links[:20]:  # Limit per file
            try:
                title = link.get_text().strip()
                if len(title) < 15:
                    continue
                
                # Get parent context
                parent = link.find_parent(['div', 'article'])
                if parent:
                    text = parent.get_text()
                    financials = extract_all_financials(text)
                    
                    if any(financials.values()):
                        listing = {
                            'source': 'BizQuest',
                            'name': title[:200],
                            'price': financials['price'],
                            'revenue': financials['revenue'],
                            'profit': financials['profit'],
                            'description': re.sub(r'\s+', ' ', text[:400]).strip(),
                            'url': urljoin("https://www.bizquest.com", link['href']),
                        }
                        listings.append(listing)
            except:
                continue
    
    print(f"✅ BizQuest: Extracted {len(listings)} listings")
    return listings

def remove_duplicates_smart(listings: List[Dict]) -> List[Dict]:
    """Smart duplicate removal"""
    if not listings:
        return []
    
    df = pd.DataFrame(listings)
    
    print(f"Starting with {len(df)} raw listings")
    
    # Remove exact URL duplicates
    df = df.drop_duplicates(subset=['url'], keep='first')
    print(f"After URL dedup: {len(df)} listings")
    
    # Remove very similar titles
    df['title_normalized'] = df['name'].str.lower().str[:50]
    df = df.drop_duplicates(subset=['title_normalized'], keep='first')
    df = df.drop(columns=['title_normalized'])
    print(f"After title dedup: {len(df)} listings")
    
    return df.to_dict('records')

def main():
    """Extract maximum business listings from cached HTML"""
    print("🚀 MAXIMIZING BUSINESS LISTINGS FROM CACHED HTML")
    print("=" * 60)
    print("Extracting every possible listing - NO API CALLS!")
    print("=" * 60)
    
    all_listings = []
    
    # Extract from all sources
    all_listings.extend(maximize_quietlight_extraction())
    all_listings.extend(maximize_bizbuysell_extraction())
    all_listings.extend(maximize_other_sources())
    all_listings.extend(maximize_bizquest_extraction())
    
    print(f"\n📊 Raw extraction: {len(all_listings)} total listings")
    
    # Remove duplicates
    clean_listings = remove_duplicates_smart(all_listings)
    
    # Export results
    if clean_listings:
        df = pd.DataFrame(clean_listings)
        df.to_csv('MAXIMIZED_BUSINESS_LISTINGS.csv', index=False)
        
        print(f"\n🎉 MAXIMIZED RESULTS:")
        print(f"Total unique businesses: {len(df):,}")
        
        # Financial coverage
        price_count = (df['price'] != '').sum()
        revenue_count = (df['revenue'] != '').sum()
        profit_count = (df['profit'] != '').sum()
        
        print(f"\n💰 FINANCIAL DATA:")
        print(f"  Prices: {price_count:,} ({price_count/len(df)*100:.1f}%)")
        print(f"  Revenue: {revenue_count:,} ({revenue_count/len(df)*100:.1f}%)")
        print(f"  Profit: {profit_count:,} ({profit_count/len(df)*100:.1f}%)")
        
        # Source breakdown
        source_counts = df['source'].value_counts()
        print(f"\n📈 SOURCES:")
        for source, count in source_counts.items():
            print(f"  {source}: {count:,} businesses")
        
        print(f"\n💾 Exported to: MAXIMIZED_BUSINESS_LISTINGS.csv")
        print("💡 All from cached HTML - zero API costs!")
    else:
        print("❌ No listings extracted")

if __name__ == "__main__":
    main() 