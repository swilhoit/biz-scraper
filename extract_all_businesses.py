#!/usr/bin/env python3
"""
Extract ALL Businesses From Cached HTML
Get back to the thousands of businesses while avoiding placeholders
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

def extract_robust_financials(text: str) -> Dict[str, str]:
    """Extract financial data avoiding known placeholders"""
    
    # Financial patterns
    price_patterns = [
        r'asking[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'price[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'listed[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(?:asking|price|listed)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:asking|price|listed)',
    ]
    
    revenue_patterns = [
        r'revenue[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'sales[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'gross[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'monthly[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'annual[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:revenue|sales|monthly|annual)',
    ]
    
    profit_patterns = [
        r'profit[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'cash\s*flow[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'sde[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'ebitda[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'earnings[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:profit|sde|ebitda|cash\s*flow)',
    ]
    
    # Known placeholder values to completely avoid
    placeholder_values = [
        '250000', '250,000', '$250,000',
        '500004', '500,004', '$500,004', 
        '2022', '2021', '2020', '2019',
    ]
    
    def find_financial_match(patterns: List[str]) -> str:
        """Find financial match avoiding placeholders"""
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = next((m for m in match if m and m.strip()), match[-1])
                
                clean_match = re.sub(r'[^\d.,KkMm]', '', str(match))
                
                # Skip known placeholder values
                if any(placeholder in clean_match.replace(',', '') for placeholder in placeholder_values):
                    continue
                
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
                        
                        # Reasonable business value ranges
                        if 1000 <= value <= 100000000:  # $1K to $100M
                            return f"${value:,.0f}"
                    except:
                        continue
        return ""
    
    return {
        'price': find_financial_match(price_patterns),
        'revenue': find_financial_match(revenue_patterns),
        'profit': find_financial_match(profit_patterns)
    }

def extract_from_bizquest_cached() -> List[Dict]:
    """Extract from BizQuest cached files (our best source)"""
    print("🚀 EXTRACTING FROM BIZQUEST (Best Source)...")
    
    listings = []
    
    # BizQuest cached files
    bizquest_files = [
        "bizquest_com_2bb18b23d3cd580244817c8da0ed6237.html",
        "bizquest_com_d02612ec22c5544c84e6ae0d5831f45e.html",
        "bizquest_com_83f22f5630e3f8268e6acf6fad366af0.html", 
        "bizquest_com_f127c31d144f0bd5a552b98d96a90ff5.html",
        "bizquest_com_0728f014b7438ac75ce8c44aef35ef21.html",
    ]
    
    for filename in bizquest_files:
        html = load_cached_html(filename)
        if not html:
            continue
            
        # Skip small files (likely error pages)
        if len(html) < 10000:
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # BizQuest uses consistent structure - find business cards
        business_cards = soup.select('div[class*="card"], div[class*="listing"], div[class*="business"], article')
        
        print(f"BizQuest ({filename}): Found {len(business_cards)} potential cards")
        
        extracted_count = 0
        seen_urls = set()
        
        for card in business_cards:
            try:
                # Must have a link to individual business
                link_elem = card.find('a', href=True)
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                
                # Must be an individual business URL (not category/navigation)
                if not any(keyword in href for keyword in ['/business', '/listing', '/sale', 'opportunity']):
                    continue
                
                # Skip navigation/category pages
                if any(skip in href.lower() for skip in ['category', 'search', 'filter', 'page', 'sort']):
                    continue
                
                url = urljoin("https://www.bizquest.com", href)
                
                # Skip duplicates
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Extract title
                title = link_elem.get_text().strip()
                if not title or len(title) < 10:
                    # Try other title sources
                    title_elem = card.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                    if title_elem:
                        title = title_elem.get_text().strip()
                    else:
                        title = f"Business Listing #{extracted_count + 1}"
                
                # Get card text
                card_text = card.get_text()
                
                # Extract financial data
                financials = extract_robust_financials(card_text)
                
                # Create listing
                listing = {
                    'source': 'BizQuest',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'], 
                    'profit': financials['profit'],
                    'description': re.sub(r'\s+', ' ', card_text[:400]).strip(),
                    'url': url,
                    'category': 'General Business'
                }
                listings.append(listing)
                extracted_count += 1
                
            except Exception as e:
                continue
        
        print(f"✅ BizQuest ({filename}): Extracted {extracted_count} businesses")
    
    return listings

def extract_from_quietlight_cached() -> List[Dict]:
    """Extract from QuietLight cached files"""
    print("🚀 EXTRACTING FROM QUIETLIGHT...")
    
    listings = []
    
    # QuietLight cached files
    quietlight_files = [
        "quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html",  # amazon-fba
        "quietlight_com_769ba6690f9f17e6cd331d74d0f04c90.html",  # ecommerce
        "quietlight_com_755d4f3bf7f83f7f482bba171ef5b079.html",  # saas
    ]
    
    for filename in quietlight_files:
        html = load_cached_html(filename)
        if not html:
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # QuietLight business containers
        business_containers = soup.select('div[class*="listing"], article, div[class*="business"], div[class*="card"]')
        
        print(f"QuietLight ({filename}): Found {len(business_containers)} potential containers")
        
        extracted_count = 0
        seen_urls = set()
        
        for container in business_containers:
            try:
                # Must have a link to actual listing
                link_elem = container.find('a', href=True)
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                
                # Must be a listing URL
                if not any(keyword in href for keyword in ['/listing', '/business', '/sale']):
                    continue
                
                url = urljoin("https://quietlight.com", href)
                
                # Skip duplicates
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Extract title
                title = link_elem.get_text().strip()
                if not title or len(title) < 10:
                    title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                    if title_elem:
                        title = title_elem.get_text().strip()
                    else:
                        continue
                
                # Get container text
                container_text = container.get_text()
                
                # Extract financial data
                financials = extract_robust_financials(container_text)
                
                # Determine category
                category = 'General Business'
                if 'amazon' in container_text.lower() or 'fba' in container_text.lower():
                    category = 'Amazon FBA'
                elif 'ecommerce' in container_text.lower():
                    category = 'Ecommerce'
                elif 'saas' in container_text.lower() or 'software' in container_text.lower():
                    category = 'SaaS'
                
                listing = {
                    'source': 'QuietLight',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': re.sub(r'\s+', ' ', container_text[:400]).strip(),
                    'url': url,
                    'category': category
                }
                listings.append(listing)
                extracted_count += 1
                
            except Exception as e:
                continue
        
        print(f"✅ QuietLight ({filename}): Extracted {extracted_count} businesses")
    
    return listings

def extract_from_other_cached() -> List[Dict]:
    """Extract from other cached sources"""
    print("🚀 EXTRACTING FROM OTHER SOURCES...")
    
    listings = []
    
    # Other sources
    other_sources = [
        ("BizBuySell", "bizbuysell_com_b106cc7129ce583113a76ced7e280513.html"),
        ("Investors", "investors_club_295bd3c8770bd3c900945f9ec6ab9028.html"),
        ("WebsiteProperties", "websiteproperties_com_96edeadf9264cd3bc2d4d63394d49206.html"),
    ]
    
    for source_name, filename in other_sources:
        html = load_cached_html(filename)
        if not html:
            continue
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find business containers
        containers = soup.select('div[class*="business"], div[class*="listing"], div[class*="card"], article')
        
        print(f"{source_name}: Found {len(containers)} potential containers")
        
        extracted_count = 0
        seen_urls = set()
        
        for container in containers:
            try:
                # Must have a link
                link_elem = container.find('a', href=True)
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                
                # Skip category/navigation pages
                if any(skip in href.lower() for skip in ['category', 'search', 'filter', 'page', 'sort']):
                    continue
                
                # Build URL
                if source_name == "BizBuySell":
                    base_url = "https://www.bizbuysell.com"
                elif source_name == "Investors":
                    base_url = "https://investors.club"
                else:
                    base_url = "https://websiteproperties.com"
                
                url = urljoin(base_url, href)
                
                # Skip duplicates
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Extract title
                title = link_elem.get_text().strip()
                if not title or len(title) < 10:
                    title_elem = container.find(['h1', 'h2', 'h3', 'h4'])
                    if title_elem:
                        title = title_elem.get_text().strip()
                    else:
                        continue
                
                # Skip generic category titles
                if any(generic in title.lower() for generic in ['for sale', 'businesses', 'buy', 'browse']):
                    if len(title) < 40:  # Short generic titles
                        continue
                
                # Get container text
                container_text = container.get_text()
                
                # Extract financial data
                financials = extract_robust_financials(container_text)
                
                # Determine category
                category = 'General Business'
                if any(keyword in container_text.lower() for keyword in ['amazon', 'fba']):
                    category = 'Amazon FBA'
                elif 'ecommerce' in container_text.lower():
                    category = 'Ecommerce'
                
                listing = {
                    'source': source_name,
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': re.sub(r'\s+', ' ', container_text[:400]).strip(),
                    'url': url,
                    'category': category
                }
                listings.append(listing)
                extracted_count += 1
                
            except Exception as e:
                continue
        
        print(f"✅ {source_name}: Extracted {extracted_count} businesses")
    
    return listings

def main():
    """Extract ALL businesses from cached HTML"""
    print("🚀 EXTRACTING ALL BUSINESSES FROM CACHED HTML")
    print("="*60)
    print("Getting back to thousands of businesses while avoiding placeholders")
    print("="*60)
    
    all_listings = []
    
    # Extract from all sources
    all_listings.extend(extract_from_bizquest_cached())
    all_listings.extend(extract_from_quietlight_cached())
    all_listings.extend(extract_from_other_cached())
    
    if all_listings:
        # Smart deduplication
        df = pd.DataFrame(all_listings)
        
        print(f"\n📊 ALL BUSINESS RESULTS:")
        print(f"Raw listings extracted: {len(df)}")
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        url_dedup_count = len(df)
        
        df = df.drop_duplicates(subset=['name'], keep='first') 
        final_count = len(df)
        
        print(f"After URL dedup: {url_dedup_count} (-{initial_count - url_dedup_count})")
        print(f"After name dedup: {final_count} (-{url_dedup_count - final_count})")
        print(f"Deduplication rate: {(initial_count - final_count) / initial_count * 100:.1f}%")
        
        # Export results
        df.to_csv('ALL_CACHED_BUSINESSES.csv', index=False)
        
        # Analysis
        price_count = (df['price'] != '').sum()
        revenue_count = (df['revenue'] != '').sum()
        profit_count = (df['profit'] != '').sum()
        
        print(f"\n💰 FINANCIAL DATA:")
        print(f"  Prices: {price_count}/{len(df)} ({price_count/len(df)*100:.1f}%)")
        print(f"  Revenue: {revenue_count}/{len(df)} ({revenue_count/len(df)*100:.1f}%)")
        print(f"  Profit: {profit_count}/{len(df)} ({profit_count/len(df)*100:.1f}%)")
        
        # Source breakdown
        source_counts = df['source'].value_counts()
        print(f"\n📈 SOURCE BREAKDOWN:")
        for source, count in source_counts.items():
            print(f"  {source}: {count:,} businesses")
        
        # Category breakdown
        category_counts = df['category'].value_counts()
        print(f"\n📂 CATEGORY BREAKDOWN:")
        for category, count in category_counts.items():
            print(f"  {category}: {count:,} businesses")
        
        print(f"\n💾 Exported to: ALL_CACHED_BUSINESSES.csv")
        print(f"🎯 Total businesses: {len(df):,} (vs 31 Amazon FBA only)")
        print("💡 Zero API costs - all from cached HTML!")
        
    else:
        print("❌ No businesses found")

if __name__ == "__main__":
    main() 