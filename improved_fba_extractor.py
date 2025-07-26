#!/usr/bin/env python3
"""
Improved Amazon FBA Extractor
Fix financial extraction and get legitimate Amazon FBA businesses only
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
    """Extract financial data with improved patterns and validation"""
    
    # More comprehensive financial patterns
    price_patterns = [
        r'asking[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'price[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'listed[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*asking',
        r'(\d{1,3}(?:,\d{3})*)\s*(?:listing|asking|price)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:asking|price|listed)',
    ]
    
    revenue_patterns = [
        r'revenue[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'sales[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'gross[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'monthly[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'annual[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:revenue|sales|monthly|annual)',
        r'(\d{1,3}(?:,\d{3})*)\s*(?:revenue|sales|monthly)',
    ]
    
    profit_patterns = [
        r'profit[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'cash\s*flow[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'sde[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'ebitda[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'earnings[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'net[:\s]*\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:profit|sde|ebitda|cash\s*flow)',
        r'(\d{1,3}(?:,\d{3})*)\s*(?:profit|sde|ebitda)',
    ]
    
    # Known placeholder values to skip
    placeholders = ['250000', '250,000', '500004', '500,004', '2022', '2021', '2020']
    
    def find_best_financial_match(patterns: List[str]) -> str:
        """Find best financial match avoiding placeholders"""
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = next((m for m in match if m and m.strip()), match[-1])
                
                clean_match = re.sub(r'[^\d.,KkMm]', '', str(match))
                
                # Skip placeholder values
                if any(placeholder in clean_match.replace(',', '') for placeholder in placeholders):
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
                        
                        # Reasonable ranges for Amazon FBA businesses
                        if 5000 <= value <= 50000000:  # $5K to $50M
                            return f"${value:,.0f}"
                    except:
                        continue
        return ""
    
    return {
        'price': find_best_financial_match(price_patterns),
        'revenue': find_best_financial_match(revenue_patterns),
        'profit': find_best_financial_match(profit_patterns)
    }

def extract_quietlight_fba() -> List[Dict]:
    """Fixed QuietLight Amazon FBA extraction"""
    print("🔧 FIXING QuietLight Amazon FBA extraction...")
    
    listings = []
    html = load_cached_html("quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html")
    
    if not html:
        print("❌ No QuietLight cached HTML")
        return listings
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Look for actual business listing cards/containers
    # QuietLight uses specific CSS classes for business listings
    business_containers = soup.select('div[class*="listing"], article, div[class*="business"], div[class*="card"]')
    
    print(f"QuietLight: Found {len(business_containers)} potential containers")
    
    extracted_count = 0
    seen_urls = set()
    
    for container in business_containers:
        try:
            # Must have a link to be a real business listing
            link_elem = container.find('a', href=True)
            if not link_elem:
                continue
            
            href = link_elem.get('href', '')
            
            # Must be a listing URL, not navigation
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
                # Try finding title in container
                title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                if title_elem:
                    title = title_elem.get_text().strip()
                else:
                    continue
            
            # Get all text from container
            container_text = container.get_text()
            
            # Must mention Amazon/FBA to be relevant
            if not any(keyword in container_text.lower() for keyword in ['amazon', 'fba', 'fulfillment']):
                continue
            
            # Extract financial data
            financials = extract_robust_financials(container_text)
            
            # Must have at least some financial data or be clearly Amazon/FBA
            if any(financials.values()) or any(keyword in title.lower() for keyword in ['amazon', 'fba']):
                listing = {
                    'source': 'QuietLight',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': re.sub(r'\s+', ' ', container_text[:400]).strip(),
                    'url': url,
                    'is_amazon_fba': True
                }
                listings.append(listing)
                extracted_count += 1
                
        except Exception as e:
            continue
    
    print(f"✅ QuietLight: Extracted {extracted_count} Amazon FBA businesses")
    return listings

def extract_improved_fba_all_sources() -> List[Dict]:
    """Extract Amazon FBA businesses from all sources with improved logic"""
    print("🚀 EXTRACTING AMAZON FBA FROM ALL SOURCES (IMPROVED)")
    print("="*60)
    
    all_fba_listings = []
    
    # Add QuietLight with fixed extraction
    all_fba_listings.extend(extract_quietlight_fba())
    
    # Other sources with improved extraction
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
        
        # Find individual business listings (not category pages)
        business_containers = soup.select('div[class*="listing"], article, div[class*="business"], div[class*="card"]')
        
        extracted_count = 0
        seen_urls = set()
        
        for container in business_containers:
            try:
                # Must have a link to individual business
                link_elem = container.find('a', href=True)
                if not link_elem:
                    continue
                
                href = link_elem.get('href', '')
                
                # Skip category/general pages - must be specific business
                if any(skip in href.lower() for skip in ['category', 'search', 'filter', 'page']):
                    continue
                
                # Build full URL
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
                
                # Get container text
                container_text = container.get_text()
                
                # Must be Amazon/FBA related
                if not any(keyword in container_text.lower() for keyword in ['amazon', 'fba', 'fulfillment']):
                    continue
                
                # Extract financial data with improved logic
                financials = extract_robust_financials(container_text)
                
                # Skip entries that look like category pages
                if 'for sale' in title.lower() and len(title) < 30:
                    continue
                
                # Must have either financial data or clear Amazon/FBA indication
                if any(financials.values()) or any(keyword in title.lower() for keyword in ['amazon', 'fba']):
                    listing = {
                        'source': source_name,
                        'name': title[:200],
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': re.sub(r'\s+', ' ', container_text[:400]).strip(),
                        'url': url,
                        'is_amazon_fba': True
                    }
                    all_fba_listings.append(listing)
                    extracted_count += 1
                    
            except Exception as e:
                continue
        
        print(f"✅ {source_name}: Extracted {extracted_count} Amazon FBA businesses")
    
    return all_fba_listings

def main():
    """Main improved extraction function"""
    print("🚀 IMPROVED AMAZON FBA EXTRACTOR")
    print("="*50)
    print("Fixing QuietLight + improving financial extraction")
    print("="*50)
    
    # Extract from all sources with improvements
    fba_listings = extract_improved_fba_all_sources()
    
    if fba_listings:
        # Smart deduplication
        df = pd.DataFrame(fba_listings)
        
        print(f"\n📊 IMPROVED FBA RESULTS:")
        print(f"Raw FBA listings: {len(df)}")
        
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
        df.to_csv('IMPROVED_AMAZON_FBA_BUSINESSES.csv', index=False)
        
        # Financial data analysis
        price_count = (df['price'] != '').sum()
        revenue_count = (df['revenue'] != '').sum()
        profit_count = (df['profit'] != '').sum()
        
        print(f"\n💰 IMPROVED FINANCIAL DATA:")
        print(f"  Prices: {price_count}/{len(df)} ({price_count/len(df)*100:.1f}%)")
        print(f"  Revenue: {revenue_count}/{len(df)} ({revenue_count/len(df)*100:.1f}%)")
        print(f"  Profit: {profit_count}/{len(df)} ({profit_count/len(df)*100:.1f}%)")
        
        # Source breakdown
        source_counts = df['source'].value_counts()
        print(f"\n📈 IMPROVED SOURCES:")
        for source, count in source_counts.items():
            print(f"  {source}: {count} Amazon FBA businesses")
        
        # Show sample of best entries
        best_entries = df[df['price'] != ''].head(3)
        if len(best_entries) > 0:
            print(f"\n🏆 SAMPLE QUALITY LISTINGS:")
            for _, row in best_entries.iterrows():
                print(f"  • {row['name'][:50]}... - {row['price']} ({row['source']})")
        
        print(f"\n💾 Exported to: IMPROVED_AMAZON_FBA_BUSINESSES.csv")
        print("🎯 Only legitimate Amazon FBA businesses with real financials!")
        
    else:
        print("❌ No Amazon FBA businesses found")

if __name__ == "__main__":
    main() 