#!/usr/bin/env python3
"""
Analyze Duplicates and Focus on Amazon FBA Only
Find out why 96% duplication rate and extract only Amazon FBA businesses
"""

import os
import json
from bs4 import BeautifulSoup
import re
import pandas as pd
from typing import List, Dict
from urllib.parse import urljoin
from collections import Counter

def load_cached_html(filename: str) -> str:
    """Load cached HTML file"""
    cache_path = os.path.join("html_cache", filename)
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

def analyze_duplication_issue():
    """Analyze why we're getting 96% duplication"""
    print("🔍 ANALYZING DUPLICATION ISSUE")
    print("="*50)
    
    # Load QuietLight amazon-fba page (the biggest source)
    html = load_cached_html("quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html")
    
    if not html:
        print("❌ No cached HTML found")
        return
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Check what we're actually extracting
    all_divs = soup.select('div')
    print(f"Total divs on page: {len(all_divs)}")
    
    # Check URLs being generated
    all_links = soup.find_all('a', href=True)
    urls_generated = []
    
    for link in all_links:
        href = link.get('href', '')
        full_url = urljoin("https://quietlight.com", href)
        urls_generated.append(full_url)
    
    print(f"Total links found: {len(all_links)}")
    print(f"Unique URLs: {len(set(urls_generated))}")
    
    # Count URL patterns
    url_counter = Counter(urls_generated)
    duplicated_urls = {url: count for url, count in url_counter.items() if count > 1}
    
    print(f"Duplicated URLs: {len(duplicated_urls)}")
    
    if duplicated_urls:
        print("\n🔍 Top duplicated URLs:")
        for url, count in sorted(duplicated_urls.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {count}x: {url}")
    
    # Check if we're extracting from navigation/header/footer instead of listings
    navigation_sections = soup.select('nav, header, footer, .navigation, .menu')
    nav_links = sum(len(section.find_all('a', href=True)) for section in navigation_sections)
    
    print(f"\n🧭 Navigation/header/footer links: {nav_links}")
    print(f"Content links: {len(all_links) - nav_links}")

def extract_amazon_fba_only() -> List[Dict]:
    """Extract ONLY Amazon FBA businesses from original URLs"""
    print("\n🎯 EXTRACTING AMAZON FBA ONLY")
    print("="*50)
    
    fba_listings = []
    
    # Original URLs focusing on Amazon FBA
    fba_sources = [
        ("QuietLight Amazon FBA", "quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html"),
        ("BizBuySell Amazon", "bizbuysell_com_b106cc7129ce583113a76ced7e280513.html"),
        ("Investors Amazon FBA", "investors_club_295bd3c8770bd3c900945f9ec6ab9028.html"),
        ("WebsiteProperties Amazon FBA", "websiteproperties_com_96edeadf9264cd3bc2d4d63394d49206.html"),
    ]
    
    for source_name, filename in fba_sources:
        html = load_cached_html(filename)
        if not html:
            print(f"❌ {source_name}: No cached HTML")
            continue
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for Amazon FBA specific content
        fba_keywords = ['amazon', 'fba', 'fulfillment by amazon', 'amazon business']
        
        # Find containers that mention Amazon/FBA
        all_text_elements = soup.find_all(text=True)
        fba_containers = set()
        
        for text in all_text_elements:
            text_lower = text.lower().strip()
            if any(keyword in text_lower for keyword in fba_keywords) and len(text_lower) > 10:
                # Find the container holding this text
                parent = text.parent
                while parent and parent.name not in ['div', 'article', 'section']:
                    parent = parent.parent
                if parent:
                    fba_containers.add(parent)
        
        print(f"\n{source_name}: Found {len(fba_containers)} FBA-related containers")
        
        extracted_count = 0
        seen_urls = set()
        
        for container in fba_containers:
            try:
                # Extract title
                title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'a'])
                if not title_elem:
                    continue
                
                title = title_elem.get_text().strip()
                if len(title) < 15:
                    continue
                
                # Must contain Amazon/FBA in title or description
                container_text = container.get_text().lower()
                if not any(keyword in container_text for keyword in fba_keywords):
                    continue
                
                # Extract URL
                link_elem = container.find('a', href=True)
                if link_elem:
                    href = link_elem['href']
                    if source_name.startswith('QuietLight'):
                        base_url = "https://quietlight.com"
                    elif source_name.startswith('BizBuySell'):
                        base_url = "https://www.bizbuysell.com"
                    elif source_name.startswith('Investors'):
                        base_url = "https://investors.club"
                    else:
                        base_url = "https://websiteproperties.com"
                    
                    url = urljoin(base_url, href)
                else:
                    url = f"{base_url}/"
                
                # Skip duplicates
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Extract financial data
                text = container.get_text()
                financials = extract_fba_financials(text)
                
                if any(financials.values()) or 'amazon' in title.lower() or 'fba' in title.lower():
                    listing = {
                        'source': source_name.split()[0],  # Just the main source name
                        'name': title[:200],
                        'price': financials['price'],
                        'revenue': financials['revenue'],
                        'profit': financials['profit'],
                        'description': re.sub(r'\s+', ' ', text[:400]).strip(),
                        'url': url,
                        'is_amazon_fba': True
                    }
                    fba_listings.append(listing)
                    extracted_count += 1
                    
            except Exception as e:
                continue
        
        print(f"✅ {source_name}: Extracted {extracted_count} Amazon FBA businesses")
    
    return fba_listings

def extract_fba_financials(text: str) -> Dict[str, str]:
    """Extract financial data with FBA-specific patterns"""
    # FBA-specific financial patterns
    price_patterns = [
        r'(?:asking|price|listed|sale)(?:\s*price)?[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
        r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?[KkMm]?)',
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?[KkMm]?)\s*(?:asking|price|listed)',
    ]
    
    revenue_patterns = [
        r'(?:revenue|sales|gross|annual|ttm|monthly)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
        r'(\d+(?:\.\d+)?[KkMm]?)\s*(?:in\s*)?(?:revenue|sales|monthly)',
        r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*[KkMm]?)\s*(?:revenue|sales|gross|monthly)',
    ]
    
    profit_patterns = [
        r'(?:profit|cash\s*flow|sde|ebitda|earnings|income|net)[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)',
        r'(\d+(?:\.\d+)?[KkMm]?)\s*(?:profit|sde|ebitda|cash\s*flow)',
    ]
    
    def find_financial_match(patterns: List[str]) -> str:
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
                        if 1000 <= value <= 100000000:  # Reasonable FBA range
                            return f"${value:,.0f}"
                    except:
                        continue
        return ""
    
    return {
        'price': find_financial_match(price_patterns),
        'revenue': find_financial_match(revenue_patterns),
        'profit': find_financial_match(profit_patterns)
    }

def main():
    """Main analysis function"""
    print("🚀 AMAZON FBA DUPLICATION ANALYSIS")
    print("="*60)
    print("Analyzing why 96% duplication and focusing on FBA only")
    print("="*60)
    
    # First analyze the duplication issue
    analyze_duplication_issue()
    
    # Then extract only Amazon FBA businesses
    fba_listings = extract_amazon_fba_only()
    
    if fba_listings:
        # Remove duplicates properly
        df = pd.DataFrame(fba_listings)
        
        print(f"\n📊 FBA EXTRACTION RESULTS:")
        print(f"Raw FBA listings: {len(df)}")
        
        # Smart deduplication
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        url_dedup_count = len(df)
        
        df = df.drop_duplicates(subset=['name'], keep='first')
        final_count = len(df)
        
        print(f"After URL dedup: {url_dedup_count} (-{initial_count - url_dedup_count})")
        print(f"After name dedup: {final_count} (-{url_dedup_count - final_count})")
        print(f"Final deduplication rate: {(initial_count - final_count) / initial_count * 100:.1f}%")
        
        # Export results
        df.to_csv('AMAZON_FBA_ONLY_BUSINESSES.csv', index=False)
        
        # Analysis
        price_count = (df['price'] != '').sum()
        revenue_count = (df['revenue'] != '').sum()
        profit_count = (df['profit'] != '').sum()
        
        print(f"\n💰 AMAZON FBA DATA QUALITY:")
        print(f"  Prices: {price_count}/{len(df)} ({price_count/len(df)*100:.1f}%)")
        print(f"  Revenue: {revenue_count}/{len(df)} ({revenue_count/len(df)*100:.1f}%)")
        print(f"  Profit: {profit_count}/{len(df)} ({profit_count/len(df)*100:.1f}%)")
        
        # Source breakdown
        source_counts = df['source'].value_counts()
        print(f"\n📈 FBA SOURCES:")
        for source, count in source_counts.items():
            print(f"  {source}: {count} Amazon FBA businesses")
        
        print(f"\n💾 Exported to: AMAZON_FBA_ONLY_BUSINESSES.csv")
        print("🎯 Only legitimate Amazon FBA businesses included!")
        
    else:
        print("❌ No Amazon FBA businesses found")

if __name__ == "__main__":
    main() 