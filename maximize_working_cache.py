#!/usr/bin/env python3
"""
Maximize Working Cache Sources
Extract every possible business from QuietLight, WebsiteProperties, Investors, BizBuySell
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

def extract_financials_aggressive(text: str) -> Dict[str, str]:
    """Aggressive financial extraction avoiding only known placeholders"""
    
    # More aggressive patterns
    price_patterns = [
        r'asking[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'price[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'listed[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:asking|price|listed)',
        r'(\d+(?:\.\d+)?[KkMm])',
    ]
    
    revenue_patterns = [
        r'revenue[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'sales[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'gross[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'monthly[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'annual[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'ttm[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:revenue|sales|monthly|annual)',
    ]
    
    profit_patterns = [
        r'profit[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'cash\s*flow[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'sde[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'ebitda[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'earnings[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'income[:\s]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)',
        r'(\d+(?:\.\d+)?[KkMm])\s*(?:profit|sde|ebitda|cash\s*flow|income)',
    ]
    
    # Only avoid these exact placeholder values
    forbidden_values = ['250000', '250,000', '500004', '500,004']
    
    def find_best_match(patterns: List[str]) -> str:
        """Find best financial match"""
        best_value = ""
        best_numeric = 0
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = next((m for m in match if m and m.strip()), match[-1])
                
                clean_match = re.sub(r'[^\d.,KkMm]', '', str(match))
                
                # Skip exact placeholder values
                if clean_match.replace(',', '') in forbidden_values:
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
                        
                        # Much broader range - almost any reasonable business value
                        if 100 <= value <= 500000000:  # $100 to $500M
                            if value > best_numeric:
                                best_numeric = value
                                best_value = f"${value:,.0f}"
                    except:
                        continue
        
        return best_value
    
    return {
        'price': find_best_match(price_patterns),
        'revenue': find_best_match(revenue_patterns),
        'profit': find_best_match(profit_patterns)
    }

def extract_quietlight_aggressive() -> List[Dict]:
    """Aggressively extract from QuietLight - get every possible business"""
    print("🔥 AGGRESSIVE QuietLight extraction...")
    
    listings = []
    
    quietlight_files = [
        "quietlight_com_54f6db706e3aada7af8f0a6485dfa61b.html",
        "quietlight_com_769ba6690f9f17e6cd331d74d0f04c90.html", 
        "quietlight_com_755d4f3bf7f83f7f482bba171ef5b079.html",
    ]
    
    for filename in quietlight_files:
        html = load_cached_html(filename)
        if not html:
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try multiple container strategies
        strategies = [
            soup.select('div[class*="listing"]'),
            soup.select('article'),
            soup.select('div[class*="business"]'),
            soup.select('div[class*="card"]'),
            soup.select('div[class*="item"]'),
            soup.select('div[data-testid]'),
            soup.select('a[href*="/listing"]'),
        ]
        
        all_containers = set()
        for strategy in strategies:
            for container in strategy:
                all_containers.add(container)
        
        print(f"QuietLight ({filename}): Found {len(all_containers)} total containers")
        
        extracted_count = 0
        seen_urls = set()
        
        for container in all_containers:
            try:
                # Find any link in this container
                link_elem = container.find('a', href=True) if hasattr(container, 'find') else container
                
                if not link_elem or not link_elem.get('href'):
                    continue
                
                href = link_elem.get('href', '')
                
                # Must be some kind of listing URL
                if not any(keyword in href for keyword in ['/listing', '/business', '/sale', '/broker', '/opportunity']):
                    continue
                
                # Skip obvious navigation
                if any(nav in href.lower() for nav in ['/about', '/contact', '/learn', '/sell', '/buy', 'javascript:', '#']):
                    continue
                
                url = urljoin("https://quietlight.com", href)
                
                # Skip duplicates
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Extract title - be very flexible
                title = ""
                if hasattr(link_elem, 'get_text'):
                    title = link_elem.get_text().strip()
                
                if not title or len(title) < 8:
                    # Try parent container
                    parent = link_elem.find_parent() if hasattr(link_elem, 'find_parent') else container
                    if parent:
                        title_elem = parent.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                        if title_elem:
                            title = title_elem.get_text().strip()
                
                if not title or len(title) < 8:
                    title = f"QuietLight Business #{extracted_count + 1}"
                
                # Get all text from container/parent
                if hasattr(container, 'get_text'):
                    text = container.get_text()
                else:
                    parent = link_elem.find_parent()
                    text = parent.get_text() if parent else link_elem.get_text()
                
                # Extract financials aggressively
                financials = extract_financials_aggressive(text)
                
                # Determine category
                category = 'General Business'
                text_lower = text.lower()
                if any(keyword in text_lower for keyword in ['amazon', 'fba']):
                    category = 'Amazon FBA'
                elif any(keyword in text_lower for keyword in ['ecommerce', 'e-commerce']):
                    category = 'Ecommerce'
                elif any(keyword in text_lower for keyword in ['saas', 'software', 'app']):
                    category = 'SaaS'
                elif any(keyword in text_lower for keyword in ['content', 'blog', 'affiliate']):
                    category = 'Content'
                
                listing = {
                    'source': 'QuietLight',
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': re.sub(r'\s+', ' ', text[:400]).strip(),
                    'url': url,
                    'category': category
                }
                listings.append(listing)
                extracted_count += 1
                
            except Exception as e:
                continue
        
        print(f"✅ QuietLight ({filename}): Extracted {extracted_count} businesses")
    
    return listings

def extract_other_sources_aggressive() -> List[Dict]:
    """Aggressively extract from other sources"""
    print("🔥 AGGRESSIVE extraction from other sources...")
    
    listings = []
    
    sources = [
        ("BizBuySell", "bizbuysell_com_b106cc7129ce583113a76ced7e280513.html"),
        ("Investors", "investors_club_295bd3c8770bd3c900945f9ec6ab9028.html"),
        ("WebsiteProperties", "websiteproperties_com_96edeadf9264cd3bc2d4d63394d49206.html"),
    ]
    
    for source_name, filename in sources:
        html = load_cached_html(filename)
        if not html:
            continue
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try multiple extraction strategies
        all_links = soup.find_all('a', href=True)
        containers = soup.select('div, article, section')
        
        print(f"{source_name}: Found {len(all_links)} links, {len(containers)} containers")
        
        extracted_count = 0
        seen_urls = set()
        
        # Strategy 1: Extract from links that look like businesses
        for link in all_links:
            try:
                href = link.get('href', '')
                
                # Skip obviously non-business links
                if any(skip in href.lower() for skip in [
                    'javascript:', '#', '/about', '/contact', '/search', '/category',
                    '/login', '/register', '/privacy', '/terms', '/blog', '/news'
                ]):
                    continue
                
                # Must have some business indicators
                if not any(indicator in href.lower() for indicator in [
                    'business', 'listing', 'opportunity', 'sale', 'broker', 'company', 'fba', 'amazon'
                ]):
                    # Check link text
                    link_text = link.get_text().lower()
                    if not any(indicator in link_text for indicator in [
                        'business', 'company', 'brand', 'store', 'website', 'fba', 'amazon', 'ecommerce'
                    ]):
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
                title = link.get_text().strip()
                if not title or len(title) < 8:
                    continue
                
                # Skip obviously generic titles
                if title.lower() in ['home', 'about', 'contact', 'search', 'browse', 'buy', 'sell']:
                    continue
                
                # Get surrounding context
                parent = link.find_parent(['div', 'article', 'section'])
                text = parent.get_text() if parent else link.get_text()
                
                # Extract financials
                financials = extract_financials_aggressive(text)
                
                # Determine category
                category = 'General Business'
                text_lower = text.lower()
                if any(keyword in text_lower for keyword in ['amazon', 'fba']):
                    category = 'Amazon FBA'
                elif any(keyword in text_lower for keyword in ['ecommerce', 'e-commerce']):
                    category = 'Ecommerce'
                
                listing = {
                    'source': source_name,
                    'name': title[:200],
                    'price': financials['price'],
                    'revenue': financials['revenue'],
                    'profit': financials['profit'],
                    'description': re.sub(r'\s+', ' ', text[:400]).strip(),
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
    """Maximize extraction from working cached sources"""
    print("🚀 MAXIMIZING WORKING CACHED SOURCES")
    print("="*60)
    print("BizQuest cache failed (bot protection), maximizing other sources")
    print("="*60)
    
    all_listings = []
    
    # Extract aggressively from working sources
    all_listings.extend(extract_quietlight_aggressive())
    all_listings.extend(extract_other_sources_aggressive())
    
    if all_listings:
        # Create DataFrame
        df = pd.DataFrame(all_listings)
        
        print(f"\n📊 MAXIMIZED RESULTS:")
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
        df.to_csv('MAXIMIZED_WORKING_CACHE.csv', index=False)
        
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
        
        # Amazon FBA specific count
        amazon_fba = df[df['category'] == 'Amazon FBA']
        print(f"\n🎯 AMAZON FBA FOCUS:")
        print(f"  Amazon FBA businesses: {len(amazon_fba):,}")
        print(f"  Amazon FBA with prices: {(amazon_fba['price'] != '').sum()}")
        print(f"  Amazon FBA with revenue: {(amazon_fba['revenue'] != '').sum()}")
        
        print(f"\n💾 Exported to: MAXIMIZED_WORKING_CACHE.csv")
        print(f"🔥 Maximized from working sources: {len(df):,} businesses")
        print("⚠️  BizQuest cache failed due to bot protection")
        
    else:
        print("❌ No businesses found")

if __name__ == "__main__":
    main() 