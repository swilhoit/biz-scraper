#!/usr/bin/env python3
"""
Test all scrapers and verify data extraction for revenue, profit, and other fields
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime
import time

def parse_price(price_str):
    """Parse price string to float"""
    if not price_str:
        return 0.0
    
    price_str = str(price_str).replace('$', '').replace(',', '').strip()
    
    multipliers = {
        'k': 1000,
        'thousand': 1000,
        'm': 1000000,
        'mil': 1000000,
        'million': 1000000,
        'mm': 1000000,
        'b': 1000000000,
        'billion': 1000000000
    }
    
    for suffix, multiplier in multipliers.items():
        if price_str.lower().endswith(suffix):
            num_part = price_str[:-len(suffix)].strip()
            try:
                return float(num_part) * multiplier
            except:
                pass
    
    try:
        return float(price_str)
    except:
        return 0.0

def scrape_bizquest():
    """Scrape BizQuest listings with comprehensive data extraction"""
    print("\n" + "="*60)
    print("SCRAPING BIZQUEST")
    print("="*60)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    url = "https://www.bizquest.com/businesses-for-sale/"
    response = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all listing containers 
    listing_divs = soup.select('div.listing')[:5]  # Test first 5
    
    results = []
    for i, listing_div in enumerate(listing_divs, 1):
        print(f"\n{i}. Processing listing...")
        
        # Get listing URL
        link = listing_div.select_one('a[href*="/business-for-sale/"]')
        if not link:
            continue
            
        href = link.get('href')
        if href.startswith('/'):
            listing_url = "https://www.bizquest.com" + href
        else:
            listing_url = href
        
        # Extract data from search result
        listing_text = listing_div.get_text(separator=' ', strip=True)
        
        data = {
            'source': 'BizQuest',
            'listing_url': listing_url,
            'scraped_at': datetime.now().isoformat()
        }
        
        # Title - from link text or URL
        title_text = link.get_text(strip=True)
        if title_text and len(title_text) > 10:
            data['title'] = title_text
        else:
            match = re.search(r'/business-for-sale/([^/]+)/', listing_url)
            if match:
                data['title'] = match.group(1).replace('-', ' ').title()
        
        # Price extraction
        price_match = re.search(r'\$?([\d,]+(?:\.\d+)?[KkMm]?(?:illion)?)', listing_text)
        if price_match:
            data['asking_price'] = parse_price(price_match.group(1))
            data['asking_price_raw'] = price_match.group(0)
        
        # Cash Flow extraction (BizQuest shows this prominently)
        cash_flow_match = re.search(r'cash flow[:\s]*\$?([\d,]+(?:\.\d+)?[KkMm]?)', listing_text, re.I)
        if cash_flow_match:
            cf_value = parse_price(cash_flow_match.group(1))
            data['cash_flow'] = cf_value
            data['cash_flow_raw'] = cash_flow_match.group(0)
            # Use as profit if no separate profit found
            data['profit'] = cf_value
            data['profit_raw'] = cash_flow_match.group(0)
        
        # Location extraction
        location_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})', listing_text)
        if location_match:
            data['location'] = location_match.group(1)
            parts = data['location'].split(',')
            if len(parts) >= 2:
                data['city'] = parts[0].strip()
                data['state'] = parts[-1].strip()
        
        # Category
        data['category'] = 'Business'
        data['business_type'] = 'General'
        
        results.append(data)
        print(f"   ✓ Extracted: Price=${data.get('asking_price', 0):,.0f}, CF=${data.get('cash_flow', 0):,.0f}, Location={data.get('location', 'N/A')}")
    
    return results

def scrape_empireflippers():
    """Scrape EmpireFlippers listings"""
    print("\n" + "="*60)
    print("SCRAPING EMPIRE FLIPPERS")
    print("="*60)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    url = "https://empireflippers.com/marketplace/"
    response = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find listing cards
    listings = soup.select('div[class*="listing"]')[:3]  # Test first 3
    
    results = []
    for i, listing in enumerate(listings, 1):
        print(f"\n{i}. Processing listing...")
        
        data = {
            'source': 'EmpireFlippers',
            'scraped_at': datetime.now().isoformat()
        }
        
        # Extract from card
        card_text = listing.get_text(separator=' ', strip=True)
        
        # Price
        price_match = re.search(r'\$?([\d,]+)', card_text)
        if price_match:
            data['asking_price'] = parse_price(price_match.group(1))
            data['asking_price_raw'] = price_match.group(0)
        
        # Monthly profit (EF shows monthly)
        profit_match = re.search(r'(?:monthly profit|net profit)[:\s]*\$?([\d,]+)', card_text, re.I)
        if profit_match:
            monthly = parse_price(profit_match.group(1))
            data['profit'] = monthly * 12  # Annualize
            data['profit_raw'] = f"${monthly:,.0f}/month"
        
        # Monthly revenue
        revenue_match = re.search(r'(?:monthly revenue|gross revenue)[:\s]*\$?([\d,]+)', card_text, re.I)
        if revenue_match:
            monthly = parse_price(revenue_match.group(1))
            data['revenue'] = monthly * 12  # Annualize
            data['revenue_raw'] = f"${monthly:,.0f}/month"
        
        # Business type
        if 'amazon' in card_text.lower() or 'fba' in card_text.lower():
            data['business_type'] = 'E-commerce'
            data['category'] = 'E-commerce'
        elif 'saas' in card_text.lower() or 'software' in card_text.lower():
            data['business_type'] = 'Technology'
            data['category'] = 'Technology'
        else:
            data['business_type'] = 'Online Business'
            data['category'] = 'Online'
        
        # Title
        title_elem = listing.select_one('h2, h3, a')
        if title_elem:
            data['title'] = title_elem.get_text(strip=True)
        else:
            data['title'] = 'Empire Flippers Listing'
        
        # URL
        link = listing.select_one('a[href*="/listing/"]')
        if link:
            href = link.get('href')
            if href.startswith('/'):
                data['listing_url'] = "https://empireflippers.com" + href
            else:
                data['listing_url'] = href
        
        results.append(data)
        print(f"   ✓ Extracted: Price=${data.get('asking_price', 0):,.0f}, Revenue=${data.get('revenue', 0):,.0f}, Profit=${data.get('profit', 0):,.0f}")
    
    return results

def scrape_websiteproperties():
    """Scrape WebsiteProperties listings"""
    print("\n" + "="*60)
    print("SCRAPING WEBSITE PROPERTIES")
    print("="*60)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    url = "https://websiteproperties.com/listings/"
    response = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find listings
    listings = soup.select('article.listing, div.listing-item')[:3]
    
    results = []
    for i, listing in enumerate(listings, 1):
        print(f"\n{i}. Processing listing...")
        
        data = {
            'source': 'WebsiteProperties',
            'scraped_at': datetime.now().isoformat()
        }
        
        listing_text = listing.get_text(separator=' ', strip=True)
        
        # Title
        title_elem = listing.select_one('h2, h3')
        if title_elem:
            data['title'] = title_elem.get_text(strip=True)
        
        # Price
        price_match = re.search(r'\$?([\d,]+(?:\.\d+)?[KkMm]?)', listing_text)
        if price_match:
            data['asking_price'] = parse_price(price_match.group(1))
            data['asking_price_raw'] = price_match.group(0)
        
        # Monthly profit
        profit_match = re.search(r'(?:monthly profit|cash flow)[:\s]*\$?([\d,]+)', listing_text, re.I)
        if profit_match:
            monthly = parse_price(profit_match.group(1))
            data['profit'] = monthly * 12
            data['profit_raw'] = f"${monthly:,.0f}/month"
            data['cash_flow'] = data['profit']
        
        # Revenue
        revenue_match = re.search(r'(?:revenue|sales)[:\s]*\$?([\d,]+)', listing_text, re.I)
        if revenue_match:
            data['revenue'] = parse_price(revenue_match.group(1))
            data['revenue_raw'] = revenue_match.group(0)
        
        # Business type
        data['business_type'] = 'Online Business'
        data['category'] = 'Online'
        
        # URL
        link = listing.select_one('a[href*="/listing"]')
        if link:
            href = link.get('href')
            if href.startswith('/'):
                data['listing_url'] = "https://websiteproperties.com" + href
            else:
                data['listing_url'] = href
        
        results.append(data)
        print(f"   ✓ Extracted: Price=${data.get('asking_price', 0):,.0f}, Profit=${data.get('profit', 0):,.0f}")
    
    return results

def scrape_quietlight():
    """Scrape QuietLight listings"""
    print("\n" + "="*60)
    print("SCRAPING QUIET LIGHT")
    print("="*60)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    url = "https://quietlight.com/listings/"
    response = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find listings
    listings = soup.select('div.listing-card, article.listing')[:3]
    
    results = []
    for i, listing in enumerate(listings, 1):
        print(f"\n{i}. Processing listing...")
        
        data = {
            'source': 'QuietLight',
            'scraped_at': datetime.now().isoformat()
        }
        
        listing_text = listing.get_text(separator=' ', strip=True)
        
        # Title
        title_elem = listing.select_one('h2, h3, .listing-title')
        if title_elem:
            data['title'] = title_elem.get_text(strip=True)
        
        # Price
        price_match = re.search(r'\$?([\d,]+(?:\.\d+)?[KkMm]?)', listing_text)
        if price_match:
            data['asking_price'] = parse_price(price_match.group(1))
            data['asking_price_raw'] = price_match.group(0)
        
        # TTM (Trailing Twelve Months) earnings/profit
        ttm_match = re.search(r'(?:ttm|earnings|sde)[:\s]*\$?([\d,]+)', listing_text, re.I)
        if ttm_match:
            data['profit'] = parse_price(ttm_match.group(1))
            data['profit_raw'] = ttm_match.group(0)
            data['cash_flow'] = data['profit']
        
        # Revenue
        revenue_match = re.search(r'(?:revenue|sales)[:\s]*\$?([\d,]+)', listing_text, re.I)
        if revenue_match:
            data['revenue'] = parse_price(revenue_match.group(1))
            data['revenue_raw'] = revenue_match.group(0)
        
        # Business type
        if 'saas' in listing_text.lower():
            data['business_type'] = 'SaaS'
            data['category'] = 'Technology'
        elif 'ecommerce' in listing_text.lower() or 'e-commerce' in listing_text.lower():
            data['business_type'] = 'E-commerce'
            data['category'] = 'E-commerce'
        else:
            data['business_type'] = 'Online Business'
            data['category'] = 'Online'
        
        # URL
        link = listing.select_one('a[href*="/listing"]')
        if link:
            href = link.get('href')
            if href.startswith('/'):
                data['listing_url'] = "https://quietlight.com" + href
            else:
                data['listing_url'] = href
        
        results.append(data)
        print(f"   ✓ Extracted: Price=${data.get('asking_price', 0):,.0f}, Profit=${data.get('profit', 0):,.0f}")
    
    return results

def scrape_bizbuysell():
    """Scrape BizBuySell listings"""
    print("\n" + "="*60)
    print("SCRAPING BIZBUYSELL")
    print("="*60)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    url = "https://www.bizbuysell.com/businesses-for-sale/"
    response = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find listings
    listings = soup.select('div.listing, div[class*="listing-card"]')[:3]
    
    results = []
    for i, listing in enumerate(listings, 1):
        print(f"\n{i}. Processing listing...")
        
        data = {
            'source': 'BizBuySell',
            'scraped_at': datetime.now().isoformat()
        }
        
        listing_text = listing.get_text(separator=' ', strip=True)
        
        # Title
        title_elem = listing.select_one('h2, h3, .title')
        if title_elem:
            data['title'] = title_elem.get_text(strip=True)
        
        # Price
        price_match = re.search(r'asking price[:\s]*\$?([\d,]+)', listing_text, re.I)
        if not price_match:
            price_match = re.search(r'\$?([\d,]+)', listing_text)
        if price_match:
            data['asking_price'] = parse_price(price_match.group(1))
            data['asking_price_raw'] = price_match.group(0)
        
        # Cash Flow
        cf_match = re.search(r'cash flow[:\s]*\$?([\d,]+)', listing_text, re.I)
        if cf_match:
            data['cash_flow'] = parse_price(cf_match.group(1))
            data['cash_flow_raw'] = cf_match.group(0)
            data['profit'] = data['cash_flow']
        
        # Gross Revenue
        revenue_match = re.search(r'(?:gross revenue|revenue)[:\s]*\$?([\d,]+)', listing_text, re.I)
        if revenue_match:
            data['revenue'] = parse_price(revenue_match.group(1))
            data['revenue_raw'] = revenue_match.group(0)
        
        # Location
        location_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})', listing_text)
        if location_match:
            data['location'] = location_match.group(1)
            parts = data['location'].split(',')
            if len(parts) >= 2:
                data['city'] = parts[0].strip()
                data['state'] = parts[-1].strip()
        
        # Business type from title or text
        text_lower = listing_text.lower()
        if 'restaurant' in text_lower:
            data['business_type'] = 'Restaurant'
            data['category'] = 'Restaurant'
        elif 'retail' in text_lower:
            data['business_type'] = 'Retail'
            data['category'] = 'Retail'
        elif 'service' in text_lower:
            data['business_type'] = 'Service'
            data['category'] = 'Service'
        else:
            data['business_type'] = 'Business'
            data['category'] = 'General'
        
        # URL
        link = listing.select_one('a[href*="/Business"]')
        if link:
            href = link.get('href')
            if href.startswith('/'):
                data['listing_url'] = "https://www.bizbuysell.com" + href
            else:
                data['listing_url'] = href
        
        results.append(data)
        print(f"   ✓ Extracted: Price=${data.get('asking_price', 0):,.0f}, Revenue=${data.get('revenue', 0):,.0f}, CF=${data.get('cash_flow', 0):,.0f}")
    
    return results

def main():
    """Run all scrapers and save results"""
    print("="*60)
    print("BUSINESS LISTING SCRAPER - COMPREHENSIVE TEST")
    print("="*60)
    print(f"Started at: {datetime.now()}")
    
    all_results = []
    
    # Run each scraper
    scrapers = [
        ('BizQuest', scrape_bizquest),
        ('EmpireFlippers', scrape_empireflippers),
        ('WebsiteProperties', scrape_websiteproperties),
        ('QuietLight', scrape_quietlight),
        ('BizBuySell', scrape_bizbuysell)
    ]
    
    for name, scraper_func in scrapers:
        try:
            results = scraper_func()
            all_results.extend(results)
            print(f"\n✅ {name}: Scraped {len(results)} listings")
        except Exception as e:
            print(f"\n❌ {name}: Error - {e}")
    
    # Summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    
    if all_results:
        total = len(all_results)
        has_price = sum(1 for r in all_results if r.get('asking_price', 0) > 0)
        has_revenue = sum(1 for r in all_results if r.get('revenue', 0) > 0)
        has_profit = sum(1 for r in all_results if r.get('profit', 0) > 0)
        has_location = sum(1 for r in all_results if r.get('location') or r.get('city'))
        
        print(f"Total listings scraped: {total}")
        print(f"With asking price: {has_price}/{total} ({100*has_price/total:.0f}%)")
        print(f"With revenue data: {has_revenue}/{total} ({100*has_revenue/total:.0f}%)")
        print(f"With profit data: {has_profit}/{total} ({100*has_profit/total:.0f}%)")
        print(f"With location data: {has_location}/{total} ({100*has_location/total:.0f}%)")
        
        # Save results
        filename = f'scraper_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(filename, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {filename}")
        
        # Group by source
        print("\nBY SOURCE:")
        for source in set(r['source'] for r in all_results):
            source_results = [r for r in all_results if r['source'] == source]
            source_has_revenue = sum(1 for r in source_results if r.get('revenue', 0) > 0)
            source_has_profit = sum(1 for r in source_results if r.get('profit', 0) > 0)
            print(f"  {source}: {len(source_results)} listings")
            print(f"    - With revenue: {source_has_revenue}/{len(source_results)}")
            print(f"    - With profit: {source_has_profit}/{len(source_results)}")
    else:
        print("No results collected!")
    
    print(f"\nCompleted at: {datetime.now()}")

if __name__ == "__main__":
    main()