#!/usr/bin/env python3
"""
Fixed extraction methods for the enhanced detail scraper
"""

import re
from bs4 import BeautifulSoup

# Example fixes for the identified issues

def fixed_extract_financial_metrics(soup: BeautifulSoup) -> dict:
    """Fixed version of financial metric extraction"""
    metrics = {}
    page_text = soup.get_text()
    
    # FIXED: Extract asking price value only (not the full match)
    asking_price_patterns = [
        r'asking\s*price[:\s]*(\$?[\d,]+(?:\.\d+)?[KkMm]?)',  # Capture group for value
        r'price[:\s]*(\$?[\d,]+(?:\.\d+)?[KkMm]?)',
        r'for\s*sale[:\s]*(\$?[\d,]+(?:\.\d+)?[KkMm]?)'
    ]
    
    for pattern in asking_price_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            metrics['asking_price'] = match.group(1)  # Get capture group, not full match
            break
    
    # FIXED: Handle inventory with both numeric and text values
    inventory_match = re.search(r'inventory[:\s]*(.+?)(?=\n|$|\.)', page_text, re.IGNORECASE)
    if inventory_match:
        inv_value = inventory_match.group(1).strip()
        # Check if it's actually a value or just "not included"
        if 'not included' not in inv_value.lower():
            metrics['inventory_value'] = inv_value
        else:
            metrics['inventory_included'] = 'No'
    
    return metrics


def fixed_extract_business_details(soup: BeautifulSoup) -> dict:
    """Fixed version of business details extraction"""
    details = {}
    
    # FIXED: More specific industry extraction
    # Look for industry in specific sections first
    industry_sections = soup.find_all(['div', 'section'], class_=re.compile('business-details|listing-details|overview'))
    
    for section in industry_sections:
        # Look for labeled industry data
        industry_label = section.find(text=re.compile(r'Industry|Sector|Category', re.IGNORECASE))
        if industry_label:
            # Get the next sibling or parent's text
            parent = industry_label.parent
            if parent:
                # Try to find the value in various ways
                value_elem = parent.find_next_sibling()
                if value_elem:
                    industry_text = value_elem.get_text().strip()
                else:
                    # Get text after the label
                    full_text = parent.get_text()
                    match = re.search(r'(?:Industry|Sector|Category)[:\s]*([^,\n]{3,50})', full_text, re.IGNORECASE)
                    if match:
                        industry_text = match.group(1).strip()
                
                # Validate it's actually an industry
                if industry_text and not any(skip in industry_text.lower() for skip in [
                    'reference', 'terms', 'mailbox', 'newsletter', 'sign up', 'subscribe'
                ]):
                    details['industry'] = industry_text
                    break
    
    # FIXED: More specific location extraction
    location_sections = soup.find_all(['div', 'span'], class_=re.compile('location|address|geography'))
    
    for section in location_sections:
        location_text = section.get_text().strip()
        # Validate it's actually a location
        if location_text and not any(skip in location_text.lower() for skip in [
            'filter', 'search', 'industries', 'price', 'listing types', 'clear', 'save',
            'entrepreneur', 'freedom', 'opportunity'
        ]):
            details['location'] = location_text
            break
    
    # If not found in specific sections, try patterns but with validation
    if 'location' not in details:
        page_text = soup.get_text()
        location_match = re.search(r'(?:Location|Based in|Located in)[:\s]*([A-Za-z\s,]{3,50})', page_text, re.IGNORECASE)
        if location_match:
            loc = location_match.group(1).strip()
            if not any(skip in loc.lower() for skip in ['select', 'filter', 'search']):
                details['location'] = loc
    
    return details


def fixed_scrape_bizquest_detail(soup: BeautifulSoup) -> dict:
    """New method specifically for BizQuest listings"""
    data = {}
    
    # BizQuest has a specific structure - look for their detail table
    details_table = soup.find('table', class_='business-details-table')
    if not details_table:
        # Try alternative selectors
        details_table = soup.find('div', class_='listing-details')
    
    if details_table:
        # Extract from table rows
        rows = details_table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                label = cells[0].get_text().strip().lower()
                value = cells[1].get_text().strip()
                
                if 'location' in label:
                    # Clean location value
                    if not any(skip in value.lower() for skip in ['industries', 'filter', 'search']):
                        data['location'] = value
                elif 'industry' in label:
                    data['industry'] = value
                elif 'asking price' in label:
                    data['asking_price'] = value
                elif 'gross revenue' in label:
                    data['annual_revenue'] = value
                elif 'cash flow' in label:
                    data['annual_profit'] = value
    
    # Look for location in breadcrumbs (often more reliable)
    breadcrumbs = soup.find('div', class_='breadcrumbs')
    if breadcrumbs and 'location' not in data:
        # BizQuest often shows location in breadcrumbs
        location_match = re.search(r'in\s+([A-Za-z\s]+(?:,\s*[A-Z]{2})?)', breadcrumbs.get_text())
        if location_match:
            data['location'] = location_match.group(1).strip()
    
    return data


def improved_extraction_logic():
    """
    Summary of improvements needed:
    
    1. Use capturing groups in regex patterns to extract values only
    2. Add validation to reject non-relevant matches
    3. Look for data in specific HTML sections first (classes/ids)
    4. Add marketplace-specific extraction methods
    5. Validate extracted values before storing
    """
    
    improvements = {
        'financial_patterns': {
            'asking_price': r'asking\s*price[:\s]*(\$?[\d,]+(?:\.\d+)?[KkMm]?)',
            'revenue': r'(?:annual\s*)?revenue[:\s]*(\$?[\d,]+(?:\.\d+)?[KkMm]?)',
            'profit': r'(?:annual\s*)?profit[:\s]*(\$?[\d,]+(?:\.\d+)?[KkMm]?)'
        },
        'validation_rules': {
            'industry': ['min_length: 3', 'max_length: 50', 'no_keywords: [reference, terms, mailbox, filter]'],
            'location': ['min_length: 3', 'max_length: 100', 'no_keywords: [filter, search, industries, entrepreneur]'],
            'price': ['must_contain: [digit or $]', 'min_length: 2']
        },
        'selector_priority': [
            '1. Try specific class/id selectors first',
            '2. Look in labeled sections (dt/dd, table rows)',
            '3. Use regex patterns as fallback',
            '4. Always validate extracted values'
        ]
    }
    
    return improvements


# Example of how to fix the main extraction method
def fixed_extract_from_sections(soup: BeautifulSoup, details: dict) -> None:
    """Fixed version that looks for data in specific sections"""
    
    # 1. Try to find a business details/overview section
    overview_selectors = [
        'div.business-overview',
        'section.listing-details',
        'div.business-information',
        'div[class*="details"]',
        'section[class*="overview"]'
    ]
    
    for selector in overview_selectors:
        section = soup.select_one(selector)
        if section:
            # Extract industry from this section only
            industry_elem = section.find(text=re.compile(r'Industry|Sector', re.IGNORECASE))
            if industry_elem:
                # Get the associated value
                parent = industry_elem.parent
                if parent.name in ['dt', 'th', 'strong', 'b']:
                    # Look for dd, td, or next sibling
                    value_elem = parent.find_next_sibling()
                    if value_elem:
                        industry = value_elem.get_text().strip()
                        if 3 <= len(industry) <= 50:  # Validate length
                            details['industry'] = industry
            
            # Extract location similarly
            location_elem = section.find(text=re.compile(r'Location|Based in', re.IGNORECASE))
            if location_elem:
                parent = location_elem.parent
                if parent.name in ['dt', 'th', 'strong', 'b']:
                    value_elem = parent.find_next_sibling()
                    if value_elem:
                        location = value_elem.get_text().strip()
                        # Validate it's a real location
                        if re.search(r'[A-Za-z]{3,}', location) and len(location) < 100:
                            details['location'] = location
            
            # If we found data in this section, stop looking
            if 'industry' in details or 'location' in details:
                break


if __name__ == "__main__":
    print("Extraction selector fixes:")
    print("\n1. Use capturing groups to extract values only")
    print("2. Add validation to reject irrelevant matches")
    print("3. Look for data in specific HTML sections first")
    print("4. Add Bizquest-specific extraction method")
    print("5. Validate all extracted values before storing")
    
    improvements = improved_extraction_logic()
    print("\n\nRecommended improvements:")
    for category, items in improvements.items():
        print(f"\n{category}:")
        if isinstance(items, dict):
            for key, value in items.items():
                print(f"  {key}: {value}")
        else:
            for item in items:
                print(f"  - {item}")