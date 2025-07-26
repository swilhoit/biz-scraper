#!/usr/bin/env python3
"""
Analyze the scraped data for mismatches and incorrect extractions
"""

import pandas as pd
import numpy as np
import re

def analyze_mismatches():
    # Load the enhanced CSV
    df = pd.read_csv('enhanced_business_details_v2.csv')
    
    print("=== DATA MISMATCH ANALYSIS ===\n")
    
    # 1. Check for values in wrong fields
    print("1. POTENTIAL FIELD MISMATCHES:")
    
    # Check if industry contains non-industry values
    print("\n[Industry Field Issues]")
    suspicious_industries = []
    for idx, industry in df['industry'].dropna().items():
        industry_str = str(industry)
        # Check for phrases that don't look like industries
        if any(phrase in industry_str.lower() for phrase in [
            'price', 'revenue', 'profit', 'sale', 'email', 'mailbox', 
            'reference', 'that is', 'industries price', 'listing types'
        ]):
            suspicious_industries.append({
                'row': idx,
                'source': df.loc[idx, 'source'],
                'name': df.loc[idx, 'name'][:50],
                'industry': industry_str[:100]
            })
    
    if suspicious_industries:
        for item in suspicious_industries[:5]:
            print(f"  Row {item['row']} ({item['source']}): '{item['industry']}'")
    
    # Check if location contains non-location values
    print("\n[Location Field Issues]")
    suspicious_locations = []
    for idx, location in df['location'].dropna().items():
        location_str = str(location)
        # Check for phrases that don't look like locations
        if any(phrase in location_str.lower() for phrase in [
            'entrepreneur', 'freedom', 'business', 'opportunity', 
            'reference', 'industries', 'filter', 'clear save'
        ]):
            suspicious_locations.append({
                'row': idx,
                'source': df.loc[idx, 'source'],
                'name': df.loc[idx, 'name'][:50],
                'location': location_str[:100]
            })
    
    if suspicious_locations:
        for item in suspicious_locations[:5]:
            print(f"  Row {item['row']} ({item['source']}): '{item['location']}'")
    
    # 2. Check for price/revenue format inconsistencies
    print("\n\n2. FINANCIAL DATA FORMAT ISSUES:")
    
    # Check asking_price field
    print("\n[Asking Price Format Issues]")
    price_issues = []
    for idx, price in df['asking_price'].dropna().items():
        price_str = str(price)
        # Check if price doesn't contain expected patterns
        if not re.search(r'\$|asking|price|\d', price_str, re.IGNORECASE):
            price_issues.append({
                'row': idx,
                'source': df.loc[idx, 'source'],
                'price': price_str[:100]
            })
    
    if price_issues:
        for item in price_issues[:5]:
            print(f"  Row {item['row']} ({item['source']}): '{item['price']}'")
    
    # 3. Check for multiple values that seem swapped
    print("\n\n3. POTENTIALLY SWAPPED VALUES:")
    
    # Check if inventory_value contains non-inventory data
    print("\n[Inventory Value Issues]")
    for idx, inv in df['inventory_value'].dropna().items():
        inv_str = str(inv)
        if not any(char in inv_str for char in ['$', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']):
            print(f"  Row {idx} ({df.loc[idx, 'source']}): inventory_value = '{inv_str[:50]}'")
    
    # 4. Check numeric extraction issues
    print("\n\n4. NUMERIC EXTRACTION ISSUES:")
    
    # Compare original vs numeric values
    print("\n[Price Extraction Failures]")
    price_failures = df[
        df['asking_price'].notna() & 
        df['asking_price_numeric'].isna() &
        df['asking_price'].astype(str).str.contains(r'\d', regex=True)
    ]
    
    if len(price_failures) > 0:
        for idx, row in price_failures.head(5).iterrows():
            print(f"  Row {idx}: Original='{row['asking_price'][:50]}' → Numeric=None")
    
    # 5. Check for data in wrong sections
    print("\n\n5. DATA IN WRONG SECTIONS:")
    
    # Check if growth_rate contains non-growth data
    print("\n[Growth Rate Field Issues]")
    for idx, growth in df['growth_rate'].dropna().items():
        growth_str = str(growth)
        if not any(char in growth_str for char in ['%', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']):
            print(f"  Row {idx} ({df.loc[idx, 'source']}): growth_rate = '{growth_str[:100]}'")
    
    # 6. Check for multiple extraction issues
    print("\n\n6. MULTIPLE FIELD EXTRACTION ISSUES:")
    
    # Check if 'multiple' field contains actual multiples
    print("\n[Multiple Field Issues]")
    multiple_issues = []
    for idx, mult in df['multiple'].dropna().items():
        mult_str = str(mult)
        # Should contain 'x' or numeric value
        if not re.search(r'\d|x|multiple', mult_str, re.IGNORECASE):
            multiple_issues.append({
                'row': idx,
                'source': df.loc[idx, 'source'],
                'multiple': mult_str[:100]
            })
    
    if multiple_issues:
        for item in multiple_issues[:5]:
            print(f"  Row {item['row']} ({item['source']}): '{item['multiple']}'")
    
    # 7. Check for HTML/JavaScript artifacts
    print("\n\n7. HTML/JAVASCRIPT ARTIFACTS:")
    
    html_patterns = [r'<[^>]+>', r'function\s*\(', r'\{[^}]+\}', r'var\s+\w+', r'document\.']
    
    text_fields = ['name', 'industry', 'location', 'growth_opportunities']
    for field in text_fields:
        if field in df.columns:
            for idx, value in df[field].dropna().items():
                value_str = str(value)
                for pattern in html_patterns:
                    if re.search(pattern, value_str):
                        print(f"  {field} (Row {idx}): Contains HTML/JS artifacts")
                        break
    
    # 8. Summary of issues by source
    print("\n\n8. ISSUES BY SOURCE:")
    
    # Count issues per source
    source_issues = {}
    for source in df['source'].unique():
        source_df = df[df['source'] == source]
        issues = 0
        
        # Count various issues
        issues += len(source_df[source_df['industry'].astype(str).str.contains('reference|mailbox|that is', case=False, na=False)])
        issues += len(source_df[source_df['location'].astype(str).str.contains('entrepreneur|freedom|business', case=False, na=False)])
        issues += len(source_df[source_df['asking_price'].notna() & source_df['asking_price_numeric'].isna()])
        
        source_issues[source] = {
            'total_records': len(source_df),
            'issue_count': issues,
            'issue_rate': (issues / len(source_df) * 100) if len(source_df) > 0 else 0
        }
    
    print("\nIssue rates by source:")
    for source, stats in sorted(source_issues.items(), key=lambda x: x[1]['issue_rate'], reverse=True):
        print(f"  {source}: {stats['issue_count']} issues in {stats['total_records']} records ({stats['issue_rate']:.1f}%)")
    
    # 9. Specific selector issues
    print("\n\n9. SPECIFIC SELECTOR RECOMMENDATIONS:")
    
    # Analyze patterns in mismatched data
    print("\n[Recommended Selector Fixes]")
    
    # For Websiteproperties
    wp_issues = df[df['source'] == 'Websiteproperties']
    if 'Terms Reference' in wp_issues['industry'].values:
        print("  - Websiteproperties: Industry selector is catching navigation/footer text")
        print("    Current: May be selecting '.terms' or similar")
        print("    Fix: Use more specific selector like '.business-details .industry'")
    
    # For Bizquest
    bq_issues = df[df['source'] == 'Bizquest']
    if any(bq_issues['location'].astype(str).str.contains('Industries Price', na=False)):
        print("\n  - Bizquest: Location selector is catching filter menu text")
        print("    Current: May be selecting too broad a container")
        print("    Fix: Use more specific selector like '.listing-location' or '.business-location'")
    
    # For QuietLight
    ql_issues = df[df['source'] == 'QuietLight']
    if any(ql_issues['industry'].astype(str).str.contains('Insights in your Mailbox', na=False)):
        print("\n  - QuietLight: Industry selector is catching newsletter signup text")
        print("    Current: May be selecting sidebar or footer content")
        print("    Fix: Use more specific selector within main content area")
    
    # 10. Data quality recommendations
    print("\n\n10. DATA QUALITY RECOMMENDATIONS:")
    print("  1. Add validation to reject non-industry values in industry field")
    print("  2. Improve location parsing to filter out non-geographic text")
    print("  3. Add stricter patterns for financial data extraction")
    print("  4. Implement field-specific cleaning functions")
    print("  5. Add confidence scores for extracted values")
    print("  6. Consider using NLP to classify text into correct categories")

if __name__ == "__main__":
    analyze_mismatches()