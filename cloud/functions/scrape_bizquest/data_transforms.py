"""
Data transformation functions for normalizing scraped business data
"""

import re
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Union, Any
import hashlib


def normalize_price(price_str: str) -> Dict[str, Union[str, float]]:
    """
    Normalize price strings to structured format
    
    Args:
        price_str: Raw price string from scraper
        
    Returns:
        Dict with raw, numeric, and currency values
    """
    result = {
        'raw': price_str,
        'numeric': None,
        'currency': 'USD'
    }
    
    if not price_str:
        return result
    
    # Clean the string
    clean_str = price_str.strip().upper()
    
    # Handle special cases
    if any(phrase in clean_str for phrase in ['NOT DISCLOSED', 'CONFIDENTIAL', 'CONTACT', 'NEGOTIABLE']):
        return result
    
    # Extract numeric value
    # Look for patterns like $1,234,567, $1.2M, $500K, etc.
    patterns = [
        r'(?:USD?\s*)?[$]?([0-9,]+(?:\.[0-9]{1,2})?)\s*([KMB])?',
        r'([0-9,]+(?:\.[0-9]{1,2})?)\s*([KMB])?\s*(?:USD?|DOLLARS?)?',
        r'([0-9,]+(?:\.[0-9]{1,2})?)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, clean_str)
        if match:
            try:
                # Extract number and multiplier
                number_str = match.group(1).replace(',', '')
                multiplier_str = match.group(2) if len(match.groups()) > 1 else None
                
                numeric_value = float(number_str)
                
                # Apply multiplier
                if multiplier_str:
                    if multiplier_str == 'K':
                        numeric_value *= 1_000
                    elif multiplier_str == 'M':
                        numeric_value *= 1_000_000
                    elif multiplier_str == 'B':
                        numeric_value *= 1_000_000_000
                
                # Validate reasonable business price range
                if 1_000 <= numeric_value <= 1_000_000_000:
                    result['numeric'] = numeric_value
                    break
                    
            except (ValueError, IndexError):
                continue
    
    return result


def normalize_location(location_str: str) -> Dict[str, Optional[str]]:
    """
    Normalize location strings to structured format
    
    Args:
        location_str: Raw location string from scraper
        
    Returns:
        Dict with country, state, city components
    """
    result = {
        'raw': location_str,
        'country': None,
        'state': None,
        'city': None
    }
    
    if not location_str:
        return result
    
    # US states mapping
    us_states = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
        'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
        'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming'
    }
    
    clean_location = location_str.strip()
    
    # Look for US state patterns
    for abbr, full_name in us_states.items():
        if abbr in clean_location.upper():
            result['state'] = abbr
            result['country'] = 'US'
            break
        elif full_name.upper() in clean_location.upper():
            result['state'] = abbr
            result['country'] = 'US'
            break
    
    # Extract city (before state)
    if result['state']:
        city_match = re.search(rf'([^,]+),?\s*{result["state"]}', clean_location, re.IGNORECASE)
        if city_match:
            result['city'] = city_match.group(1).strip()
    
    # Handle international locations
    if not result['country']:
        international_patterns = [
            r'(Canada|UK|United Kingdom|Australia|Germany|France)',
            r'(Toronto|London|Sydney|Berlin|Paris)'
        ]
        
        for pattern in international_patterns:
            match = re.search(pattern, clean_location, re.IGNORECASE)
            if match:
                location_found = match.group(1).lower()
                if location_found in ['canada', 'toronto']:
                    result['country'] = 'CA'
                elif location_found in ['uk', 'united kingdom', 'london']:
                    result['country'] = 'GB'
                elif location_found in ['australia', 'sydney']:
                    result['country'] = 'AU'
                elif location_found in ['germany', 'berlin']:
                    result['country'] = 'DE'
                elif location_found in ['france', 'paris']:
                    result['country'] = 'FR'
                break
    
    return result


def normalize_niches(niches_list: List[str]) -> List[str]:
    """
    Normalize and standardize niche categories
    
    Args:
        niches_list: List of raw niche strings
        
    Returns:
        List of normalized niche strings
    """
    if not niches_list:
        return []
    
    # Niche mapping for standardization
    niche_mapping = {
        'amazon fba': 'Amazon FBA',
        'amazon': 'Amazon FBA',
        'fba': 'Amazon FBA',
        'ecommerce': 'E-commerce',
        'e-commerce': 'E-commerce',
        'saas': 'SaaS',
        'software': 'SaaS',
        'content': 'Content',
        'blog': 'Content',
        'affiliate': 'Affiliate Marketing',
        'dropshipping': 'Dropshipping',
        'wholesale': 'Wholesale',
        'retail': 'Retail',
        'marketplace': 'Marketplace',
        'service': 'Service Business',
        'consulting': 'Consulting',
        'digital marketing': 'Digital Marketing',
        'seo': 'SEO',
        'advertising': 'Advertising'
    }
    
    normalized = []
    for niche in niches_list:
        if not niche:
            continue
            
        clean_niche = niche.lower().strip()
        
        # Check for direct mapping
        if clean_niche in niche_mapping:
            normalized.append(niche_mapping[clean_niche])
        else:
            # Check for partial matches
            for key, value in niche_mapping.items():
                if key in clean_niche:
                    normalized.append(value)
                    break
            else:
                # Keep original if no mapping found
                normalized.append(niche.title())
    
    return list(set(normalized))  # Remove duplicates


def calculate_metrics(asking_price: float, revenue: float, profit: float) -> Dict[str, Optional[float]]:
    """
    Calculate business metrics from financial data
    
    Args:
        asking_price: Asking price numeric value
        revenue: Revenue numeric value
        profit: Profit numeric value
        
    Returns:
        Dict with calculated metrics
    """
    result = {
        'price_to_revenue_multiple': None,
        'price_to_profit_multiple': None,
        'profit_margin_percent': None
    }
    
    # Price to revenue multiple
    if asking_price and revenue and revenue > 0:
        result['price_to_revenue_multiple'] = round(asking_price / revenue, 2)
    
    # Price to profit multiple
    if asking_price and profit and profit > 0:
        result['price_to_profit_multiple'] = round(asking_price / profit, 2)
    
    # Profit margin
    if revenue and profit and revenue > 0:
        result['profit_margin_percent'] = round((profit / revenue) * 100, 2)
    
    return result


def calculate_data_completeness(data: Dict[str, Any]) -> float:
    """
    Calculate data completeness score (0-1)
    
    Args:
        data: Dictionary of listing data
        
    Returns:
        Completeness score between 0 and 1
    """
    # Define important fields and their weights
    fields_weights = {
        'title': 0.15,
        'asking_price_numeric': 0.20,
        'revenue_numeric': 0.15,
        'profit_numeric': 0.15,
        'description': 0.10,
        'location_raw': 0.08,
        'niches': 0.10,
        'business_type': 0.05,
        'established_year': 0.02
    }
    
    total_weight = 0
    achieved_weight = 0
    
    for field, weight in fields_weights.items():
        total_weight += weight
        
        if field in data:
            value = data[field]
            
            # Check if field has meaningful value
            if value is not None:
                if isinstance(value, str) and value.strip():
                    achieved_weight += weight
                elif isinstance(value, (int, float)) and value > 0:
                    achieved_weight += weight
                elif isinstance(value, list) and len(value) > 0:
                    achieved_weight += weight
                elif isinstance(value, bool):
                    achieved_weight += weight
    
    return round(achieved_weight / total_weight, 3) if total_weight > 0 else 0


def generate_listing_id(source: str, url: str, title: str) -> str:
    """
    Generate a consistent listing ID
    
    Args:
        source: Source marketplace name
        url: Listing URL
        title: Listing title
        
    Returns:
        Consistent listing ID
    """
    # Use URL as primary identifier, fallback to title
    identifier = url if url else title
    
    if not identifier:
        # Generate random ID if no identifier available
        identifier = f"{source}_{datetime.now().isoformat()}"
    
    # Create hash for consistency
    hash_object = hashlib.md5(identifier.encode())
    hash_hex = hash_object.hexdigest()[:12]
    
    # Format: SOURCE_HASH
    source_code = {
        'Empire Flippers': 'EF',
        'BizQuest': 'BQ',
        'Quiet Light': 'QL',
        'BizBuySell': 'BBS'
    }.get(source, 'UNK')
    
    return f"{source_code}_{hash_hex}"


def transform_firestore_to_bigquery(firestore_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Firestore document to BigQuery format
    
    Args:
        firestore_doc: Raw Firestore document
        
    Returns:
        Transformed document for BigQuery
    """
    # Extract basic fields
    source = firestore_doc.get('source', '')
    title = firestore_doc.get('title') or firestore_doc.get('name', '')
    url = firestore_doc.get('url', '')
    
    # Generate consistent listing ID
    listing_id = generate_listing_id(source, url, title)
    
    # Normalize financial data
    price_data = normalize_price(firestore_doc.get('price', ''))
    revenue_data = normalize_price(firestore_doc.get('revenue', ''))
    profit_data = normalize_price(firestore_doc.get('profit', ''))
    
    # Normalize location
    location_data = normalize_location(firestore_doc.get('location', ''))
    
    # Normalize niches
    raw_niches = firestore_doc.get('niches', [])
    if isinstance(raw_niches, str):
        raw_niches = [raw_niches]
    niches = normalize_niches(raw_niches)
    
    # Calculate metrics
    metrics = calculate_metrics(
        price_data['numeric'],
        revenue_data['numeric'],
        profit_data['numeric']
    )
    
    # Build transformed document
    transformed = {
        'listing_id': listing_id,
        'source': source,
        'source_url': url,
        'title': title,
        'description': firestore_doc.get('description', ''),
        'business_type': firestore_doc.get('business_type') or firestore_doc.get('category'),
        'category': firestore_doc.get('category'),
        
        # Financial data
        'asking_price_raw': price_data['raw'],
        'asking_price_numeric': price_data['numeric'],
        'asking_price_currency': price_data['currency'],
        
        'revenue_raw': revenue_data['raw'],
        'revenue_numeric': revenue_data['numeric'],
        'revenue_currency': revenue_data['currency'],
        'revenue_period': 'annual',  # Default assumption
        
        'profit_raw': profit_data['raw'],
        'profit_numeric': profit_data['numeric'],
        'profit_currency': profit_data['currency'],
        'profit_period': 'annual',  # Default assumption
        
        # Calculated metrics
        'price_to_revenue_multiple': metrics['price_to_revenue_multiple'],
        'price_to_profit_multiple': metrics['price_to_profit_multiple'],
        'profit_margin_percent': metrics['profit_margin_percent'],
        
        # Location
        'location_raw': location_data['raw'],
        'country': location_data['country'],
        'state': location_data['state'],
        'city': location_data['city'],
        
        # Business characteristics
        'established_date': firestore_doc.get('established_date'),
        'niches': niches,
        'tags': firestore_doc.get('tags', []),
        
        # Operational details
        'monetization_method': firestore_doc.get('monetization_method'),
        'traffic_source': firestore_doc.get('traffic_source'),
        'business_model': firestore_doc.get('business_model'),
        
        # Data quality
        'is_active': firestore_doc.get('is_active', True),
        'last_verified': firestore_doc.get('last_updated'),
        
        # Timestamps
        'first_seen': firestore_doc.get('first_seen'),
        'last_updated': firestore_doc.get('last_updated'),
        'scraped_at': datetime.utcnow(),
        'ingestion_date': date.today(),
        
        # Raw data preservation
        'raw_data': json.dumps(firestore_doc, default=lambda o: o.isoformat())
    }
    
    # Calculate data completeness
    transformed['data_completeness_score'] = calculate_data_completeness(transformed)
    
    return transformed