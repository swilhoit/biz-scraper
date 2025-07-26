#!/usr/bin/env python3
"""
Enhanced Business Listing Detail Scraper
Extracts comprehensive information from individual listing pages
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import time
import re
import json
from urllib.parse import urlparse, urljoin
from typing import List, Dict, Optional, Tuple, Any
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnhancedDetailScraper:
    def __init__(self):
        self.api_key = os.getenv('SCRAPER_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPER_API_KEY not found in .env file")
        
        self.base_url = "http://api.scraperapi.com"
        self.session = requests.Session()
        self.detailed_data = []
        self.lock = threading.Lock()
        
        # Load the CSV file from the first scraper
        self.listings = self.load_listings()
        
    def load_listings(self, filename: str = 'business_listings.csv') -> pd.DataFrame:
        """Load the business listings from CSV"""
        try:
            df = pd.read_csv(filename)
            logger.info(f"Loaded {len(df)} listings from {filename}")
            return df
        except FileNotFoundError:
            logger.error(f"File {filename} not found. Please run the first scraper first.")
            raise
    
    def make_request(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """Make a request using Scraper API with retries"""
        params = {
            'api_key': self.api_key,
            'url': url,
            'render': 'true'  # Enable JavaScript rendering
        }
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(self.base_url, params=params, timeout=90)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
        return None
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace and newlines
        text = re.sub(r'\s+', ' ', text.strip())
        return text.strip()
    
    def extract_all_financial_metrics(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract comprehensive financial metrics from the page"""
        metrics = {
            # Pricing
            'asking_price': '',
            'minimum_bid': '',
            'buy_it_now_price': '',
            'reserve_price': '',
            
            # Revenue metrics
            'annual_revenue': '',
            'monthly_revenue': '',
            'ttm_revenue': '',  # Trailing twelve months
            'revenue_growth_rate': '',
            'revenue_breakdown': '',
            
            # Profit metrics
            'annual_profit': '',
            'monthly_profit': '',
            'net_profit': '',
            'gross_profit': '',
            'operating_profit': '',
            'ebitda': '',
            'sde': '',  # Seller's Discretionary Earnings
            'profit_margin': '',
            
            # Valuation metrics
            'multiple': '',
            'revenue_multiple': '',
            'profit_multiple': '',
            'ebitda_multiple': '',
            
            # Other financial data
            'inventory_value': '',
            'accounts_receivable': '',
            'working_capital': '',
            'debt': '',
            'capex': '',
            'tax_rate': '',
            'break_even': '',
            
            # Performance metrics
            'conversion_rate': '',
            'average_order_value': '',
            'customer_acquisition_cost': '',
            'lifetime_value': '',
            'churn_rate': '',
            'retention_rate': '',
        }
        
        # Get all text from the page
        page_text = soup.get_text()
        
        # Comprehensive patterns for financial data
        financial_patterns = {
            'asking_price': [
                r'asking\s*price[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'price[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'for\s*sale[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'
            ],
            'annual_revenue': [
                r'annual\s*revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'yearly\s*revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?\s*(?:per\s*year|\/year|annually)'
            ],
            'monthly_revenue': [
                r'monthly\s*revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?\s*(?:per\s*month|\/month|monthly)'
            ],
            'ttm_revenue': [
                r'ttm\s*revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'trailing\s*twelve\s*months?\s*revenue[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'
            ],
            'net_profit': [
                r'net\s*profit[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'net\s*income[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'
            ],
            'ebitda': [
                r'ebitda[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'adjusted\s*ebitda[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'
            ],
            'sde': [
                r'sde[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r"seller'?s?\s*discretionary\s*earnings?[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?"
            ],
            'multiple': [
                r'multiple[:\s]*[\d.]+x?',
                r'[\d.]+x?\s*multiple',
                r'valuation[:\s]*[\d.]+x?'
            ],
            'profit_margin': [
                r'profit\s*margin[:\s]*[\d.]+%?',
                r'margin[:\s]*[\d.]+%?',
                r'net\s*margin[:\s]*[\d.]+%?'
            ],
            'inventory_value': [
                r'inventory[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?',
                r'stock\s*value[:\s]*\$?[\d,]+(?:\.\d+)?[KkMm]?'
            ]
        }
        
        # Search for each metric
        for metric, patterns in financial_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    metrics[metric] = self.clean_text(match.group())
                    break
        
        # Look for financial tables
        tables = soup.find_all('table')
        for table in tables:
            self._extract_from_table(table, metrics)
        
        # Look for definition lists (common for financial data)
        dl_elements = soup.find_all('dl')
        for dl in dl_elements:
            self._extract_from_dl(dl, metrics)
        
        return metrics
    
    def _extract_from_table(self, table: BeautifulSoup, metrics: Dict[str, str]) -> None:
        """Extract financial data from tables"""
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                key = self.clean_text(cells[0].get_text()).lower()
                value = self.clean_text(cells[1].get_text())
                
                # Map table keys to our metrics
                key_mapping = {
                    'asking price': 'asking_price',
                    'price': 'asking_price',
                    'revenue': 'annual_revenue',
                    'annual revenue': 'annual_revenue',
                    'monthly revenue': 'monthly_revenue',
                    'profit': 'annual_profit',
                    'net profit': 'net_profit',
                    'gross profit': 'gross_profit',
                    'ebitda': 'ebitda',
                    'sde': 'sde',
                    'inventory': 'inventory_value',
                    'multiple': 'multiple',
                    'margin': 'profit_margin',
                    'profit margin': 'profit_margin',
                    'conversion rate': 'conversion_rate',
                    'aov': 'average_order_value',
                    'average order value': 'average_order_value'
                }
                
                for table_key, metric_key in key_mapping.items():
                    if table_key in key and not metrics.get(metric_key):
                        metrics[metric_key] = value
    
    def _extract_from_dl(self, dl: BeautifulSoup, metrics: Dict[str, str]) -> None:
        """Extract data from definition lists"""
        dts = dl.find_all('dt')
        dds = dl.find_all('dd')
        
        for dt, dd in zip(dts, dds):
            key = self.clean_text(dt.get_text()).lower()
            value = self.clean_text(dd.get_text())
            
            # Similar mapping as tables
            if 'price' in key and not metrics.get('asking_price'):
                metrics['asking_price'] = value
            elif 'revenue' in key and not metrics.get('annual_revenue'):
                metrics['annual_revenue'] = value
            # Add more mappings as needed
    
    def extract_comprehensive_business_details(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract comprehensive business details"""
        details = {
            # Basic information
            'business_name': '',
            'listing_id': '',
            'business_type': '',
            'industry': '',
            'sub_industry': '',
            'niche': '',
            'business_model': '',
            
            # History and establishment
            'established_date': '',
            'years_in_business': '',
            'incorporation_type': '',
            'ownership_percentage': '',
            
            # Location and operations
            'location': '',
            'city': '',
            'state': '',
            'country': '',
            'operates_in': '',
            'headquarters': '',
            'facilities': '',
            'warehouse_locations': '',
            
            # Team and employees
            'employees': '',
            'full_time_employees': '',
            'part_time_employees': '',
            'contractors': '',
            'key_personnel': '',
            'management_staying': '',
            
            # Products and services
            'main_products': '',
            'number_of_skus': '',
            'top_selling_products': '',
            'product_categories': '',
            'services_offered': '',
            'private_label': '',
            'proprietary_products': '',
            
            # Sales and marketing
            'sales_channels': '',
            'marketing_channels': '',
            'customer_base': '',
            'number_of_customers': '',
            'repeat_customer_rate': '',
            'customer_concentration': '',
            'geographic_distribution': '',
            
            # Operations
            'fulfillment_method': '',
            'suppliers': '',
            'number_of_suppliers': '',
            'supplier_relationships': '',
            'inventory_turnover': '',
            'lead_times': '',
            
            # Technology and IP
            'technology_stack': '',
            'software_used': '',
            'patents': '',
            'trademarks': '',
            'copyrights': '',
            'domain_names': '',
            
            # Growth and opportunity
            'growth_rate': '',
            'growth_opportunities': '',
            'expansion_potential': '',
            'competitive_advantages': '',
            'market_position': '',
            
            # Risk and challenges
            'competition_level': '',
            'main_competitors': '',
            'market_risks': '',
            'operational_risks': '',
            'regulatory_risks': '',
            
            # Reason for sale
            'reason_for_selling': '',
            'seller_involvement_post_sale': '',
            'training_provided': '',
            'transition_period': '',
            
            # Additional info
            'certifications': '',
            'licenses': '',
            'memberships': '',
            'awards': '',
            'media_mentions': '',
            'customer_reviews_rating': '',
            'social_media_followers': '',
        }
        
        # Get all text
        page_text = soup.get_text()
        
        # Comprehensive patterns for business details
        detail_patterns = {
            'established_date': [
                r'established[:\s]*(\d{4})',
                r'founded[:\s]*(\d{4})',
                r'since[:\s]*(\d{4})',
                r'started[:\s]*(\d{4})'
            ],
            'years_in_business': [
                r'(\d+)\s*years?\s*in\s*business',
                r'operating\s*for\s*(\d+)\s*years?',
                r'(\d+)\s*years?\s*old'
            ],
            'employees': [
                r'employees?[:\s]*(\d+)',
                r'staff[:\s]*(\d+)',
                r'team\s*size[:\s]*(\d+)',
                r'(\d+)\s*employees?'
            ],
            'location': [
                r'location[:\s]*([^,\n]+)',
                r'based\s*in[:\s]*([^,\n]+)',
                r'located\s*in[:\s]*([^,\n]+)'
            ],
            'industry': [
                r'industry[:\s]*([^,\n]+)',
                r'sector[:\s]*([^,\n]+)',
                r'category[:\s]*([^,\n]+)'
            ],
            'number_of_skus': [
                r'(\d+)\s*skus?',
                r'skus?[:\s]*(\d+)',
                r'(\d+)\s*products?'
            ],
            'reason_for_selling': [
                r'reason\s*for\s*sell(?:ing)?[:\s]*([^.]+)',
                r'why\s*sell(?:ing)?[:\s]*([^.]+)',
                r'seller\s*motivation[:\s]*([^.]+)'
            ],
            'growth_rate': [
                r'growth\s*rate[:\s]*(\d+%)',
                r'(\d+%)\s*growth',
                r'growing\s*at\s*(\d+%)'
            ]
        }
        
        # Search for patterns
        for detail, patterns in detail_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    details[detail] = match.group(1) if match.groups() else self.clean_text(match.group())
                    break
        
        # Extract from specific sections
        self._extract_from_sections(soup, details)
        
        return details
    
    def _extract_from_sections(self, soup: BeautifulSoup, details: Dict[str, str]) -> None:
        """Extract details from specific page sections"""
        # Look for business overview/description sections
        overview_selectors = [
            'div.business-overview',
            'section.overview',
            'div.description',
            'div.listing-details',
            'div.business-details'
        ]
        
        for selector in overview_selectors:
            section = soup.select_one(selector)
            if section:
                section_text = section.get_text()
                
                # Extract specific details from this section
                if not details['business_model']:
                    model_match = re.search(r'business\s*model[:\s]*([^.]+)', section_text, re.IGNORECASE)
                    if model_match:
                        details['business_model'] = self.clean_text(model_match.group(1))
                
                if not details['main_products']:
                    products_match = re.search(r'products?[:\s]*([^.]+)', section_text, re.IGNORECASE)
                    if products_match:
                        details['main_products'] = self.clean_text(products_match.group(1))
    
    def extract_assets_and_included_items(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract detailed information about assets included in the sale"""
        assets_data = {
            'included_in_sale': [],
            'not_included': [],
            'real_estate_included': '',
            'equipment_included': [],
            'vehicles_included': [],
            'furniture_fixtures': [],
            'technology_assets': [],
            'intellectual_property': [],
            'customer_lists': '',
            'vendor_contracts': '',
            'lease_details': '',
            'franchise_agreement': '',
            'training_materials': '',
            'marketing_materials': '',
            'social_media_accounts': [],
            'websites_included': [],
            'inventory_details': '',
            'prepaid_expenses': '',
            'deposits': '',
            'licenses_permits': [],
        }
        
        # Look for "included in sale" sections
        included_keywords = [
            'included in sale', 'what\'s included', 'sale includes',
            'assets included', 'package includes', 'comes with'
        ]
        
        for keyword in included_keywords:
            elements = soup.find_all(string=re.compile(keyword, re.IGNORECASE))
            for element in elements:
                parent = element.parent
                if parent:
                    # Look for lists
                    lists = parent.find_next_siblings(['ul', 'ol'], limit=3)
                    lists.extend(parent.find_all(['ul', 'ol']))
                    
                    for lst in lists:
                        items = lst.find_all('li')
                        for item in items:
                            item_text = self.clean_text(item.get_text())
                            if item_text:
                                assets_data['included_in_sale'].append(item_text)
                                
                                # Categorize the asset
                                self._categorize_asset(item_text.lower(), assets_data)
        
        # Look for "not included" sections
        not_included_keywords = ['not included', 'excluded', 'separate']
        for keyword in not_included_keywords:
            elements = soup.find_all(string=re.compile(keyword, re.IGNORECASE))
            for element in elements:
                parent = element.parent
                if parent:
                    next_text = parent.find_next_sibling()
                    if next_text:
                        assets_data['not_included'].append(self.clean_text(next_text.get_text()))
        
        # Remove duplicates
        assets_data['included_in_sale'] = list(set(assets_data['included_in_sale']))
        assets_data['not_included'] = list(set(assets_data['not_included']))
        
        return assets_data
    
    def _categorize_asset(self, asset_text: str, assets_data: Dict[str, Any]) -> None:
        """Categorize an asset based on its description"""
        if any(word in asset_text for word in ['equipment', 'machine', 'tool']):
            assets_data['equipment_included'].append(asset_text)
        elif any(word in asset_text for word in ['vehicle', 'truck', 'van', 'car']):
            assets_data['vehicles_included'].append(asset_text)
        elif any(word in asset_text for word in ['furniture', 'fixture', 'desk', 'chair']):
            assets_data['furniture_fixtures'].append(asset_text)
        elif any(word in asset_text for word in ['software', 'computer', 'server', 'technology']):
            assets_data['technology_assets'].append(asset_text)
        elif any(word in asset_text for word in ['patent', 'trademark', 'copyright', 'ip']):
            assets_data['intellectual_property'].append(asset_text)
        elif any(word in asset_text for word in ['website', 'domain', 'url']):
            assets_data['websites_included'].append(asset_text)
        elif any(word in asset_text for word in ['social media', 'facebook', 'instagram', 'twitter']):
            assets_data['social_media_accounts'].append(asset_text)
        elif any(word in asset_text for word in ['license', 'permit', 'certification']):
            assets_data['licenses_permits'].append(asset_text)
    
    def extract_traffic_and_marketing_metrics(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract traffic and marketing related metrics"""
        metrics = {
            # Traffic metrics
            'monthly_visitors': '',
            'unique_visitors': '',
            'page_views': '',
            'bounce_rate': '',
            'average_session_duration': '',
            'traffic_sources': '',
            'organic_traffic_percentage': '',
            'paid_traffic_percentage': '',
            'direct_traffic_percentage': '',
            'social_traffic_percentage': '',
            
            # SEO metrics
            'domain_authority': '',
            'number_of_backlinks': '',
            'referring_domains': '',
            'keyword_rankings': '',
            'top_keywords': '',
            
            # Social media metrics
            'facebook_followers': '',
            'instagram_followers': '',
            'twitter_followers': '',
            'youtube_subscribers': '',
            'linkedin_followers': '',
            'tiktok_followers': '',
            'email_subscribers': '',
            
            # Engagement metrics
            'engagement_rate': '',
            'click_through_rate': '',
            'email_open_rate': '',
            'social_engagement': '',
            
            # Advertising metrics
            'ad_spend': '',
            'cost_per_acquisition': '',
            'return_on_ad_spend': '',
            'advertising_channels': '',
        }
        
        # Get page text
        page_text = soup.get_text()
        
        # Traffic patterns
        traffic_patterns = {
            'monthly_visitors': [
                r'monthly\s*visitors?[:\s]*[\d,]+',
                r'[\d,]+\s*visitors?\s*per\s*month',
                r'traffic[:\s]*[\d,]+\s*\/month'
            ],
            'unique_visitors': [
                r'unique\s*visitors?[:\s]*[\d,]+',
                r'[\d,]+\s*unique\s*visitors?'
            ],
            'email_subscribers': [
                r'email\s*subscribers?[:\s]*[\d,]+',
                r'[\d,]+\s*email\s*subscribers?',
                r'mailing\s*list[:\s]*[\d,]+'
            ],
            'domain_authority': [
                r'domain\s*authority[:\s]*\d+',
                r'da[:\s]*\d+',
                r'moz\s*da[:\s]*\d+'
            ]
        }
        
        # Search for metrics
        for metric, patterns in traffic_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    metrics[metric] = self.clean_text(match.group())
                    break
        
        # Look for traffic/analytics sections
        analytics_sections = soup.find_all(['div', 'section'], class_=re.compile('traffic|analytics|metrics|statistics'))
        for section in analytics_sections:
            section_text = section.get_text()
            # Extract any numbers with context
            numbers = re.findall(r'([\w\s]+)[:\s]*([\d,]+(?:\.\d+)?%?)', section_text)
            for context, value in numbers:
                context_lower = context.lower().strip()
                if 'visitor' in context_lower and not metrics['monthly_visitors']:
                    metrics['monthly_visitors'] = value
                elif 'subscriber' in context_lower and not metrics['email_subscribers']:
                    metrics['email_subscribers'] = value
                elif 'follower' in context_lower:
                    # Try to identify which platform
                    if 'facebook' in context_lower:
                        metrics['facebook_followers'] = value
                    elif 'instagram' in context_lower:
                        metrics['instagram_followers'] = value
                    elif 'twitter' in context_lower:
                        metrics['twitter_followers'] = value
        
        return metrics
    
    def extract_marketplace_specific_data(self, soup: BeautifulSoup, domain: str) -> Dict[str, Any]:
        """Extract data specific to each marketplace"""
        specific_data = {}
        
        if 'quietlight' in domain:
            specific_data = self._extract_quietlight_specific(soup)
        elif 'empireflippers' in domain:
            specific_data = self._extract_empireflippers_specific(soup)
        elif 'flippa' in domain:
            specific_data = self._extract_flippa_specific(soup)
        elif 'bizbuysell' in domain:
            specific_data = self._extract_bizbuysell_specific(soup)
        elif 'websiteproperties' in domain:
            specific_data = self._extract_websiteproperties_specific(soup)
        elif 'investors.club' in domain:
            specific_data = self._extract_investorsclub_specific(soup)
        elif 'acquire.com' in domain:
            specific_data = self._extract_acquire_specific(soup)
        
        return specific_data
    
    def _extract_quietlight_specific(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract QuietLight specific data"""
        data = {
            'ql_listing_number': '',
            'ql_broker': '',
            'ql_listing_status': '',
            'ql_seller_interview': '',
            'ql_financial_verified': '',
            'ql_loi_process': ''
        }
        
        # Look for QuietLight specific elements
        listing_header = soup.find('div', class_='listing-header')
        if listing_header:
            # Extract listing number
            listing_num = listing_header.find(string=re.compile(r'Listing #\d+'))
            if listing_num:
                data['ql_listing_number'] = self.clean_text(listing_num)
        
        # Look for broker information
        broker_section = soup.find('div', class_=['broker-info', 'advisor-info'])
        if broker_section:
            broker_name = broker_section.find(['h3', 'h4', 'p'])
            if broker_name:
                data['ql_broker'] = self.clean_text(broker_name.get_text())
        
        return data
    
    def _extract_empireflippers_specific(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract Empire Flippers specific data"""
        data = {
            'ef_listing_number': '',
            'ef_monetization': '',
            'ef_content_type': '',
            'ef_link_profile': '',
            'ef_work_required': '',
            'ef_growth_trend': ''
        }
        
        # Empire Flippers often has specific data attributes
        listing_data = soup.find('div', {'data-listing-id': True})
        if listing_data:
            data['ef_listing_number'] = listing_data.get('data-listing-id', '')
        
        # Look for their specific metrics sections
        metrics_cards = soup.find_all('div', class_='metric-card')
        for card in metrics_cards:
            title = card.find('h4', class_='metric-title')
            value = card.find('div', class_='metric-value')
            if title and value:
                title_text = self.clean_text(title.get_text()).lower()
                value_text = self.clean_text(value.get_text())
                
                if 'monetization' in title_text:
                    data['ef_monetization'] = value_text
                elif 'content' in title_text:
                    data['ef_content_type'] = value_text
                elif 'work' in title_text:
                    data['ef_work_required'] = value_text
        
        return data
    
    def _extract_flippa_specific(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract Flippa specific data"""
        data = {
            'flippa_auction_end': '',
            'flippa_bid_count': '',
            'flippa_current_bid': '',
            'flippa_bin_price': '',
            'flippa_verified_metrics': '',
            'flippa_seller_rating': ''
        }
        
        # Auction information
        auction_info = soup.find('div', class_=['auction-info', 'bidding-section'])
        if auction_info:
            # End time
            end_time = auction_info.find(string=re.compile(r'ends in|time left', re.IGNORECASE))
            if end_time:
                data['flippa_auction_end'] = self.clean_text(end_time.parent.get_text())
            
            # Bid count
            bid_count = auction_info.find(string=re.compile(r'\d+\s*bids?'))
            if bid_count:
                data['flippa_bid_count'] = self.clean_text(bid_count)
        
        # Seller information
        seller_info = soup.find('div', class_='seller-info')
        if seller_info:
            rating = seller_info.find(class_=['rating', 'stars'])
            if rating:
                data['flippa_seller_rating'] = self.clean_text(rating.get_text())
        
        return data
    
    def _extract_bizbuysell_specific(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract BizBuySell specific data"""
        data = {
            'bbs_listing_id': '',
            'bbs_business_category': '',
            'bbs_franchise': '',
            'bbs_relocatable': '',
            'bbs_financing_available': '',
            'bbs_training_support': ''
        }
        
        # Look for listing ID
        listing_id = soup.find('span', class_='listing-id')
        if listing_id:
            data['bbs_listing_id'] = self.clean_text(listing_id.get_text())
        
        # Business details section
        details_section = soup.find('section', class_='business-details')
        if details_section:
            # Look for specific fields
            fields = details_section.find_all('div', class_='detail-item')
            for field in fields:
                label = field.find('span', class_='label')
                value = field.find('span', class_='value')
                if label and value:
                    label_text = self.clean_text(label.get_text()).lower()
                    value_text = self.clean_text(value.get_text())
                    
                    if 'franchise' in label_text:
                        data['bbs_franchise'] = value_text
                    elif 'relocatable' in label_text:
                        data['bbs_relocatable'] = value_text
                    elif 'financing' in label_text:
                        data['bbs_financing_available'] = value_text
        
        return data
    
    def _extract_websiteproperties_specific(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract Website Properties specific data"""
        data = {
            'wp_listing_id': '',
            'wp_seller_financing': '',
            'wp_earn_out': '',
            'wp_training_period': '',
            'wp_support_period': '',
            'wp_non_compete': ''
        }
        
        # Look for listing ID in various places
        listing_id_match = re.search(r'listing[:\s]*#?(\d+)', soup.get_text(), re.IGNORECASE)
        if listing_id_match:
            data['wp_listing_id'] = listing_id_match.group(1)
        
        # Terms section
        terms_section = soup.find(['div', 'section'], class_=['terms', 'deal-terms'])
        if terms_section:
            terms_text = terms_section.get_text()
            
            # Seller financing
            if 'seller financing' in terms_text.lower():
                financing_match = re.search(r'seller\s*financing[:\s]*([^,\n]+)', terms_text, re.IGNORECASE)
                if financing_match:
                    data['wp_seller_financing'] = self.clean_text(financing_match.group(1))
            
            # Training period
            training_match = re.search(r'training[:\s]*(\d+\s*(?:hours?|days?|weeks?))', terms_text, re.IGNORECASE)
            if training_match:
                data['wp_training_period'] = training_match.group(1)
        
        return data
    
    def _extract_investorsclub_specific(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract Investors Club specific data"""
        data = {
            'ic_broker_managed': '',
            'ic_seller_type': '',
            'ic_deal_structure': '',
            'ic_verified_financials': '',
            'ic_growth_score': ''
        }
        
        # Check if broker managed
        broker_badge = soup.find('span', class_=['broker-badge', 'managed-by-broker'])
        if broker_badge:
            data['ic_broker_managed'] = 'Yes'
        
        # Deal structure
        deal_section = soup.find('div', class_='deal-structure')
        if deal_section:
            data['ic_deal_structure'] = self.clean_text(deal_section.get_text())
        
        return data
    
    def _extract_acquire_specific(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract Acquire.com specific data"""
        data = {
            'acquire_startup_stage': '',
            'acquire_tech_stack': '',
            'acquire_team_size': '',
            'acquire_investors': '',
            'acquire_revenue_model': ''
        }
        
        # Startup details
        startup_details = soup.find('div', class_=['startup-details', 'company-details'])
        if startup_details:
            # Stage
            stage = startup_details.find(string=re.compile(r'stage|phase', re.IGNORECASE))
            if stage:
                data['acquire_startup_stage'] = self.clean_text(stage.parent.get_text())
            
            # Tech stack
            tech = startup_details.find(string=re.compile(r'tech stack|technology', re.IGNORECASE))
            if tech:
                data['acquire_tech_stack'] = self.clean_text(tech.parent.get_text())
        
        return data
    
    def extract_all_images_and_media(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract all images and media URLs"""
        media = {
            'images': [],
            'videos': [],
            'documents': [],
            'presentations': []
        }
        
        # Extract all images
        images = soup.find_all('img')
        for img in images:
            src = img.get('src', '')
            if src and not any(skip in src for skip in ['logo', 'icon', 'avatar', 'placeholder']):
                if src.startswith('//'):
                    src = 'https:' + src
                elif src.startswith('/'):
                    src = urljoin(self.base_url, src)
                media['images'].append(src)
        
        # Look for video embeds
        videos = soup.find_all(['video', 'iframe'])
        for video in videos:
            src = video.get('src', '')
            if src and any(platform in src for platform in ['youtube', 'vimeo', 'wistia']):
                media['videos'].append(src)
        
        # Look for document links
        doc_links = soup.find_all('a', href=re.compile(r'\.(pdf|doc|docx|xls|xlsx)$', re.IGNORECASE))
        for link in doc_links:
            href = link.get('href', '')
            if href:
                if href.startswith('/'):
                    href = urljoin(self.base_url, href)
                media['documents'].append(href)
        
        # Remove duplicates
        for key in media:
            media[key] = list(set(media[key]))
        
        return media
    
    def scrape_detail_page(self, row: pd.Series) -> Dict[str, Any]:
        """Scrape a single detail page with comprehensive extraction"""
        url = row['url']
        
        # Skip invalid URLs
        if not url or url == 'nan' or not url.startswith('http'):
            logger.warning(f"Skipping invalid URL: {url}")
            return None
        
        # Skip generic pages
        if any(skip in url.lower() for skip in ['pricing', 'sellers', 'buyers', 'partner', 'listing/']):
            logger.info(f"Skipping generic page: {url}")
            return None
        
        response = self.make_request(url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        domain = urlparse(url).netloc.lower()
        
        # Base data from original scrape
        detail_data = {
            'source': row['source'],
            'name': row['name'],
            'original_price': row['price'],
            'original_revenue': row['revenue'],
            'original_profit': row['profit'],
            'url': url,
            'scrape_timestamp': datetime.now().isoformat()
        }
        
        try:
            # Extract all categories of data
            logger.info(f"Extracting comprehensive data for: {row['name'][:50]}...")
            
            # Financial metrics
            detail_data.update(self.extract_all_financial_metrics(soup))
            
            # Business details
            detail_data.update(self.extract_comprehensive_business_details(soup))
            
            # Assets and included items
            assets_data = self.extract_assets_and_included_items(soup)
            detail_data['assets_included'] = assets_data['included_in_sale']
            detail_data['assets_not_included'] = assets_data['not_included']
            detail_data['equipment_list'] = assets_data['equipment_included']
            detail_data['ip_assets'] = assets_data['intellectual_property']
            
            # Traffic and marketing metrics
            detail_data.update(self.extract_traffic_and_marketing_metrics(soup))
            
            # Marketplace specific data
            marketplace_data = self.extract_marketplace_specific_data(soup, domain)
            detail_data.update(marketplace_data)
            
            # Media and images
            media_data = self.extract_all_images_and_media(soup)
            detail_data['image_urls'] = media_data['images']
            detail_data['video_urls'] = media_data['videos']
            detail_data['document_urls'] = media_data['documents']
            
            # Extract page title and meta description
            title_tag = soup.find('title')
            if title_tag:
                detail_data['page_title'] = self.clean_text(title_tag.get_text())
            
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                detail_data['meta_description'] = meta_desc.get('content', '')
            
            # Extract any structured data (JSON-LD)
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                try:
                    structured_data = json.loads(json_ld.string)
                    detail_data['structured_data'] = json.dumps(structured_data)
                except:
                    pass
            
            # Full page text (limited)
            detail_data['full_description'] = self.clean_text(soup.get_text())[:3000]
            
            logger.info(f"Successfully extracted comprehensive data for: {row['name'][:50]}...")
            return detail_data
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return detail_data
    
    def scrape_all_details(self, max_workers: int = 10, limit: Optional[int] = None):
        """Scrape all detail pages in parallel"""
        logger.info("Starting comprehensive detail page scraping...")
        
        # Filter out rows with invalid URLs
        valid_listings = self.listings[
            self.listings['url'].notna() & 
            (self.listings['url'] != '') &
            self.listings['url'].str.startswith('http')
        ]
        
        if limit:
            valid_listings = valid_listings.head(limit)
        
        logger.info(f"Scraping comprehensive details for {len(valid_listings)} valid listings...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks
            futures = []
            for idx, row in valid_listings.iterrows():
                future = executor.submit(self.scrape_detail_page, row)
                futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        with self.lock:
                            self.detailed_data.append(result)
                except Exception as e:
                    logger.error(f"Error processing future: {e}")
        
        logger.info(f"Completed scraping {len(self.detailed_data)} detail pages")
    
    def export_to_csv(self, filename: str = 'enhanced_business_details.csv'):
        """Export detailed data to CSV"""
        if not self.detailed_data:
            logger.warning("No detailed data to export")
            return
        
        df = pd.DataFrame(self.detailed_data)
        
        # Convert lists to strings for CSV export
        list_columns = ['assets_included', 'assets_not_included', 'equipment_list', 
                       'ip_assets', 'image_urls', 'video_urls', 'document_urls']
        
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: '; '.join(x) if isinstance(x, list) else x)
        
        # Priority column order
        priority_columns = [
            'source', 'name', 'page_title',
            # Pricing
            'asking_price', 'minimum_bid', 'buy_it_now_price',
            # Revenue
            'annual_revenue', 'monthly_revenue', 'ttm_revenue', 'revenue_growth_rate',
            # Profit
            'annual_profit', 'monthly_profit', 'net_profit', 'ebitda', 'sde', 'profit_margin',
            # Valuation
            'multiple', 'revenue_multiple', 'profit_multiple',
            # Business details
            'business_type', 'industry', 'sub_industry', 'established_date', 'years_in_business',
            'location', 'city', 'state', 'country',
            # Operations
            'employees', 'number_of_skus', 'sales_channels', 'fulfillment_method',
            # Growth
            'growth_rate', 'growth_opportunities', 'competitive_advantages',
            # Traffic
            'monthly_visitors', 'email_subscribers', 'conversion_rate',
            # Social
            'facebook_followers', 'instagram_followers', 'twitter_followers',
            # Assets
            'assets_included', 'inventory_value', 'real_estate_included',
            # Sale details
            'reason_for_selling', 'training_provided', 'seller_financing',
            # URL
            'url'
        ]
        
        # Only include columns that exist
        columns = [col for col in priority_columns if col in df.columns]
        # Add any remaining columns
        remaining_columns = [col for col in df.columns if col not in columns]
        columns.extend(remaining_columns)
        
        df = df[columns]
        df.to_csv(filename, index=False)
        logger.info(f"Enhanced detailed data exported to {filename}")
    
    def export_to_json(self, filename: str = 'enhanced_business_details.json'):
        """Export detailed data to JSON for better structure preservation"""
        if not self.detailed_data:
            logger.warning("No detailed data to export")
            return
        
        with open(filename, 'w') as f:
            json.dump(self.detailed_data, f, indent=2)
        logger.info(f"Enhanced detailed data exported to {filename}")
    
    def print_summary(self):
        """Print comprehensive summary of scraped data"""
        if not self.detailed_data:
            logger.info("No detailed data scraped")
            return
        
        df = pd.DataFrame(self.detailed_data)
        
        print("\n" + "="*70)
        print("ENHANCED DETAIL SCRAPING SUMMARY")
        print("="*70)
        print(f"Total detail pages scraped: {len(df)}")
        print(f"Sources: {df['source'].value_counts().to_dict()}")
        
        # Count fields with data
        print("\nData extraction summary:")
        categories = {
            'Financial Metrics': ['asking_price', 'annual_revenue', 'annual_profit', 'ebitda', 'multiple'],
            'Business Details': ['industry', 'established_date', 'employees', 'location', 'business_model'],
            'Growth & Performance': ['growth_rate', 'revenue_growth_rate', 'profit_margin', 'conversion_rate'],
            'Traffic & Marketing': ['monthly_visitors', 'email_subscribers', 'facebook_followers', 'instagram_followers'],
            'Assets & IP': ['assets_included', 'equipment_list', 'ip_assets', 'inventory_value'],
            'Sale Terms': ['reason_for_selling', 'training_provided', 'seller_financing', 'transition_period']
        }
        
        for category, fields in categories.items():
            print(f"\n{category}:")
            for field in fields:
                if field in df.columns:
                    non_empty = df[field].astype(str).str.strip().str.len().gt(0).sum()
                    if non_empty > 0:
                        print(f"  {field}: {non_empty} ({non_empty/len(df)*100:.1f}%)")
        
        # Show most complete listing
        print("\nMost complete listing example:")
        # Count non-empty fields per row
        non_empty_counts = df.apply(lambda row: row.astype(str).str.strip().str.len().gt(0).sum(), axis=1)
        most_complete_idx = non_empty_counts.idxmax()
        sample = df.loc[most_complete_idx]
        
        print(f"\nBusiness: {sample.get('name', 'N/A')}")
        print(f"Source: {sample.get('source', 'N/A')}")
        print(f"Asking Price: {sample.get('asking_price', 'N/A')}")
        print(f"Annual Revenue: {sample.get('annual_revenue', 'N/A')}")
        print(f"Annual Profit: {sample.get('annual_profit', 'N/A')}")
        print(f"EBITDA: {sample.get('ebitda', 'N/A')}")
        print(f"Multiple: {sample.get('multiple', 'N/A')}")
        print(f"Industry: {sample.get('industry', 'N/A')}")
        print(f"Location: {sample.get('location', 'N/A')}")
        print(f"Employees: {sample.get('employees', 'N/A')}")
        print(f"Growth Rate: {sample.get('growth_rate', 'N/A')}")
        print(f"Monthly Visitors: {sample.get('monthly_visitors', 'N/A')}")
        print(f"Email Subscribers: {sample.get('email_subscribers', 'N/A')}")
        print(f"Fields with data: {non_empty_counts[most_complete_idx]} out of {len(df.columns)}")


def main():
    """Main function to run the enhanced detail scraper"""
    try:
        scraper = EnhancedDetailScraper()
        
        # You can limit the number of URLs to scrape for testing
        # scraper.scrape_all_details(max_workers=5, limit=10)  # Test with 10 URLs
        
        # Or scrape all URLs
        scraper.scrape_all_details(max_workers=10)
        
        # Export results
        scraper.export_to_csv()
        scraper.export_to_json()
        scraper.print_summary()
        
    except Exception as e:
        logger.error(f"Enhanced detail scraper failed: {e}")
        raise


if __name__ == "__main__":
    main()