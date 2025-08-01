"""
Amazon FBA Business Detection Utility
Identifies Amazon FBA businesses from listing data
"""
import re
from typing import Dict, List

class AmazonFBADetector:
    """Detects and flags Amazon FBA businesses"""
    
    # Keywords that indicate Amazon FBA business
    FBA_KEYWORDS = [
        'amazon fba', 'fba business', 'fulfillment by amazon',
        'amazon seller', 'amazon store', 'fba seller',
        'amazon brand', 'private label', 'amazon wholesale',
        'amazon dropship', 'amazon ppc', 'seller central',
        'amazon account', 'asin', 'buy box', 'prime eligible'
    ]
    
    # Keywords that indicate other Amazon-related but not FBA
    AMAZON_RELATED = [
        'amazon affiliate', 'amazon associates', 'kindle',
        'amazon kdp', 'amazon merch', 'audible'
    ]
    
    @classmethod
    def is_amazon_fba(cls, listing_data: Dict) -> bool:
        """
        Determine if a listing is an Amazon FBA business
        
        Args:
            listing_data: Dictionary with listing information
            
        Returns:
            bool: True if likely Amazon FBA business
        """
        # Combine all text fields for analysis
        text_fields = []
        for field in ['title', 'description', 'industry']:
            if listing_data.get(field):
                text_fields.append(str(listing_data[field]).lower())
        
        combined_text = ' '.join(text_fields)
        
        # Check for FBA keywords
        fba_score = 0
        for keyword in cls.FBA_KEYWORDS:
            if keyword in combined_text:
                fba_score += 1
        
        # Check URL
        url = listing_data.get('listing_url', '').lower()
        if 'amazon' in url or 'fba' in url:
            fba_score += 2
        
        # Industry check
        industry = listing_data.get('industry', '').lower()
        if any(term in industry for term in ['ecommerce', 'e-commerce', 'online retail']):
            fba_score += 0.5
        
        return fba_score >= 1
    
    @classmethod
    def get_amazon_type(cls, listing_data: Dict) -> str:
        """
        Get specific type of Amazon business
        
        Returns:
            str: Type of Amazon business or 'non-amazon'
        """
        text_fields = []
        for field in ['title', 'description', 'industry']:
            if listing_data.get(field):
                text_fields.append(str(listing_data[field]).lower())
        
        combined_text = ' '.join(text_fields)
        
        if cls.is_amazon_fba(listing_data):
            if 'private label' in combined_text:
                return 'amazon_fba_private_label'
            elif 'wholesale' in combined_text:
                return 'amazon_fba_wholesale'
            elif 'dropship' in combined_text:
                return 'amazon_fba_dropship'
            else:
                return 'amazon_fba'
        elif any(keyword in combined_text for keyword in cls.AMAZON_RELATED):
            if 'affiliate' in combined_text or 'associates' in combined_text:
                return 'amazon_affiliate'
            elif 'kdp' in combined_text or 'kindle' in combined_text:
                return 'amazon_kdp'
            else:
                return 'amazon_other'
        else:
            return 'non_amazon'
    
    @classmethod
    def enhance_listing(cls, listing_data: Dict) -> Dict:
        """
        Add Amazon FBA detection fields to listing
        
        Args:
            listing_data: Original listing data
            
        Returns:
            Dict: Enhanced listing with Amazon fields
        """
        listing_data['is_amazon_fba'] = cls.is_amazon_fba(listing_data)
        listing_data['amazon_business_type'] = cls.get_amazon_type(listing_data)
        return listing_data