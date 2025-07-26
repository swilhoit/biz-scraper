"""
Database schema for business listings
"""

from datetime import datetime
from typing import Optional, Dict, Any

class BusinessListing:
    """Schema for a business listing document in Firestore"""
    
    def __init__(
        self,
        listing_id: str,
        source: str,
        name: str,
        url: str,
        price: Optional[str] = None,
        revenue: Optional[str] = None,
        profit: Optional[str] = None,
        ebitda: Optional[str] = None,
        location: Optional[str] = None,
        description: Optional[str] = None,
        category: Optional[str] = None,
        employees: Optional[str] = None,
        years_in_operation: Optional[str] = None,
        financing_available: bool = False,
        first_seen: datetime = None,
        last_updated: datetime = None,
        is_active: bool = True,
        raw_data: Optional[Dict[str, Any]] = None
    ):
        self.listing_id = listing_id
        self.source = source
        self.name = name
        self.url = url
        self.price = price
        self.revenue = revenue
        self.profit = profit
        self.ebitda = ebitda
        self.location = location
        self.description = description
        self.category = category
        self.employees = employees
        self.years_in_operation = years_in_operation
        self.financing_available = financing_available
        self.first_seen = first_seen or datetime.utcnow()
        self.last_updated = last_updated or datetime.utcnow()
        self.is_active = is_active
        self.raw_data = raw_data or {}
    
    def to_dict(self) -> dict:
        """Convert to Firestore document"""
        return {
            'listing_id': self.listing_id,
            'source': self.source,
            'name': self.name,
            'url': self.url,
            'price': self.price,
            'revenue': self.revenue,
            'profit': self.profit,
            'ebitda': self.ebitda,
            'location': self.location,
            'description': self.description,
            'category': self.category,
            'employees': self.employees,
            'years_in_operation': self.years_in_operation,
            'financing_available': self.financing_available,
            'first_seen': self.first_seen,
            'last_updated': self.last_updated,
            'is_active': self.is_active,
            'raw_data': self.raw_data
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'BusinessListing':
        """Create from Firestore document"""
        return cls(**data)


class ScraperRun:
    """Schema for tracking scraper runs"""
    
    def __init__(
        self,
        run_id: str,
        source: str,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        status: str = 'running',
        listings_found: int = 0,
        new_listings: int = 0,
        updated_listings: int = 0,
        errors: Optional[list] = None
    ):
        self.run_id = run_id
        self.source = source
        self.start_time = start_time
        self.end_time = end_time
        self.status = status
        self.listings_found = listings_found
        self.new_listings = new_listings
        self.updated_listings = updated_listings
        self.errors = errors or []
    
    def to_dict(self) -> dict:
        """Convert to Firestore document"""
        return {
            'run_id': self.run_id,
            'source': self.source,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'status': self.status,
            'listings_found': self.listings_found,
            'new_listings': self.new_listings,
            'updated_listings': self.updated_listings,
            'errors': self.errors
        }