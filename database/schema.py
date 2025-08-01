from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Boolean, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
import os
import re

from config import SITES

Base = declarative_base()

# A dictionary to hold the dynamically created table classes
TABLE_CLASSES = {}

class BusinessBase:
    """A base class for business listings, containing all common columns."""
    id = Column(Integer, primary_key=True)
    listing_url = Column(String(500), unique=True, nullable=False)
    title = Column(String(500))
    price = Column(Float)
    revenue = Column(Float)
    cash_flow = Column(Float)
    multiple = Column(Float)
    location = Column(String(200))
    industry = Column(String(200))
    description = Column(Text)
    seller_financing = Column(Boolean, default=False)
    established_year = Column(Integer)
    employees = Column(Integer)
    ebitda = Column(Float)
    inventory_value = Column(Float)
    ffe_value = Column(Float)
    reason_for_selling = Column(Text)
    website = Column(String(500))
    monthly_traffic = Column(String(100))
    is_amazon_fba = Column(Boolean, default=False)
    amazon_business_type = Column(String(50))
    scraped_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    enhanced_at = Column(DateTime)

    def __repr__(self):
        return f"<Business(title='{self.title}', price={self.price})>"

def _create_dynamic_table_class(site_name):
    """Factory function to create a new table class for a given site name."""
    # Sanitize the site name to create a valid table name
    # e.g., 'BizBuySell' -> 'businesses_bizbuysell'
    sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '', site_name).lower()
    table_name = f'businesses_{sanitized_name}'

    # Create a new class with the appropriate table name
    return type(
        f'{site_name}Business',
        (BusinessBase, Base),
        {'__tablename__': table_name}
    )

# Dynamically create and register a table class for each site
for site in SITES:
    site_name = site['name']
    TABLE_CLASSES[site_name] = _create_dynamic_table_class(site_name)

def get_table_class(site_name: str):
    """Returns the SQLAlchemy table class for a given site name."""
    return TABLE_CLASSES.get(site_name)

def get_database_url():
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'businesses.db')
    return f'sqlite:///{db_path}'

def init_database():
    engine = create_engine(get_database_url())
    Base.metadata.create_all(engine)
    return engine

def get_session():
    engine = init_database()
    Session = sessionmaker(bind=engine)
    return Session()
