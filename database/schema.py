from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()

class Business(Base):
    __tablename__ = 'businesses'
    
    id = Column(Integer, primary_key=True)
    source_site = Column(String(50), nullable=False)
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
    
    # Enhanced details
    ebitda = Column(Float)
    inventory_value = Column(Float)
    ffe_value = Column(Float)  # Furniture, Fixtures & Equipment
    reason_for_selling = Column(Text)
    website = Column(String(500))
    monthly_traffic = Column(String(100))
    
    # Amazon FBA fields
    is_amazon_fba = Column(Boolean, default=False)
    amazon_business_type = Column(String(50))
    
    # Timestamps
    scraped_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    enhanced_at = Column(DateTime)
    
    def __repr__(self):
        return f"<Business(title='{self.title}', source='{self.source_site}', price={self.price})>"

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