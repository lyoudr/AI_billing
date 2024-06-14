from sqlalchemy import (
    Column , 
    BigInteger, 
    String, 
    Numeric,
    TEXT,
    Date,
    DECIMAL,
)
from . import Base 

class BillingReport(Base):
    __tablename__ = "billing_report"

    id = Column(BigInteger, primary_key=True)
    company = Column(String(100), nullable=False)
    total_cost = Column(Numeric(65, 2), nullable=False)
    discounted_cost = Column(Numeric(65, 2), nullable=False)
    tech_fee = Column(Numeric(65, 2), nullable=False)

class PubSubReport(Base):
    __tablename__ = "pubsub_report"

    id = Column(BigInteger, primary_key=True)
    company = Column(String(100), nullable=False)
    service = Column(String(100), nullable=False)
    cost = Column(Numeric(65, 2), nullable=False)

class GCPBill(Base):
    __tablename__ = 'gcp_bill'

    id = Column(BigInteger, primary_key=True)
    account_name = Column(String(100))
    account_id = Column(String(100))
    project_name = Column(String(100))
    project_id = Column(String(100))
    project_number = Column(BigInteger)
    service_name = Column(String(100))
    service_id = Column(String(100))
    service_sku = Column(TEXT)
    sku_id = Column(String(50))
    start_date = Column(Date)
    end_date = Column(Date)
    amount = Column(DECIMAL(precision=10, scale=2))
    amount_unit = Column(String(50))
    detail_fee = Column(DECIMAL(precision=10, scale=2))
    price = Column(DECIMAL(precision=10, scale=2))
    unrounded_fee = Column(DECIMAL(precision=10, scale=2))
    rounded_fee = Column(DECIMAL(precision=10, scale=2))