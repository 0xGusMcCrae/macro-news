from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, Enum, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum as PyEnum

Base = declarative_base()

class DataSource(PyEnum):
    FRED = "fred"
    BLS = "bls"
    YAHOO = "yahoo"
    FED = "fed"
    OTHER = "other"

class AssetClass(PyEnum):
    EQUITY = "equity"
    BOND = "bond"
    FX = "fx"
    COMMODITY = "commodity"
    CRYPTO = "crypto"
    OTHER = "other"

class EconomicRelease(Base):
    """Economic data releases"""
    __tablename__ = 'economic_releases'

    id = Column(Integer, primary_key=True)
    indicator = Column(String, index=True)
    value = Column(Float)
    previous = Column(Float)
    expected = Column(Float, nullable=True)
    timestamp = Column(DateTime, index=True)
    source = Column(Enum(DataSource))
    frequency = Column(String)  # daily, weekly, monthly, quarterly
    importance = Column(String)  # high, medium, low
    analysis = Column(JSON, nullable=True)  # Store analysis results
    created_at = Column(DateTime, default=datetime.utcnow)

class MarketData(Base):
    """Market price and volume data"""
    __tablename__ = 'market_data'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    asset_class = Column(Enum(AssetClass))
    price = Column(Float)
    volume = Column(Float)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    timestamp = Column(DateTime, index=True)
    source = Column(Enum(DataSource))
    market_metadata = Column(JSON, nullable=True)  # Additional market data
    created_at = Column(DateTime, default=datetime.utcnow)

class BondData(Base):
    """Specific bond market data"""
    __tablename__ = 'bond_data'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    yield_value = Column(Float)
    price = Column(Float)
    duration = Column(Float)
    maturity_date = Column(DateTime)
    timestamp = Column(DateTime, index=True)
    coupon = Column(Float)
    market_metadata = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class FedSpeech(Base):
    """Federal Reserve communications"""
    __tablename__ = 'fed_speeches'

    id = Column(Integer, primary_key=True)
    speaker = Column(String, index=True)
    role = Column(String)
    title = Column(String)
    speech_type = Column(String)  # FOMC_STATEMENT, SPEECH, TESTIMONY
    content = Column(String)
    date = Column(DateTime, index=True)
    url = Column(String)
    analysis = Column(JSON, nullable=True)  # Store analysis results
    created_at = Column(DateTime, default=datetime.utcnow)

class MarketRegime(Base):
    """Market regime classifications"""
    __tablename__ = 'market_regimes'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, index=True)
    risk_environment = Column(String)  # risk_on, risk_off, neutral
    volatility_regime = Column(String)  # low, normal, high
    liquidity_conditions = Column(String)  # ample, normal, tight
    correlation_regime = Column(String)  # normal, crisis, disparate
    dominant_factors = Column(JSON)  # List of dominant market factors
    analysis = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Analysis(Base):
    """Stored analysis results"""
    __tablename__ = 'analyses'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, index=True)
    analysis_type = Column(String)  # market, economic, fed, combined
    content = Column(JSON)
    market_impact = Column(JSON, nullable=True)
    confidence = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

class Alert(Base):
    """System alerts and notifications"""
    __tablename__ = 'alerts'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, index=True)
    alert_type = Column(String)  # data_missing, anomaly, threshold
    severity = Column(String)  # high, medium, low
    message = Column(String)
    resolved = Column(Integer, default=0)  # 0=unresolved, 1=resolved
    created_at = Column(DateTime, default=datetime.utcnow)