from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import URL
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Union
import logging
from datetime import datetime, timedelta

from .models import (Base, EconomicRelease, MarketData, BondData,
                    FedSpeech, MarketRegime, Analysis, Alert)

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database operations"""

    def __init__(self, db_path: str):
        # Convert file path to SQLite URL
        connection_string = f"sqlite:///{db_path}"
        self.engine = create_engine(connection_string)
        self.SessionLocal = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    @contextmanager
    def get_session(self) -> Session:
        """Get database session with context management"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def store_economic_release(self, data: Dict[str, Any]) -> None:
        """Store economic release data"""
        with self.get_session() as session:
            release = EconomicRelease(
                indicator=data['indicator'],
                value=data['value'],
                previous=data['previous'],
                expected=data.get('expected'),
                timestamp=data['timestamp'],
                source=data['source'],
                frequency=data.get('frequency'),
                importance=data.get('importance'),
                analysis=data.get('analysis')
            )
            session.add(release)

    def store_market_data(self, data: Dict[str, Any]) -> None:
        """Store market price data"""
        with self.get_session() as session:
            market_data = MarketData(
                symbol=data['symbol'],
                asset_class=data['asset_class'],
                price=data['price'],
                volume=data.get('volume'),
                open_price=data.get('open'),
                high_price=data.get('high'),
                low_price=data.get('low'),
                close_price=data.get('close'),
                timestamp=data['timestamp'],
                source=data['source'],
                market_metadata=data.get('metadata')
            )
            session.add(market_data)

    def store_bond_data(self, data: Dict[str, Any]) -> None:
        """Store bond market data"""
        with self.get_session() as session:
            bond_data = BondData(
                symbol=data['symbol'],
                yield_value=data['yield'],
                price=data.get('price'),
                duration=data.get('duration'),
                maturity_date=data['maturity_date'],
                timestamp=data['timestamp'],
                coupon=data.get('coupon'),
                market_metadata=data.get('metadata')
            )
            session.add(bond_data)

    def store_fed_speech(self, data: Dict[str, Any]) -> None:
        """Store Fed communication"""
        with self.get_session() as session:
            speech = FedSpeech(
                speaker=data['speaker'],
                role=data['role'],
                title=data['title'],
                speech_type=data['speech_type'],
                content=data['content'],
                date=data['date'],
                url=data['url'],
                analysis=data.get('analysis')
            )
            session.add(speech)

    def get_latest_economic_data(self, 
                               indicator: str,
                               lookback_days: int = 30) -> List[EconomicRelease]:
        """Get latest economic data for indicator"""
        with self.get_session() as session:
            cutoff = datetime.now() - timedelta(days=lookback_days)
            return session.query(EconomicRelease)\
                .filter(EconomicRelease.indicator == indicator,
                       EconomicRelease.timestamp >= cutoff)\
                .order_by(desc(EconomicRelease.timestamp))\
                .all()

    def get_market_data(self,
                       symbol: str,
                       start_date: datetime,
                       end_date: datetime) -> List[MarketData]:
        """Get market data for symbol within date range"""
        with self.get_session() as session:
            return session.query(MarketData)\
                .filter(MarketData.symbol == symbol,
                       MarketData.timestamp.between(start_date, end_date))\
                .order_by(MarketData.timestamp)\
                .all()

    def get_recent_fed_speeches(self, days: int = 7) -> List[FedSpeech]:
        """Get recent Fed communications"""
        with self.get_session() as session:
            cutoff = datetime.now() - timedelta(days=days)
            return session.query(FedSpeech)\
                .filter(FedSpeech.date >= cutoff)\
                .order_by(desc(FedSpeech.date))\
                .all()

    def store_market_regime(self, data: Dict[str, Any]) -> None:
        """Store market regime classification"""
        with self.get_session() as session:
            regime = MarketRegime(
                timestamp=data['timestamp'],
                risk_environment=data['risk_environment'],
                volatility_regime=data['volatility_regime'],
                liquidity_conditions=data['liquidity_conditions'],
                correlation_regime=data['correlation_regime'],
                dominant_factors=data['dominant_factors'],
                analysis=data.get('analysis')
            )
            session.add(regime)

    def store_analysis(self, data: Dict[str, Any]) -> None:
        """Store analysis results"""
        with self.get_session() as session:
            analysis = Analysis(
                timestamp=data['timestamp'],
                analysis_type=data['type'],
                content=data['content'],
                market_impact=data.get('market_impact'),
                confidence=data.get('confidence', 0.0)
            )
            session.add(analysis)

    def create_alert(self, alert_type: str, severity: str, message: str) -> None:
        """Create system alert"""
        with self.get_session() as session:
            alert = Alert(
                timestamp=datetime.utcnow(),
                alert_type=alert_type,
                severity=severity,
                message=message
            )
            session.add(alert)

    def get_unresolved_alerts(self) -> List[Alert]:
        """Get all unresolved alerts"""
        with self.get_session() as session:
            return session.query(Alert)\
                .filter(Alert.resolved == 0)\
                .order_by(desc(Alert.timestamp))\
                .all()

    def cleanup_old_data(self, days: int = 365) -> None:
        """Clean up old data from database"""
        cutoff = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            for model in [MarketData, EconomicRelease, Analysis, Alert]:
                session.query(model)\
                    .filter(model.timestamp < cutoff)\
                    .delete()
                
    def get_latest_market_regime(self) -> Optional[MarketRegime]:
        """Get most recent market regime classification"""
        with self.get_session() as session:
            return session.query(MarketRegime)\
                .order_by(desc(MarketRegime.timestamp))\
                .first()

    def get_latest_fomc_meeting(self) -> Optional[FedSpeech]:
        """Get most recent FOMC meeting details"""
        with self.get_session() as session:
            return session.query(FedSpeech)\
                .filter(FedSpeech.speech_type == 'FOMC_STATEMENT')\
                .order_by(desc(FedSpeech.date))\
                .first()

    def get_latest_analysis(self,
                          analysis_type: str,
                          indicator: Optional[str] = None,
                          limit: int = 1) -> Union[Optional[Analysis], List[Analysis]]:
        """
        Get latest analysis results
        
        Args:
            analysis_type: Type of analysis ('market', 'economic', 'fed', etc.)
            indicator: Optional specific indicator to filter by
            limit: Number of results to return (default 1)
        """
        with self.get_session() as session:
            query = session.query(Analysis)\
                .filter(Analysis.analysis_type == analysis_type)
                
            if indicator:
                # If indicator specified, look in content JSON
                query = query.filter(
                    Analysis.content.contains({'indicator': indicator})
                )
                
            query = query.order_by(desc(Analysis.timestamp))
            
            if limit == 1:
                return query.first()
            return query.limit(limit).all()

    def get_economic_data(self, lookback_days: int = 365) -> List[EconomicRelease]:
        """Get all economic releases within lookback period"""
        with self.get_session() as session:
            cutoff = datetime.now() - timedelta(days=lookback_days)
            return session.query(EconomicRelease)\
                .filter(EconomicRelease.timestamp >= cutoff)\
                .order_by(desc(EconomicRelease.timestamp))\
                .all()

    def get_market_regimes(self, 
                         start_date: datetime,
                         end_date: Optional[datetime] = None) -> List[MarketRegime]:
        """Get market regime history for specified period"""
        with self.get_session() as session:
            query = session.query(MarketRegime)\
                .filter(MarketRegime.timestamp >= start_date)
                
            if end_date:
                query = query.filter(MarketRegime.timestamp <= end_date)
                
            return query.order_by(MarketRegime.timestamp).all()

    def get_bond_data(self,
                     start_date: datetime,
                     end_date: datetime,
                     symbols: Optional[List[str]] = None) -> List[BondData]:
        """
        Get bond market data for specified period
        
        Args:
            start_date: Start of period
            end_date: End of period
            symbols: Optional list of specific bond symbols to retrieve
        """
        with self.get_session() as session:
            query = session.query(BondData)\
                .filter(BondData.timestamp.between(start_date, end_date))
                
            if symbols:
                query = query.filter(BondData.symbol.in_(symbols))
                
            return query.order_by(BondData.timestamp).all()