import asyncio
import logging
from datetime import datetime, time, timedelta, timezone
import pytz
from typing import Dict, Any
import os
from pathlib import Path

# Internal imports
from config.settings import (API_KEYS, DATABASE_CONFIG, EMAIL_CONFIG,
                           COLLECTION_CONFIG, LOGGING_CONFIG)
from collectors.market import MarketDataCollector
from collectors.economic import EconomicDataCollector
from collectors.fed_speech import FedSpeechCollector
from collectors.bond import BondCollector
from analyzers.market_analysis import MarketAnalyzer
from analyzers.fed_analysis import FedAnalyzer
from analyzers.release_analysis import ReleaseAnalyzer
from database.manager import DatabaseManager
from notifiers.email_service import EmailNotifier
from notifiers.market_newsletter import MarketNewsletterComposer
from utils.logger import setup_logger

class MarketMonitor:
    """Main application class coordinating all components"""
    
    def __init__(self):
        # Set up logging
        self.logger = setup_logger(
            'market_monitor',
            log_file='logs/market_monitor.log',
            level=logging.INFO
        )
        
        # Initialize components
        self.db = DatabaseManager(DATABASE_CONFIG['sqlite']['path'])
        self.newsletter_composer = MarketNewsletterComposer(API_KEYS)
        self.email_notifier = EmailNotifier(EMAIL_CONFIG, self.newsletter_composer)
            
        # Initialize collectors
        self.market_collector = MarketDataCollector(API_KEYS)
        self.economic_collector = EconomicDataCollector(API_KEYS)
        self.fed_collector = FedSpeechCollector(API_KEYS)
        self.bond_collector = BondCollector(API_KEYS)
        
        # Initialize analyzers
        self.market_analyzer = MarketAnalyzer(API_KEYS)
        self.fed_analyzer = FedAnalyzer(API_KEYS)
        self.release_analyzer = ReleaseAnalyzer(API_KEYS)
        
        # Track last update times
        self.last_updates = {}

        self.daily_events = {
            'market_events': [],
            'economic_releases': [],
            'fed_communications': [],
            'system_events': []  # For internal logging only
        }

    async def run(self):
        """Main application loop"""
        self.logger.info("Starting Market Monitor")
        
        while True:
            try:
                await self.check_market_hours()
                await self.check_economic_releases()
                await self.check_fed_communications()
                await self.send_daily_update()
                
                # Sleep for main loop interval
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in main loop: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying

    async def check_market_hours(self):
        """Check if markets are open and collect data if needed"""
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.now(ny_tz)
        
        # Check if it's a weekday and market hours (9:30 AM - 4:00 PM ET)
        if (now.weekday() < 5 and
            time(9, 30) <= now.time() <= time(16, 0)):
            
            # Check if we need to update market data
            if self._should_update('market_data'):
                await self.collect_market_data()

    async def collect_market_data(self):
        """Collect and analyze market data"""
        try:
            # Collect data
            market_data = await self.market_collector.collect()
            bond_data = await self.bond_collector.collect()
            
            # Store in database
            if market_data.success:
                self.db.store_market_data(market_data.data)
            if bond_data.success:
                self.db.store_bond_data(bond_data.data)
            
            # Analyze market conditions
            analysis = await self.market_analyzer.analyze_market_conditions(
                market_data.data if market_data.success else {},
                bond_data.data if bond_data.success else {}
            )
            
            # Store analysis
            self.db.store_analysis({
                'timestamp': datetime.now(timezone.utc),
                'type': 'market',
                'content': analysis
            })
            
            # Update last collection time
            self.last_updates['market_data'] = datetime.now(timezone.utc)
            
        except Exception as e:
            self.logger.error(f"Error collecting market data: {str(e)}")

    async def check_economic_releases(self):
        """Check for and process new economic releases"""
        try:
            if self._should_update('economic_data'):
                releases = await self.economic_collector.collect()
                
                if releases.success and releases.data:
                    for release in releases.data:
                        # Store release
                        self.db.store_economic_release(release)
                        
                        # Analyze release
                        analysis = await self.release_analyzer.analyze_release(
                            release,
                            await self._get_historical_context(),
                            await self._get_market_context()
                        )
                        
                        if release.get('importance') == 'high':
                            self.daily_events['economic_releases'].append({
                                'timestamp': datetime.now(timezone.utc),
                                'indicator': release['indicator'],
                                'value': release['value'],
                                'expected': release.get('expected'),
                                'previous': release['previous'],
                                'analysis': analysis
                            })
                
                self.last_updates['economic_data'] = datetime.now(timezone.utc)
                
        except Exception as e:
            self.logger.error(f"Error processing economic releases: {str(e)}")

    async def check_fed_communications(self):
        """Check for and process new Fed communications"""
        try:
            if self._should_update('fed_speeches'):
                speeches = await self.fed_collector.collect()
                
                if speeches.success and speeches.data:
                    for speech in speeches.data:
                        # Store speech
                        self.db.store_fed_speech(speech)
                        
                        # Analyze speech
                        analysis = await self.fed_analyzer.analyze_speech(
                            speech['content'],
                            speech['speaker'],
                            speech['title'],
                            speech['date']
                        )
                        
                        # Send alert for important speeches
                        if speech.get('importance', 'low') in ['high', 'medium']:
                            self.daily_events['fed_communications'].append({
                                'timestamp': datetime.now(timezone.utc),
                                'speaker': speech['speaker'],
                                'title': speech['title'],
                                'analysis': analysis
                            })
                
                self.last_updates['fed_speeches'] = datetime.now(timezone.utc)
                
        except Exception as e:
            self.logger.error(f"Error processing Fed communications: {str(e)}")

    async def send_daily_update(self):
        """Send daily market update email"""
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.now(ny_tz)
        
        # Send update at 4:30 PM ET
        if (now.weekday() < 5 and
            now.time() >= time(16, 30) and
            not self._sent_daily_update_today()):
            
            try:
                # Gather data
                market_data = await self._get_market_context()
                economic_data = await self._get_economic_context()
                fed_analysis = await self._get_fed_context()

                economic_data['significant_releases'] = self.daily_events['economic_releases']
                fed_analysis['significant_communications'] = self.daily_events['fed_communications']
                                
                # Send update
                self.logger.info("Sending daily update")
                await self.email_notifier.send_daily_update(
                    market_data,
                    economic_data,
                    fed_analysis
                )

                # Clear daily events after sending
                self.daily_events = {
                    'market_events': [],
                    'economic_releases': [],
                    'fed_communications': [],
                    'system_events': []
                }
                
                self.last_updates['daily_update'] = datetime.now(timezone.utc)
                
            except Exception as e:
                self.logger.error(f"Error sending daily update: {str(e)}")

    def _should_update(self, data_type: str) -> bool:
        """Check if we should update specific data type"""
        if data_type not in self.last_updates:
            return True
            
        elapsed = datetime.now(timezone.utc) - self.last_updates[data_type]
        return elapsed.total_seconds() >= COLLECTION_CONFIG['update_frequency'][data_type]

    def _sent_daily_update_today(self) -> bool:
        """Check if we've already sent today's update"""
        if 'daily_update' not in self.last_updates:
            return False
            
        last_update = self.last_updates['daily_update']
        return last_update.date() == datetime.now(timezone.utc).date()

    async def _get_market_context(self) -> Dict[str, Any]:
        """Get current market context"""
        try:
            # Get recent market data
            start_date = datetime.now(timezone.utc) - timedelta(days=1)
            end_date = datetime.now(timezone.utc)
            
            # Get market data for all symbols
            market_data = self.db.get_market_data(
                start_date=start_date,
                end_date=end_date
            )
            
            # Get recent bond data
            bond_data = self.db.get_bond_data(
                start_date=start_date,
                end_date=end_date
            )
            
            # Get latest market regime
            latest_regime = self.db.get_latest_market_regime()
            
            # Structure the data to match template expectations
            return {
                'data': {
                    **market_data,  # This will include all asset classes
                    'bonds': bond_data
                },
                'regime': {
                    'risk_environment': latest_regime.risk_environment if latest_regime else 'neutral',
                    'volatility_regime': latest_regime.volatility_regime if latest_regime else 'normal',
                    'liquidity_conditions': latest_regime.liquidity_conditions if latest_regime else 'normal',
                    'correlation_regime': latest_regime.correlation_regime if latest_regime else 'normal'
                },
                'trends': latest_regime.dominant_factors if latest_regime else [],
                'timestamp': datetime.now(timezone.utc)
            }
                
        except Exception as e:
            self.logger.error(f"Error getting market context: {str(e)}")
            return {
                'data': {},
                'regime': {
                    'risk_environment': 'unknown',
                    'volatility_regime': 'unknown',
                    'liquidity_conditions': 'unknown',
                    'correlation_regime': 'unknown'
                },
                'trends': [],
                'timestamp': datetime.now(timezone.utc)
            }

    async def _get_economic_context(self) -> Dict[str, Any]:
        """Get economic data context"""
        try:
            # Get recent economic releases
            releases = self.db.get_latest_economic_data(
                lookback_days=7
            )
            
            # Group releases by importance
            grouped_releases = {
                'high': [],
                'medium': [],
                'low': []
            }
            
            for release in releases:
                importance = release.importance or 'low'
                grouped_releases[importance].append(release)
            
            # Get latest analysis for each major indicator
            analyses = {}
            major_indicators = ['GDP', 'CPI', 'NFP', 'RETAIL_SALES']
            
            for indicator in major_indicators:
                latest_analysis = self.db.get_latest_analysis(
                    analysis_type='economic',
                    indicator=indicator
                )
                if latest_analysis:
                    analyses[indicator] = latest_analysis
            
            return {
                'releases': grouped_releases,
                'analyses': analyses,
                'timestamp': datetime.now(timezone.utc)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting economic context: {str(e)}")
            return {}

    async def _get_fed_context(self) -> Dict[str, Any]:
        """Get Fed analysis context"""
        try:
            # Get recent Fed communications
            recent_speeches = self.db.get_recent_fed_speeches(days=7)
            
            # Get latest FOMC meeting info
            fomc_info = self.db.get_latest_fomc_meeting()
            
            # Get latest Fed analyses
            analyses = self.db.get_latest_analysis(
                analysis_type='fed',
                limit=5
            )
            
            # Calculate aggregate policy bias
            policy_signals = []
            for speech in recent_speeches:
                if speech.analysis:
                    policy_signals.append({
                        'speaker': speech.speaker,
                        'hawkish_score': speech.analysis.get('hawkish_score', 0),
                        'date': speech.date
                    })
            
            return {
                'recent_communications': recent_speeches,
                'fomc_info': fomc_info,
                'analyses': analyses,
                'policy_signals': policy_signals,
                'timestamp': datetime.now(timezone.utc)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting Fed context: {str(e)}")
            return {}

    async def _get_historical_context(self) -> Dict[str, Any]:
        """Get historical data context"""
        try:
            # Get historical market data
            market_history = self.db.get_market_data(
                start_date=datetime.now(timezone.utc) - timedelta(days=252),  # 1 trading year
                end_date=datetime.now(timezone.utc)
            )
            
            # Get historical economic releases
            economic_history = self.db.get_economic_data(
                lookback_days=365  # 1 calendar year
            )
            
            # Calculate historical volatility
            volatility = {}
            for symbol, data in market_history.items():
                if len(data) > 20:  # Need at least 20 days for vol calc
                    returns = pd.Series(data['close']).pct_change()
                    volatility[symbol] = returns.std() * np.sqrt(252)  # Annualized
            
            # Get historical regimes
            regimes = self.db.get_market_regimes(
                start_date=datetime.now(timezone.utc) - timedelta(days=365)
            )
            
            return {
                'market_history': market_history,
                'economic_history': economic_history,
                'historical_volatility': volatility,
                'market_regimes': regimes,
                'timestamp': datetime.now(timezone.utc)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting historical context: {str(e)}")
            return {}

if __name__ == "__main__":
    monitor = MarketMonitor()
    asyncio.run(monitor.run())