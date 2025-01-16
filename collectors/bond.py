from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import logging
import aiohttp
import yfinance as yf
import pandas as pd
from .base import BaseCollector, CollectorResponse

logger = logging.getLogger(__name__)

class BondCollector(BaseCollector):
    """Dedicated collector for fixed income markets"""
    
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        
        self.symbols = {
            'treasuries': {
                'US2Y': '^IRX',     # 2-year Treasury
                'US5Y': '^FVX',     # 5-year Treasury
                'US10Y': '^TNX',    # 10-year Treasury
                'US30Y': '^TYX',    # 30-year Treasury
            },
            # 'intl_govt': {  # These symbols are incorrect
            #     'DE10Y': '^GDBR10',  # German 10Y (Bund)
            #     'GB10Y': '^GUKG10',  # UK 10Y (Gilt)
            #     'JP10Y': '^GJGB10',  # Japanese 10Y (JGB)
            #     'IT10Y': 'ITGBR10Y.MX',  # Italian 10Y (BTP)
            # },
            'corporates': {
                'IG_CORPS': 'LQD',      # Investment Grade ETF
                'HY_CORPS': 'HYG',      # High Yield ETF
                'EM_BONDS': 'EMB',      # Emerging Markets
                'TIPS': 'TIP',          # TIPS ETF
            },
            'indicators': {
                'MOVE': '^MOVE',        # Bond Volatility
                'TLT': 'TLT',           # Long Treasury ETF
                'AGG': 'AGG',           # Aggregate Bond ETF
            }
        }
        
        self.spread_definitions = {
            '2s10s': ('US2Y', 'US10Y'),
            '5s30s': ('US5Y', 'US30Y'),
            'bund_spread': ('DE10Y', 'US10Y'),
            'gilt_spread': ('GB10Y', 'US10Y'),
            'btp_bund': ('IT10Y', 'DE10Y'),
        }

    async def collect(self) -> CollectorResponse:
        """Collect comprehensive bond market data"""
        try:
            logger.info("Starting bond data collection")
            
            # Collect rates
            logger.info("Collecting treasury rates")
            rates = await self._collect_rates()
            logger.info(f"Collected rates: {rates}")
            
            # Calculate spreads
            logger.info("Calculating spreads")
            spreads = await self._collect_spreads()
            logger.info(f"Calculated spreads: {spreads}")
            
            # Collect credit markets
            logger.info("Collecting credit market data")
            credit = await self._collect_credit_markets()
            logger.info(f"Collected credit data: {credit}")
            
            # Collect metrics
            logger.info("Collecting market metrics")
            metrics = await self._collect_market_metrics()
            logger.info(f"Collected metrics: {metrics}")

            data = {
                'rates': rates,
                'spreads': spreads,
                'credit': credit,
                'metrics': metrics
            }
            
            logger.info("Bond data collection completed successfully")
            return CollectorResponse(
                success=True,
                data=data,
                metadata={
                    'timestamp': datetime.now(),
                    'coverage': list(data.keys())
                }
            )

        except Exception as e:
            logger.error(f"Error in bond data collection: {str(e)}", exc_info=True)
            return CollectorResponse(
                success=False,
                error=str(e)
            )

    async def _collect_rates(self) -> Dict[str, float]:
        """Collect government bond rates"""
        rates = {}
        
        for market in ['treasuries']:
            for name, symbol in self.symbols[market].items():
                try:
                    data = await self._get_market_data(symbol)
                    if data:
                        rates[name] = data
                except Exception as e:
                    logger.error(f"Error collecting {name}: {str(e)}")
                    continue
        
        return rates

    async def _collect_spreads(self) -> Dict[str, float]:
        """Calculate key spreads"""
        spreads = {}
        rates = await self._collect_rates()
        
        for spread_name, (base, quote) in self.spread_definitions.items():
            if base in rates and quote in rates:
                spreads[spread_name] = rates[quote]['price'] - rates[base]['price']
                
        return spreads

    async def analyze_bond_market_conditions(self) -> Dict[str, str]:
        """Analyze current bond market conditions"""
        try:
            data = await self.collect()
            if not data.success:
                raise ValueError(data.error)

            spreads = data.data['spreads']
            metrics = data.data['metrics']
            
            return {
                'yield_curve': self._analyze_curve_shape(spreads),
                'credit_conditions': self._analyze_credit_conditions(data.data['credit']),
                'volatility_regime': self._analyze_volatility(metrics.get('move_index')),
                'liquidity_conditions': self._analyze_liquidity(metrics),
                'market_stress': self._calculate_stress_index(data.data)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing bond markets: {str(e)}")
            return {}

    def _analyze_curve_shape(self, spreads: Dict[str, float]) -> str:
        """Analyze yield curve shape"""
        twos_tens = spreads.get('2s10s', 0)
        fives_thirties = spreads.get('5s30s', 0)
        
        if twos_tens < -0.1:  # 10bps inversion threshold
            return 'inverted'
        elif twos_tens < 0.1:
            return 'flat'
        elif twos_tens < 0.5:
            return 'moderately_steep'
        else:
            return 'steep'

    async def get_real_yields(self) -> Dict[str, float]:
        """Calculate real yields using TIPS"""
        try:
            tips_data = await self._get_market_data(self.symbols['corporates']['TIPS'])
            treasury_data = await self._get_market_data(self.symbols['treasuries']['US10Y'])
            
            if tips_data and treasury_data:
                nominal_yield = treasury_data['price']
                tips_yield = tips_data['yield']
                breakeven = nominal_yield - tips_yield
                
                return {
                    'real_yield': tips_yield,
                    'breakeven_inflation': breakeven,
                    'nominal_yield': nominal_yield
                }
                
        except Exception as e:
            logger.error(f"Error calculating real yields: {str(e)}")
            return {}
    
    async def validate_data(self, data: Any) -> bool:
        """Validate collected bond market data"""
        if not isinstance(data, dict):
            return False
            
        required_sections = {'rates', 'spreads', 'credit', 'metrics'}
        if not all(section in data for section in required_sections):
            return False
            
        # Validate metrics (changed from previous validation)
        if not isinstance(data['metrics'], dict):
            return False
        
        required_metrics = {'volatility', 'liquidity', 'stress_index'}
        if not all(metric in data['metrics'] for metric in required_metrics):
            return False
            
        return True

    async def _collect_credit_markets(self) -> Dict[str, float]:
        """Collect credit market data"""
        credit_data = {
            'ig_spread': 0.0,  # Default values
            'hy_spread': 0.0
        }
        
        try:
            # Get corporate bond ETF data
            for name, symbol in self.symbols['corporates'].items():
                data = await self._get_market_data(symbol)
                if data:
                    credit_data[name.lower()] = data['price']  # Store just the price
            
            # Calculate spreads if we have treasury data
            treasury_data = await self._get_market_data(self.symbols['treasuries']['US10Y'])
            if treasury_data:
                treasury_yield = treasury_data['price']
                if 'ig_corps' in credit_data:
                    credit_data['ig_spread'] = credit_data['ig_corps'] - treasury_yield
                if 'hy_corps' in credit_data:
                    credit_data['hy_spread'] = credit_data['hy_corps'] - treasury_yield
                
        except Exception as e:
            logger.error(f"Error collecting credit market data: {str(e)}")
            
        return credit_data

    async def _collect_market_metrics(self) -> Dict[str, float]:
        """Collect bond market metrics"""
        metrics = {
            'volatility': 0.0,
            'liquidity': 1.0,  # Default value
            'stress_index': 0.0
        }
        
        try:
            # Get MOVE index for volatility
            move_data = await self._get_market_data(self.symbols['indicators']['MOVE'])
            if move_data:
                metrics['volatility'] = move_data['price']
            
            # Calculate stress index
            if metrics['volatility'] > 0:
                metrics['stress_index'] = metrics['volatility'] / 100  # Simple normalization
                
        except Exception as e:
            logger.error(f"Error collecting market metrics: {str(e)}")
            
        return metrics

    async def _get_market_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get market data for a specific symbol"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period='5d')
            
            if hist.empty or len(hist) < 2:  # Need at least 2 days of data
                logger.warning(f"Insufficient data available for {symbol}")
                return None

            try:
                current_price = float(hist['Close'].iloc[-1])
                previous_close = float(hist['Close'].iloc[-2])
                volume = float(hist['Volume'].iloc[-1]) if 'Volume' in hist else 0.0
                
                return {
                    'symbol': symbol,
                    'price': current_price,
                    'change': ((current_price / previous_close) - 1) * 100,
                    'volume': volume,
                    'high': float(hist['High'].iloc[-1]),
                    'low': float(hist['Low'].iloc[-1]),
                    'open': float(hist['Open'].iloc[-1]),
                    'previous_close': previous_close,
                    'timestamp': hist.index[-1].isoformat()
                }
            except (IndexError, KeyError) as e:
                logger.error(f"Error processing data for {symbol}: {str(e)}")
                return None

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None