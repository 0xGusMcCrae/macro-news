import yfinance as yf
import aiohttp
import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
import logging
from .base import BaseCollector, CollectorResponse
from .bond import BondCollector

logger = logging.getLogger(__name__)

class MarketDataCollector(BaseCollector):
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.bond_collector = BondCollector(config)
        
        self.market_symbols = {
            'indices': {
                'SPX': '^GSPC',    # S&P 500
                'NDX': '^IXIC',    # Nasdaq
                'DJI': '^DJI',     # Dow Jones
                'RUT': '^RUT',     # Russell 2000
                'VIX': '^VIX',     # Volatility Index
            },
            'fx': {
                'DXY': 'DX-Y.NYB', # Dollar Index
                'EURUSD': 'EUR=X', # Euro
                'USDJPY': 'JPY=X', # Japanese Yen
                'GBPUSD': 'GBP=X', # British Pound
            },
            'commodities': {
                'GOLD': 'GC=F',    # Gold Futures
                'OIL': 'CL=F',     # Crude Oil Futures
                'COPPER': 'HG=F',   # Copper Futures
                'NATGAS': 'NG=F',   # Natural Gas Futures
            }
        }

    async def collect(self) -> CollectorResponse:
        """Collect market data across all asset classes"""
        try:
            # Collect market data for each asset class
            market_data = {}
            
            for asset_class, symbols in self.market_symbols.items():
                market_data[asset_class] = {}
                for symbol_name, yf_symbol in symbols.items():
                    try:
                        data = await self._get_market_data(yf_symbol)
                        if data:
                            market_data[asset_class][symbol_name] = data
                    except Exception as e:
                        logger.error(f"Error collecting data for {symbol_name}: {str(e)}")
            
            # Collect bond data
            bond_response = await self.bond_collector.collect()
            if bond_response.success:
                market_data['bonds'] = bond_response.data
            
            return CollectorResponse(
                success=True,
                data=market_data,
                metadata={
                    'timestamp': datetime.now(),
                    'coverage': list(market_data.keys())
                }
            )
            
        except Exception as e:
            logger.error(f"Error in market data collection: {str(e)}")
            return CollectorResponse(
                success=False,
                error=str(e)
            )

    async def _get_market_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get market data for a specific symbol"""
        try:
            ticker = yf.Ticker(symbol)
            
            # Get historical data for 2 days to calculate changes
            hist = ticker.history(period='2d')
            if hist.empty:
                logger.warning(f"No data available for {symbol}")
                return None

            current_price = float(hist['Close'].iloc[-1])
            previous_close = float(hist['Close'].iloc[-2])
            
            return {
                'symbol': symbol,
                'price': current_price,
                'change': ((current_price / previous_close) - 1) * 100,
                'volume': float(hist['Volume'].iloc[-1]),
                'high': float(hist['High'].iloc[-1]),
                'low': float(hist['Low'].iloc[-1]),
                'open': float(hist['Open'].iloc[-1]),
                'previous_close': previous_close,
                'timestamp': hist.index[-1].isoformat(),
                'additional_info': self._extract_relevant_info(ticker.info)
            }

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def _extract_relevant_info(self, info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant information from ticker info"""
        relevant_fields = {
            'marketCap',
            'fiftyTwoWeekHigh',
            'fiftyTwoWeekLow',
            'averageVolume',
            'beta',
            'trailingPE'
        }
        
        return {k: v for k, v in info.items() 
                if k in relevant_fields and v is not None}

    async def get_cross_asset_correlations(self) -> pd.DataFrame:
        """Calculate correlations between different assets"""
        try:
            correlation_data = await self._get_correlation_data(days=30)
            if correlation_data.empty:
                return pd.DataFrame()
                
            return correlation_data.corr()
            
        except Exception as e:
            logger.error(f"Error calculating correlations: {str(e)}")
            return pd.DataFrame()

    async def _get_correlation_data(self, days: int = 30) -> pd.DataFrame:
        """Get historical data for correlation calculation"""
        try:
            data = {}
            
            # Collect historical data for each asset
            for asset_class, symbols in self.market_symbols.items():
                for symbol_name, yf_symbol in symbols.items():
                    ticker = yf.Ticker(yf_symbol)
                    hist = ticker.history(period=f'{days}d')['Close']
                    if not hist.empty:
                        data[f"{asset_class}_{symbol_name}"] = hist

            # Add bond data if available
            bond_response = await self.bond_collector.collect()
            if bond_response.success:
                for bond_type, value in bond_response.data['rates'].items():
                    if isinstance(value, dict) and 'history' in value:
                        data[f"bonds_{bond_type}"] = value['history']

            return pd.DataFrame(data)
            
        except Exception as e:
            logger.error(f"Error getting correlation data: {str(e)}")
            return pd.DataFrame()

    async def validate_data(self, data: Any) -> bool:
        """Validate collected market data"""
        if not isinstance(data, dict):
            return False
            
        # Check if we have data for each asset class
        required_asset_classes = {'indices', 'fx', 'commodities'}
        if not all(asset_class in data for asset_class in required_asset_classes):
            return False
            
        # Check if key instruments have required fields
        required_fields = {'price', 'volume', 'timestamp'}
        
        for asset_class, instruments in data.items():
            for instrument, instrument_data in instruments.items():
                if not instrument_data:  # Skip if None
                    continue
                if not all(field in instrument_data for field in required_fields):
                    return False
                    
        return True

    def get_market_symbols(self) -> Dict[str, Dict[str, str]]:
        """Get dictionary of market symbols"""
        return self.market_symbols