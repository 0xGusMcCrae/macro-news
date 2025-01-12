from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
import pandas as pd
import numpy as np
from dataclasses import dataclass
from scipy import stats
from scipy.stats import zscore, percentileofscore
from sklearn.linear_model import LinearRegression
import logging

logger = logging.getLogger(__name__)

@dataclass
class MarketRegime:
    """Current market regime classification"""
    risk_environment: str  # 'risk_on', 'risk_off', 'neutral'
    volatility_regime: str  # 'low', 'elevated', 'high'
    liquidity_conditions: str  # 'tight', 'ample', 'stressed'
    correlation_regime: str  # 'normal', 'risk_off_correlation', 'choppy'
    dominant_factors: List[str]  # e.g., ['inflation', 'growth', 'policy']

@dataclass
class MarketAnalysis:
    """Comprehensive market analysis results"""
    regime: MarketRegime
    trends: Dict[str, str]
    correlations: pd.DataFrame
    risk_metrics: Dict[str, float]
    anomalies: List[Dict[str, Any]]
    leading_indicators: Dict[str, float]

class MarketAnalyzer:
    """Analyzes market conditions across asset classes"""

    def __init__(self, config: Dict[str, str], lookback_days: int = 252):
        self.config = config
        self.lookback_days = lookback_days
        self.historical_data = {}
        self.regime_thresholds = {
            'vix': {'low': 15, 'elevated': 25, 'high': 35},
            'correlation': {'normal': 0.4, 'high': 0.7},
            'liquidity': {'tight': -0.5, 'stressed': -1.0}
        }

    async def analyze_market_conditions(self, 
                                     current_data: Dict[str, Any],
                                     historical_data: Dict[str, Any]) -> MarketAnalysis:
        """Perform comprehensive market analysis"""
        try:
            # Update historical data
            self._update_historical_data(historical_data)

            # Analyze current regime
            regime = self._determine_market_regime(current_data)

            # Calculate various metrics
            trends = self._analyze_trends(current_data)
            correlations = self._calculate_correlations()
            risk_metrics = self._calculate_risk_metrics(current_data)
            anomalies = self._detect_anomalies(current_data)
            leading_indicators = self._analyze_leading_indicators(current_data)

            return MarketAnalysis(
                regime=regime,
                trends=trends,
                correlations=correlations,
                risk_metrics=risk_metrics,
                anomalies=anomalies,
                leading_indicators=leading_indicators
            )

        except Exception as e:
            logger.error(f"Error in market analysis: {str(e)}")
            raise

    def _determine_market_regime(self, data: Dict[str, Any]) -> MarketRegime:
        """Determine current market regime"""
        # Analyze risk environment
        risk_env = self._analyze_risk_environment(data)
        
        # Analyze volatility regime
        vol_regime = self._analyze_volatility_regime(data)
        
        # Analyze liquidity conditions
        liquidity = self._analyze_liquidity_conditions(data)
        
        # Analyze correlation regime
        corr_regime = self._analyze_correlation_regime()
        
        # Determine dominant factors
        factors = self._identify_dominant_factors(data)
        
        return MarketRegime(
            risk_environment=risk_env,
            volatility_regime=vol_regime,
            liquidity_conditions=liquidity,
            correlation_regime=corr_regime,
            dominant_factors=factors
        )

    def _analyze_trends(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Analyze trends across asset classes"""
        trends = {}
        
        for asset_class, assets in data.items():
            for asset, price_data in assets.items():
                trend = self._calculate_trend(price_data)
                trends[f"{asset_class}_{asset}"] = trend
                
        return trends

    def _calculate_trend(self, price_data: Dict[str, float]) -> str:
        """Calculate trend for a single asset"""
        # Get historical prices
        hist_prices = self.historical_data.get(price_data['symbol'], [])
        if len(hist_prices) < 20:
            return 'insufficient_data'
            
        # Calculate moving averages
        ma20 = np.mean(hist_prices[-20:])
        ma50 = np.mean(hist_prices[-50:]) if len(hist_prices) >= 50 else ma20
        current_price = price_data['price']
        
        # Determine trend strength
        if current_price > ma20 > ma50:
            return 'strong_uptrend'
        elif current_price > ma20:
            return 'uptrend'
        elif current_price < ma20 < ma50:
            return 'strong_downtrend'
        elif current_price < ma20:
            return 'downtrend'
        else:
            return 'neutral'

    def _calculate_risk_metrics(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Calculate various risk metrics"""
        metrics = {
            'cross_asset_correlation': self._calculate_average_correlation(),
            'volatility_percentile': self._calculate_volatility_percentile(data),
            'risk_dispersion': self._calculate_risk_dispersion(data),
            'tail_risk': self._calculate_tail_risk(data)
        }
        
        # Add market-specific metrics
        metrics.update(self._calculate_market_specific_metrics(data))
        
        return metrics

    def _detect_anomalies(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect market anomalies"""
        anomalies = []
        
        # Price anomalies
        price_anomalies = self._detect_price_anomalies(data)
        anomalies.extend(price_anomalies)
        
        # Correlation anomalies
        corr_anomalies = self._detect_correlation_anomalies()
        anomalies.extend(corr_anomalies)
        
        # Volume anomalies
        volume_anomalies = self._detect_volume_anomalies(data)
        anomalies.extend(volume_anomalies)
        
        return anomalies

    def _analyze_leading_indicators(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Analyze leading market indicators"""
        indicators = {
            'yield_curve_slope': self._calculate_yield_curve_slope(data),
            'credit_spreads': self._analyze_credit_spreads(data),
            'market_breadth': self._calculate_market_breadth(data),
            'sentiment_indicator': self._calculate_sentiment_indicator(data)
        }
        
        return indicators

    def _analyze_risk_environment(self, data: Dict[str, Any]) -> str:
        """Analyze current risk environment"""
        try:
            vix_level = data['indices']['VIX']['price']
            spx_trend = self._calculate_trend(data['indices']['SPX'])
            credit_spreads = data.get('credit_spreads', {}).get('high_yield')
            
            # Risk scoring
            risk_score = 0
            
            # VIX contribution
            if vix_level < self.regime_thresholds['vix']['low']:
                risk_score += 2  # risk-on
            elif vix_level > self.regime_thresholds['vix']['high']:
                risk_score -= 2  # risk-off
                
            # Trend contribution
            if spx_trend in ['strong_uptrend', 'uptrend']:
                risk_score += 1
            elif spx_trend in ['strong_downtrend', 'downtrend']:
                risk_score -= 1
                
            # Credit spread contribution
            if credit_spreads:
                if credit_spreads < 3:
                    risk_score += 1
                elif credit_spreads > 5:
                    risk_score -= 1
            
            # Determine environment
            if risk_score >= 2:
                return 'risk_on'
            elif risk_score <= -2:
                return 'risk_off'
            else:
                return 'neutral'
                
        except KeyError as e:
            logger.error(f"Missing required data for risk analysis: {e}")
            return 'unknown'

    def _analyze_correlation_regime(self) -> str:
        """Analyze correlation regime"""
        corr_matrix = self._calculate_correlations()
        avg_corr = np.mean(np.abs(corr_matrix.values))
        
        if avg_corr > self.regime_thresholds['correlation']['high']:
            return 'risk_off_correlation'
        elif avg_corr > self.regime_thresholds['correlation']['normal']:
            return 'normal'
        else:
            return 'choppy'

    def _calculate_market_specific_metrics(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Calculate market-specific risk metrics"""
        metrics = {}
        
        # Equity market metrics
        if 'indices' in data:
            metrics['equity_put_call'] = self._calculate_put_call_ratio(data)
            metrics['market_breadth'] = self._calculate_market_breadth(data)
            
        # Fixed income metrics
        if 'rates' in data:
            metrics['rate_volatility'] = self._calculate_rate_volatility(data)
            
        # Currency market metrics
        if 'fx' in data:
            metrics['currency_volatility'] = self._calculate_currency_volatility(data)
            
        return metrics

    def _calculate_volatility_percentile(self, data: Dict[str, Any]) -> float:
        """Calculate current volatility percentile"""
        try:
            vix_history = self.historical_data.get('VIX', [])
            current_vix = data['indices']['VIX']['price']
            
            if vix_history:
                return stats.percentileofscore(vix_history, current_vix) / 100
            return 0.5
            
        except (KeyError, ValueError) as e:
            logger.error(f"Error calculating volatility percentile: {e}")
            return 0.5

    def _update_historical_data(self, new_data: Dict[str, Any]):
        """Update historical price database"""
        for asset_class, assets in new_data.items():
            for asset, price_data in assets.items():
                if asset not in self.historical_data:
                    self.historical_data[asset] = []
                    
                self.historical_data[asset].append(price_data['price'])
                
                # Keep only lookback period
                self.historical_data[asset] = \
                    self.historical_data[asset][-self.lookback_days:]
    
    def _detect_price_anomalies(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect price anomalies using z-scores and recent price movements
        """
        anomalies = []
        z_score_threshold = 3.0  # Standard deviations
        move_threshold = 0.05    # 5% move

        for asset_class, assets in data.items():
            for asset, price_data in assets.items():
                if not price_data:
                    continue

                try:
                    # Get historical prices
                    hist_prices = self.historical_data.get(price_data['symbol'], [])
                    if len(hist_prices) < 20:
                        continue

                    # Calculate z-score
                    current_price = price_data['price']
                    price_zscore = zscore(hist_prices + [current_price])[-1]

                    # Calculate recent move
                    daily_return = price_data.get('change', 0) / 100

                    # Check for anomalies
                    if abs(price_zscore) > z_score_threshold:
                        anomalies.append({
                            'type': 'price_zscore',
                            'asset': f"{asset_class}_{asset}",
                            'zscore': price_zscore,
                            'severity': 'high' if abs(price_zscore) > 4 else 'medium'
                        })

                    if abs(daily_return) > move_threshold:
                        anomalies.append({
                            'type': 'price_move',
                            'asset': f"{asset_class}_{asset}",
                            'move': daily_return * 100,
                            'severity': 'high' if abs(daily_return) > 0.1 else 'medium'
                        })

                except Exception as e:
                    logger.error(f"Error detecting price anomalies for {asset}: {str(e)}")

        return anomalies

    def _detect_correlation_anomalies(self) -> List[Dict[str, Any]]:
        """
        Detect unusual changes in correlation patterns
        """
        anomalies = []
        correlation_threshold = 0.3  # Change in correlation

        try:
            # Get current correlation matrix
            current_corr = self._calculate_correlations()
            if current_corr.empty:
                return anomalies

            # Get historical correlation matrix (using last month as baseline)
            hist_corr = self._calculate_historical_correlation(30)
            if hist_corr.empty:
                return anomalies

            # Compare correlations
            for i in current_corr.index:
                for j in current_corr.columns:
                    if i >= j:  # Avoid duplicate pairs
                        continue

                    curr_corr = current_corr.loc[i, j]
                    hist_corr_val = hist_corr.loc[i, j]
                    
                    corr_change = abs(curr_corr - hist_corr_val)
                    
                    if corr_change > correlation_threshold:
                        anomalies.append({
                            'type': 'correlation_change',
                            'pair': (i, j),
                            'change': corr_change,
                            'current_correlation': curr_corr,
                            'historical_correlation': hist_corr_val,
                            'severity': 'high' if corr_change > 0.5 else 'medium'
                        })

        except Exception as e:
            logger.error(f"Error detecting correlation anomalies: {str(e)}")

        return anomalies

    def _detect_volume_anomalies(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect unusual trading volumes
        """
        anomalies = []
        volume_zscore_threshold = 2.5

        for asset_class, assets in data.items():
            for asset, price_data in assets.items():
                try:
                    if 'volume' not in price_data:
                        continue

                    current_volume = price_data['volume']
                    hist_volumes = self._get_historical_volumes(asset)
                    
                    if not hist_volumes:
                        continue

                    volume_zscore = zscore(hist_volumes + [current_volume])[-1]

                    if abs(volume_zscore) > volume_zscore_threshold:
                        anomalies.append({
                            'type': 'volume_anomaly',
                            'asset': f"{asset_class}_{asset}",
                            'zscore': volume_zscore,
                            'current_volume': current_volume,
                            'avg_volume': np.mean(hist_volumes),
                            'severity': 'high' if abs(volume_zscore) > 3 else 'medium'
                        })

                except Exception as e:
                    logger.error(f"Error detecting volume anomalies for {asset}: {str(e)}")

        return anomalies

    def _calculate_yield_curve_slope(self, data: Dict[str, Any]) -> float:
        """
        Calculate the slope of the yield curve (10Y - 2Y spread)
        """
        try:
            if 'bonds' not in data:
                return 0.0

            rates = data['bonds'].get('rates', {})
            ten_year = rates.get('US10Y', {}).get('price', 0)
            two_year = rates.get('US2Y', {}).get('price', 0)

            return ten_year - two_year

        except Exception as e:
            logger.error(f"Error calculating yield curve slope: {str(e)}")
            return 0.0

    def _analyze_credit_spreads(self, data: Dict[str, Any]) -> Dict[str, float]:
        """
        Analyze credit spread levels and changes
        """
        spreads = {}
        try:
            if 'bonds' not in data:
                return spreads

            # Get Treasury yields
            treasury_10y = data['bonds']['rates'].get('US10Y', {}).get('price', 0)
            
            # Get corporate yields
            ig_yield = data['bonds'].get('corporates', {}).get('IG', {}).get('price', 0)
            hy_yield = data['bonds'].get('corporates', {}).get('HY', {}).get('price', 0)

            # Calculate spreads
            if treasury_10y and ig_yield:
                spreads['ig_spread'] = ig_yield - treasury_10y
            if treasury_10y and hy_yield:
                spreads['hy_spread'] = hy_yield - treasury_10y

        except Exception as e:
            logger.error(f"Error analyzing credit spreads: {str(e)}")

        return spreads

    def _calculate_market_breadth(self, data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate market breadth indicators
        """
        breadth = {}
        try:
            if 'indices' not in data:
                return breadth

            # Calculate advance-decline ratio (if available)
            advances = data['indices'].get('advances', 0)
            declines = data['indices'].get('declines', 0)
            if advances and declines:
                breadth['advance_decline_ratio'] = advances / max(declines, 1)

            # Calculate percentage of stocks above moving averages
            above_50ma = data['indices'].get('above_50d_ma', 0)
            above_200ma = data['indices'].get('above_200d_ma', 0)
            
            breadth['above_50ma_pct'] = above_50ma if above_50ma is not None else 0
            breadth['above_200ma_pct'] = above_200ma if above_200ma is not None else 0

        except Exception as e:
            logger.error(f"Error calculating market breadth: {str(e)}")

        return breadth

    def _calculate_sentiment_indicator(self, data: Dict[str, Any]) -> float:
        """
        Calculate composite sentiment indicator
        """
        try:
            sentiment_score = 0.0
            components = 0

            # VIX contribution (inverse relationship)
            vix = data.get('indices', {}).get('VIX', {}).get('price')
            if vix:
                vix_score = max(0, min(1, (50 - vix) / 40))  # Normalize VIX
                sentiment_score += vix_score
                components += 1

            # Put-Call ratio contribution (inverse relationship)
            put_call = self._calculate_put_call_ratio(data)
            if put_call:
                pc_score = max(0, min(1, (1 - put_call) / 0.5))
                sentiment_score += pc_score
                components += 1

            # Credit spread contribution (inverse relationship)
            spreads = self._analyze_credit_spreads(data)
            if 'hy_spread' in spreads:
                spread_score = max(0, min(1, (10 - spreads['hy_spread']) / 8))
                sentiment_score += spread_score
                components += 1

            return sentiment_score / max(1, components)

        except Exception as e:
            logger.error(f"Error calculating sentiment indicator: {str(e)}")
            return 0.5  # Neutral sentiment as fallback

    def _calculate_put_call_ratio(self, data: Dict[str, Any]) -> Optional[float]:
        """
        Calculate put-call ratio from options data
        """
        try:
            if 'options' not in data:
                return None

            puts_volume = data['options'].get('puts_volume', 0)
            calls_volume = data['options'].get('calls_volume', 0)

            if calls_volume:
                return puts_volume / calls_volume
            return None

        except Exception as e:
            logger.error(f"Error calculating put-call ratio: {str(e)}")
            return None

    def _calculate_rate_volatility(self, data: Dict[str, Any]) -> Optional[float]:
        """
        Calculate interest rate volatility
        """
        try:
            if 'bonds' not in data:
                return None

            # Get historical 10Y yield data
            yields_10y = self.historical_data.get('US10Y', [])
            if len(yields_10y) < 20:
                return None

            # Calculate daily changes
            changes = np.diff(yields_10y)
            
            # Calculate annualized volatility
            daily_vol = np.std(changes)
            annualized_vol = daily_vol * np.sqrt(252)

            return annualized_vol

        except Exception as e:
            logger.error(f"Error calculating rate volatility: {str(e)}")
            return None

    def _calculate_currency_volatility(self, data: Dict[str, Any]) -> Optional[float]:
        """
        Calculate currency market volatility
        """
        try:
            if 'fx' not in data:
                return None

            # Get historical FX data (using DXY as proxy)
            dxy_data = self.historical_data.get('DXY', [])
            if len(dxy_data) < 20:
                return None

            # Calculate daily returns
            returns = np.diff(dxy_data) / dxy_data[:-1]
            
            # Calculate annualized volatility
            daily_vol = np.std(returns)
            annualized_vol = daily_vol * np.sqrt(252)

            return annualized_vol

        except Exception as e:
            logger.error(f"Error calculating currency volatility: {str(e)}")
            return None

    def _calculate_average_correlation(self) -> float:
        """
        Calculate average pairwise correlation
        """
        try:
            corr_matrix = self._calculate_correlations()
            if corr_matrix.empty:
                return 0.0

            # Get upper triangle of correlation matrix (excluding diagonal)
            upper_triangle = np.triu(corr_matrix.values, k=1)
            
            # Calculate mean of non-zero elements
            correlations = upper_triangle[upper_triangle != 0]
            if len(correlations) > 0:
                return float(np.mean(np.abs(correlations)))
            return 0.0

        except Exception as e:
            logger.error(f"Error calculating average correlation: {str(e)}")
            return 0.0

    def _calculate_risk_dispersion(self, data: Dict[str, Any]) -> float:
        """
        Calculate dispersion of risk across assets
        """
        try:
            volatilities = []
            
            for asset_class, assets in data.items():
                for asset, price_data in assets.items():
                    hist_data = self.historical_data.get(asset, [])
                    if len(hist_data) >= 20:
                        returns = np.diff(hist_data) / hist_data[:-1]
                        vol = np.std(returns) * np.sqrt(252)
                        volatilities.append(vol)

            if volatilities:
                return float(np.std(volatilities))
            return 0.0

        except Exception as e:
            logger.error(f"Error calculating risk dispersion: {str(e)}")
            return 0.0

    def _calculate_tail_risk(self, data: Dict[str, Any]) -> float:
        """
        Calculate tail risk measure using extreme returns
        """
        try:
            all_returns = []
            
            # Collect returns across assets
            for asset_class, assets in data.items():
                for asset, price_data in assets.items():
                    hist_data = self.historical_data.get(asset, [])
                    if len(hist_data) >= 20:
                        returns = np.diff(hist_data) / hist_data[:-1]
                        all_returns.extend(returns)

            if all_returns:
                # Calculate 5% Value at Risk
                return float(np.percentile(all_returns, 5))
            return 0.0

        except Exception as e:
            logger.error(f"Error calculating tail risk: {str(e)}")
            return 0.0

    def _analyze_volatility_regime(self, data: Dict[str, Any]) -> str:
        """
        Analyze current volatility regime
        """
        try:
            vix_level = data.get('indices', {}).get('VIX', {}).get('price')
            if not vix_level:
                return 'unknown'

            if vix_level < self.regime_thresholds['vix']['low']:
                return 'low'
            elif vix_level < self.regime_thresholds['vix']['elevated']:
                return 'normal'
            elif vix_level < self.regime_thresholds['vix']['high']:
                return 'elevated'
            else:
                return 'high'

        except Exception as e:
            logger.error(f"Error analyzing volatility regime: {str(e)}")
            return 'unknown'
        
    def _analyze_liquidity_conditions(self, data: Dict[str, Any]) -> str:
        """
        Analyze market liquidity conditions
        """
        try:
            # Calculate composite liquidity score
            score = 0
            components = 0

            # Volume analysis
            for asset_class, assets in data.items():
                for asset, asset_data in assets.items():
                    if 'volume' in asset_data:
                        hist_volumes = self._get_historical_volumes(asset)
                        if hist_volumes:
                            avg_volume = np.mean(hist_volumes)
                            if asset_data['volume'] > avg_volume:
                                score += 1
                            else:
                                score -= 1
                            components += 1

            # Bid-ask spreads if available
            spreads = data.get('market_quality', {}).get('bid_ask_spreads', {})
            if spreads:
                avg_spread = np.mean(list(spreads.values()))
                if avg_spread < self.regime_thresholds['liquidity']['tight']:
                    score += 1
                elif avg_spread > self.regime_thresholds['liquidity']['stressed']:
                    score -= 2
                components += 1

            # Determine liquidity condition
            if components == 0:
                return 'unknown'
            
            avg_score = score / components
            if avg_score > 0.5:
                return 'ample'
            elif avg_score > -0.5:
                return 'normal'
            else:
                return 'tight'

        except Exception as e:
            logger.error(f"Error analyzing liquidity conditions: {str(e)}")
            return 'unknown'

    def _identify_dominant_factors(self, data: Dict[str, Any]) -> List[str]:
        """
        Identify dominant market factors based on correlations and moves
        """
        try:
            factors = []
            
            # Check for strong trends in key indicators
            if 'indices' in data and 'VIX' in data['indices']:
                vix_level = data['indices']['VIX'].get('price', 0)
                if vix_level > self.regime_thresholds['vix']['high']:
                    factors.append('risk_aversion')

            # Check yield curve
            curve_slope = self._calculate_yield_curve_slope(data)
            if abs(curve_slope) < 0:
                factors.append('recession_risk')
            elif curve_slope > 1:
                factors.append('growth_expectations')

            # Check credit spreads
            spreads = self._analyze_credit_spreads(data)
            if spreads.get('hy_spread', 0) > 5:
                factors.append('credit_stress')

            # Check currency strength
            if 'fx' in data and 'DXY' in data['fx']:
                dxy_change = data['fx']['DXY'].get('change', 0)
                if abs(dxy_change) > 1:
                    factors.append('dollar_dominance' if dxy_change > 0 else 'dollar_weakness')

            # Check commodity prices
            if 'commodities' in data:
                if any(asset_data.get('change', 0) > 5 for asset_data in data['commodities'].values()):
                    factors.append('commodity_pressure')

            return factors if factors else ['no_dominant_factor']

        except Exception as e:
            logger.error(f"Error identifying dominant factors: {str(e)}")
            return ['unknown']

    def _calculate_historical_correlation(self, lookback_days: int) -> pd.DataFrame:
        """
        Calculate historical correlation matrix
        """
        try:
            # Collect historical prices for all assets
            price_data = {}
            for asset, hist_prices in self.historical_data.items():
                if len(hist_prices) >= lookback_days:
                    price_data[asset] = hist_prices[-lookback_days:]

            if not price_data:
                return pd.DataFrame()

            # Create DataFrame and calculate correlation
            df = pd.DataFrame(price_data)
            return df.corr()

        except Exception as e:
            logger.error(f"Error calculating historical correlation: {str(e)}")
            return pd.DataFrame()

    def _get_historical_volumes(self, asset: str) -> List[float]:
        """
        Get historical volume data for an asset
        """
        try:
            return self.historical_data.get(f"{asset}_volume", [])
        except Exception as e:
            logger.error(f"Error getting historical volumes for {asset}: {str(e)}")
            return []

    def _calculate_correlations(self) -> pd.DataFrame:
        """
        Calculate current correlation matrix across assets
        """
        try:
            # Get all available price histories
            price_data = {}
            min_history = 20  # Minimum history required
            
            for asset, prices in self.historical_data.items():
                if len(prices) >= min_history:
                    # Calculate returns
                    returns = np.diff(prices) / prices[:-1]
                    price_data[asset] = returns

            if not price_data:
                return pd.DataFrame()

            # Create DataFrame and calculate correlation
            return pd.DataFrame(price_data).corr()

        except Exception as e:
            logger.error(f"Error calculating correlations: {str(e)}")
            return pd.DataFrame()