from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import pandas as pd
import numpy as np
from dataclasses import dataclass
import logging
import anthropic

logger = logging.getLogger(__name__)

@dataclass
class ReleaseAnalysis:
    """Analysis results for economic releases"""
    impact: str  # 'positive', 'negative', 'neutral'
    surprise: float  # standardized surprise vs expectations
    trend: str  # 'improving', 'deteriorating', 'stable'
    fed_implications: Dict[str, str]
    market_implications: Dict[str, str]
    confidence: float  # 0 to 1

class ReleaseAnalyzer:
    """Analyzes economic data releases"""

    def __init__(self, config: Dict[str, str]):
        self.client = anthropic.Client(api_key=config['anthropic_key'])
        self.historical_data = {}
        self.market_context = {}

    async def analyze_release(self,
                            release_data: Dict[str, Any],
                            historical_context: Dict[str, Any],
                            market_context: Dict[str, Any]) -> ReleaseAnalysis:
        """Analyze an economic release"""
        
        # Update context
        self.market_context = market_context
        self._update_historical_data(release_data['indicator'], historical_context)
        
        # Calculate metrics
        surprise = self._calculate_surprise(release_data)
        trend = self._analyze_trend(release_data)
        
        # Get AI analysis
        implications = await self._get_release_implications(
            release_data, surprise, trend
        )
        
        return ReleaseAnalysis(
            impact=self._determine_impact(surprise),
            surprise=surprise,
            trend=trend,
            fed_implications=implications['fed'],
            market_implications=implications['market'],
            confidence=implications['confidence']
        )

    def _calculate_surprise(self, release: Dict[str, Any]) -> float:
        """Calculate standardized surprise vs expectations"""
        if not release.get('expected'):
            return 0.0
            
        actual = float(release['value'])
        expected = float(release['expected'])
        
        # Get historical surprise standard deviation
        hist_surprises = self.historical_data.get(release['indicator'], [])
        if hist_surprises:
            surprise_std = np.std([s['surprise'] for s in hist_surprises])
            if surprise_std > 0:
                return (actual - expected) / surprise_std
                
        return (actual - expected) / abs(expected) if expected != 0 else 0

    def _analyze_trend(self, release: Dict[str, Any]) -> str:
        """Analyze trend in the data"""
        hist_data = self.historical_data.get(release['indicator'], [])
        if len(hist_data) < 3:
            return 'insufficient_data'
            
        recent_values = [float(h['value']) for h in hist_data[-3:]]
        
        # Calculate trend
        slope = np.polyfit(range(len(recent_values)), recent_values, 1)[0]
        
        if abs(slope) < 0.001:
            return 'stable'
        elif slope > 0:
            return 'improving'
        else:
            return 'deteriorating'

    async def _get_release_implications(self,
                                     release: Dict[str, Any],
                                     surprise: float,
                                     trend: str) -> Dict[str, Any]:
        """Get AI analysis of release implications"""
        
        context_string = self._format_context(release)
        
        prompt = f"""Analyze this economic release and its implications:

Release Details:
- Indicator: {release['indicator']}
- Actual: {release['value']}
- Expected: {release.get('expected', 'N/A')}
- Previous: {release.get('previous', 'N/A')}
- Surprise (standardized): {surprise:.2f}
- Trend: {trend}

Context:
{context_string}

Please analyze:

1. Fed Policy Implications:
- How does this affect the Fed's dual mandate?
- Impact on near-term policy decisions?
- Longer-term policy implications?

2. Market Implications:
- Rate expectations
- Asset price implications
- Risk sentiment impact
- Sector-specific effects

Focus on:
- Whether this changes the macro narrative
- Impact on policy expectations
- Market positioning implications"""

        response = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1500,
            temperature=0,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        # Process the response
        return {
            'fed': self._extract_fed_implications(response.content),
            'market': self._extract_market_implications(response.content),
            'confidence': 0.8  # Could be adjusted based on response quality
        }

    def _determine_impact(self, surprise: float) -> str:
        """Determine overall impact of release"""
        if abs(surprise) < 0.5:
            return 'neutral'
        elif surprise > 0:
            return 'positive'
        else:
            return 'negative'

    def _format_context(self, release: Dict[str, Any]) -> str:
        """Format context for analysis"""
        context = []
        
        # Add market context
        if self.market_context:
            context.append("Market Context:")
            context.extend([
                f"- {k}: {v}" 
                for k, v in self.market_context.items()
            ])
        
        # Add historical context
        hist_data = self.historical_data.get(release['indicator'], [])
        if hist_data:
            context.append("\nRecent History:")
            context.extend([
                f"- {h['date']}: {h['value']}"
                for h in hist_data[-3:]
            ])
            
        return "\n".join(context)

    def _update_historical_data(self, 
                              indicator: str,
                              new_data: Dict[str, Any]):
        """Update historical data store"""
        if indicator not in self.historical_data:
            self.historical_data[indicator] = []
            
        self.historical_data[indicator].append(new_data)
        
        # Keep last 12 releases
        self.historical_data[indicator] = \
            self.historical_data[indicator][-12:]