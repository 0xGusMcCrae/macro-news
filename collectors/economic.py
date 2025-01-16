from datetime import datetime, timedelta
import aiohttp
import pandas as pd
from typing import Any, Dict, List, Optional
import asyncio
from .base import BaseCollector, CollectorResponse

class EconomicDataCollector(BaseCollector):
    """Collector for economic data from various sources"""

    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.fred_api_key = config['fred']
        self.bls_api_key = config['bls']
        self.release_calendar = self._load_release_calendar()

    async def collect(self) -> CollectorResponse:
        """Collect economic data based on release calendar"""
        try:
            # Get today's scheduled releases
            today_releases = self._get_todays_releases()
            if not today_releases:
                return CollectorResponse(
                    success=True,
                    data=None,
                    metadata={'message': 'No releases scheduled today'}
                )

            # Collect all scheduled releases
            collected_data = {}
            for release in today_releases:
                data = await self._collect_release(release)
                if data.success:
                    collected_data[release['id']] = data.data

            return CollectorResponse(
                success=True,
                data=collected_data,
                metadata={'releases_collected': len(collected_data)}
            )

        except Exception as e:
            return CollectorResponse(
                success=False,
                error=str(e)
            )

    async def _collect_release(self, release: Dict) -> CollectorResponse:
        """Collect specific release data"""
        collector_map = {
            'FRED': self._collect_from_fred,
            'BLS': self._collect_from_bls,
            'Census': self._collect_from_census
        }
        
        collector = collector_map.get(release['source'])
        if not collector:
            return CollectorResponse(
                success=False,
                error=f"Unknown source: {release['source']}"
            )
            
        return await collector(release)

    async def _collect_from_fred(self, release: Dict) -> CollectorResponse:
        """Collect data from FRED"""
        async with aiohttp.ClientSession() as session:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': release['series_id'],
                'api_key': self.fred_api_key,
                'file_type': 'json',
                'sort_order': 'desc',
                'limit': 1
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return CollectorResponse(
                        success=True,
                        data=data['observations'][0],
                        metadata={'source': 'FRED'}
                    )
                else:
                    return CollectorResponse(
                        success=False,
                        error=f"FRED API error: {response.status}"
                    )

    async def _collect_from_bls(self, release: Dict) -> CollectorResponse:
        """Collect data from BLS"""
        async with aiohttp.ClientSession() as session:
            url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
            headers = {'Content-type': 'application/json'}
            data = {
                "seriesid": [release['series_id']],
                "startyear": str(datetime.now().year - 1),
                "endyear": str(datetime.now().year),
                "registrationkey": self.bls_api_key
            }
            
            async with session.post(url, json=data, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return CollectorResponse(
                        success=True,
                        data=data['Results']['series'][0]['data'][0],
                        metadata={'source': 'BLS'}
                    )
                else:
                    return CollectorResponse(
                        success=False,
                        error=f"BLS API error: {response.status}"
                    )

    async def validate_data(self, data: Any) -> bool:
        """Validate collected economic data"""
        if not data:
            return False
            
        required_fields = ['value', 'timestamp']
        return all(field in data for field in required_fields)

    def _load_release_calendar(self) -> Dict:
        """Load economic release calendar"""
        # This would typically load from a configuration file or database
        # For now, returning a sample calendar structure
        return {
            'NFP': {
                'id': 'NFP',
                'name': 'Nonfarm Payrolls',
                'source': 'BLS',
                'series_id': 'CEU0000000001',
                'release_pattern': '1st friday',
                'release_time': '8:30',
                'importance': 'high'
            },
            # Add other releases...
        }

    def _get_todays_releases(self) -> List[Dict]:
        """Get today's scheduled releases"""
        today = datetime.now()
        scheduled_releases = []
        
        for release_id, release in self.release_calendar.items():
            if self._is_release_day(today, release['release_pattern']):
                scheduled_releases.append(release)
                
        return scheduled_releases

    def _is_release_day(self, date: datetime, pattern: str) -> bool:
        """Check if given date matches release pattern"""
        # Example patterns: '1st friday', 'last thursday', '15th', etc.
        # Implementation would check if date matches the pattern
        # This would need more sophisticated logic in production
        return True  # Placeholder