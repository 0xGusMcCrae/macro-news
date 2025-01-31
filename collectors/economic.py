from datetime import datetime, timedelta
import aiohttp
import pandas as pd
from typing import Any, Dict, List, Optional
import asyncio
import calendar
import json
from pathlib import Path
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
        calendar_path = Path(__file__).parent.parent / 'config' / 'economic_calendar.json'
        with open(calendar_path) as f:
            return json.load(f)

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
        current_day = date.strftime('%A').lower()
        current_month_day = date.day
        
        if pattern == 'thursday':
            return current_day == 'thursday'
            
        elif pattern == '1st friday':
            return current_day == 'friday' and 1 <= current_month_day <= 7
            
        elif pattern == '2nd week wednesday':
            return current_day == 'wednesday' and 8 <= current_month_day <= 14
            
        elif pattern == 'mid-month':
            return 13 <= current_month_day <= 17
            
        elif pattern == 'end-month':
            last_day = calendar.monthrange(date.year, date.month)[1]
            return current_month_day >= last_day - 3
            
        elif pattern == 'quarterly':
            return date.month in [1, 4, 7, 10] and 25 <= current_month_day <= 30
            
        elif pattern == '1st week':
            return 1 <= current_month_day <= 7
            
        elif pattern == '2nd_friday':
            return current_day == 'friday' and 8 <= current_month_day <= 14
            
        elif pattern == '3rd week':
            return 15 <= current_month_day <= 21
            
        elif pattern == 'last_tuesday':
            last_day = calendar.monthrange(date.year, date.month)[1]
            last_tuesday = last_day - ((last_day - 2) % 7)
            return current_month_day == last_tuesday
            
        elif pattern == '1st_business_day':
            return current_month_day == 1 and current_day not in ['saturday', 'sunday']
            
        elif pattern == '3rd_business_day':
            return current_month_day == 3 and current_day not in ['saturday', 'sunday']
            
        elif pattern == 'wed_before_nfp':
            return current_day == 'wednesday' and 1 <= current_month_day <= 7
            
        elif pattern == 'fomc_schedule':
            fomc_dates = [
                '2024-01-30', '2024-03-19', '2024-04-30', '2024-06-11',
                '2024-07-30', '2024-09-17', '2024-11-06', '2024-12-17'
            ]
            return date.strftime('%Y-%m-%d') in fomc_dates
        
        return False