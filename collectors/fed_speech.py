import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import asyncio
from .base import BaseCollector, CollectorResponse

class FedSpeechCollector(BaseCollector):
    """Collector for Federal Reserve speeches and communications"""

    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.base_urls = {
            'speeches': 'https://www.federalreserve.gov/newsevents/speeches.htm',
            'testimony': 'https://www.federalreserve.gov/newsevents/testimony.htm',
            'statements': 'https://www.federalreserve.gov/newsevents/pressreleases.htm'
        }
        self.speech_cache = {}

    async def collect(self) -> CollectorResponse:
        """Collect Fed communications"""
        try:
            all_communications = []
            
            # Collect from all sources
            for source, url in self.base_urls.items():
                comms = await self._collect_from_source(source, url)
                if comms.success:
                    all_communications.extend(comms.data)

            # Sort by date and remove duplicates
            unique_comms = self._deduplicate_communications(all_communications)
            
            return CollectorResponse(
                success=True,
                data=unique_comms,
                metadata={'total_collected': len(unique_comms)}
            )

        except Exception as e:
            return CollectorResponse(
                success=False,
                error=str(e)
            )

    async def _collect_from_source(self, source: str, url: str) -> CollectorResponse:
        """Collect communications from specific source"""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        html = await response.text()
                        communications = await self._parse_communications(html, source)
                        return CollectorResponse(
                            success=True,
                            data=communications,
                            metadata={'source': source}
                        )
                    else:
                        return CollectorResponse(
                            success=False,
                            error=f"HTTP {response.status} from {source}"
                        )
            except Exception as e:
                return CollectorResponse(
                    success=False,
                    error=f"Error collecting from {source}: {str(e)}"
                )

    async def _parse_communications(self, html: str, source: str) -> List[Dict]:
        """Parse communications from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        communications = []

        for event in soup.find_all('div', class_='row eventlist'):
            try:
                # Parse basic information
                date_str = event.find('time').text.strip()
                date = datetime.strptime(date_str, '%B %d, %Y')
                
                # Only process recent communications
                if date < datetime.now() - timedelta(days=7):
                    continue

                title = event.find('a').text.strip()
                url = 'https://www.federalreserve.gov' + event.find('a')['href']
                speaker = event.find('p', class_='speaker').text.strip()

                # Get full text if available
                full_text = await self._get_full_text(url)
                
                communications.append({
                    'date': date,
                    'title': title,
                    'url': url,
                    'speaker': speaker,
                    'source': source,
                    'full_text': full_text,
                    'type': self._determine_communication_type(title, source)
                })

            except Exception as e:
                logger.error(f"Error parsing communication: {str(e)}")
                continue

        return communications

    async def _get_full_text(self, url: str) -> Optional[str]:
        """Get full text of communication"""
        # Check cache first
        if url in self.speech_cache:
            return self.speech_cache[url]

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Find main content div
                        content = soup.find('div', class_='col-xs-12 col-md-8')
                        if content:
                            text = content.get_text(strip=True)
                            self.speech_cache[url] = text
                            return text
            except Exception as e:
                logger.error(f"Error fetching full text: {str(e)}")
        
        return None

    def _determine_communication_type(self, title: str, source: str) -> str:
        """Determine type of communication"""
        title_lower = title.lower()
        
        if 'fomc statement' in title_lower:
            return 'FOMC_STATEMENT'
        elif 'minutes' in title_lower:
            return 'FOMC_MINUTES'
        elif source == 'testimony':
            return 'CONGRESSIONAL_TESTIMONY'
        elif 'speech' in title_lower:
            return 'SPEECH'
        else:
            return 'OTHER'

    async def validate_data(self, data: Any) -> bool:
        """Validate collected Fed communications data"""
        if not isinstance(data, list):
            return False
            
        required_fields = ['date', 'title', 'speaker', 'url']
        return all(all(field in item for field in required_fields) for item in data)

    def _deduplicate_communications(self, communications: List[Dict]) -> List[Dict]:
        """Remove duplicate communications based on URL"""
        seen_urls = set()
        unique_comms = []
        
        for comm in communications:
            if comm['url'] not in seen_urls:
                seen_urls.add(comm['url'])
                unique_comms.append(comm)
        
        return unique_comms