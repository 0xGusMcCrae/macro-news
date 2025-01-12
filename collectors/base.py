from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CollectorResponse:
    """Standard response format for all collectors"""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    timestamp: datetime = datetime.now()
    metadata: Dict = None

class BaseCollector(ABC):
    """Base class for all data collectors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.last_collection_time = None
        self.collection_errors = []

    @abstractmethod
    async def collect(self) -> CollectorResponse:
        """Main collection method to be implemented by specific collectors"""
        pass

    @abstractmethod
    async def validate_data(self, data: Any) -> bool:
        """Validate collected data"""
        pass

    async def execute_collection(self) -> CollectorResponse:
        """Template method for executing collection with error handling"""
        try:
            # Record collection attempt
            self.last_collection_time = datetime.now()
            
            # Perform collection
            response = await self.collect()
            
            # Validate if successful
            if response.success and not await self.validate_data(response.data):
                response.success = False
                response.error = "Data validation failed"
            
            return response

        except Exception as e:
            error_msg = f"Collection failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.collection_errors.append({
                'timestamp': datetime.now(),
                'error': error_msg
            })
            return CollectorResponse(
                success=False,
                error=error_msg,
                metadata={'collector_type': self.__class__.__name__}
            )

    def get_collection_status(self) -> Dict[str, Any]:
        """Get status of collector"""
        return {
            'last_collection': self.last_collection_time,
            'recent_errors': self.collection_errors[-5:],
            'total_errors': len(self.collection_errors)
        }