import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from datetime import datetime
import sys
import json
from functools import wraps
import time
import traceback

class CustomFormatter(logging.Formatter):
    """Custom formatter with color support for console output"""
    
    COLORS = {
        'DEBUG': '\033[94m',    # Blue
        'INFO': '\033[92m',     # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[95m', # Magenta
        'RESET': '\033[0m'      # Reset
    }

    def format(self, record):
        if hasattr(record, 'color'):
            record.msg = f"{self.COLORS[record.levelname]}{record.msg}{self.COLORS['RESET']}"
        return super().format(record)

def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    rotation: str = 'midnight'
) -> logging.Logger:
    """Set up logger with both file and console handlers"""
    
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers
    logger.handlers = []

    # Console handler with color formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(CustomFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(console_handler)

    # File handler if log file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file,
            when=rotation,
            encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(file_handler)

    return logger

def log_execution_time(logger: logging.Logger):
    """Decorator to log function execution time"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.debug(
                    f"Function {func.__name__} executed in {execution_time:.2f} seconds"
                )
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    f"Function {func.__name__} failed after {execution_time:.2f} seconds: {str(e)}"
                )
                raise
        return wrapper
    return decorator

class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    
    def format(self, record):
        log_data = {
            'timestamp': datetime.utcfromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        if record.exc_info:
            log_data['exception'] = traceback.format_exception(*record.exc_info)
            
        return json.dumps(log_data)

# Create main logger instance
logger = setup_logger(
    'macro_monitor',
    log_file='logs/macro_monitor.log',
    level=logging.INFO
)