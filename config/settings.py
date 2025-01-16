from typing import Dict, Any
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Base Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
LOG_DIR = BASE_DIR / 'logs'

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# API Keys and Authentication
API_KEYS = {
    'fred': os.getenv('FRED_API_KEY'),
    'bls': os.getenv('BLS_API_KEY'),
    'anthropic': os.getenv('ANTHROPIC_API_KEY'),
}

# Database Configuration
DATABASE_CONFIG = {
    'sqlite': {
        'path': str(DATA_DIR / 'market_monitor.db'),
        'backup_path': str(DATA_DIR / 'backups'),
    },
    'redis': {
        'host': os.getenv('REDIS_HOST', 'localhost'),
        'port': int(os.getenv('REDIS_PORT', 6379)),
        'db': int(os.getenv('REDIS_DB', 0)),
    }
}

# Email Configuration
EMAIL_CONFIG = {
    'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
    'smtp_port': int(os.getenv('SMTP_PORT', 587)),
    'sender_email': os.getenv('SENDER_EMAIL'),
    'sender_password': os.getenv('SENDER_PASSWORD'),
    'recipient_email': os.getenv('RECIPIENT_EMAIL'),
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': str(LOG_DIR / 'market_monitor.log'),
            'formatter': 'standard',
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        }
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console', 'file'],
            'level': os.getenv('LOG_LEVEL', 'INFO'),
        }
    }
}

# Collection Settings
COLLECTION_CONFIG = {
    'default_lookback_days': 252,  # One trading year
    'update_frequency': {
        'market_data': 300,  # 5 minutes
        'economic_data': 3600,  # 1 hour
        'fed_speeches': 3600,  # 1 hour
    },
    'retry_settings': {
        'max_retries': 3,
        'backoff_factor': 0.3,
        'retry_statuses': [500, 502, 503, 504],
    }
}

# Analysis Settings
ANALYSIS_CONFIG = {
    'regime_thresholds': {
        'vix': {'low': 15, 'elevated': 25, 'high': 35},
        'correlation': {'normal': 0.4, 'high': 0.7},
        'liquidity': {'tight': -0.5, 'stressed': -1.0},
    },
    'trend_settings': {
        'short_term_window': 20,
        'medium_term_window': 50,
        'long_term_window': 200,
    },
    'volatility_settings': {
        'lookback_period': 30,
        'high_vol_threshold': 25,
    }
}

# Cache Settings
CACHE_CONFIG = {
    'default_ttl': 3600,  # 1 hour
    'extended_ttl': 86400,  # 1 day
    'market_data_ttl': 300,  # 5 minutes
}