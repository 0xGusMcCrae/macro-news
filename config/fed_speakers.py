from typing import Dict, Any
from enum import Enum

class SpeakerWeight(str, Enum):
    VERY_HIGH = 'very_high'
    HIGH = 'high'
    MEDIUM = 'medium'
    LOW = 'low'

class SpeakerBias(str, Enum):
    HAWKISH = 'hawkish'
    DOVISH = 'dovish'
    CENTRIST = 'centrist'
    UNKNOWN = 'unknown'

# Current FOMC Members
FOMC_MEMBERS = {
    # Board of Governors
    'POWELL': {
        'name': 'Jerome Powell',
        'role': 'Chair',
        'weight': SpeakerWeight.VERY_HIGH,
        'voting': True,
        'bias': SpeakerBias.CENTRIST,
        'term_expiry': '2026-02-05',
        'position': 'Board of Governors',
    },
    'JEFFERSON': {
        'name': 'Philip Jefferson',
        'role': 'Vice Chair',
        'weight': SpeakerWeight.HIGH,
        'voting': True,
        'bias': SpeakerBias.DOVISH,
        'term_expiry': '2036-01-31',
        'position': 'Board of Governors',
    },
    'COOK': {
        'name': 'Lisa Cook',
        'role': 'Governor',
        'weight': SpeakerWeight.HIGH,
        'voting': True,
        'bias': SpeakerBias.DOVISH,
        'term_expiry': '2038-01-31',
        'position': 'Board of Governors',
    },
    'BARR': {
        'name': 'Michael Barr',
        'role': 'Vice Chair for Supervision',
        'weight': SpeakerWeight.HIGH,
        'voting': True,
        'bias': SpeakerBias.CENTRIST,
        'term_expiry': '2032-01-31',
        'position': 'Board of Governors',
    },
    'WALLER': {
        'name': 'Christopher Waller',
        'role': 'Governor',
        'weight': SpeakerWeight.HIGH,
        'voting': True,
        'bias': SpeakerBias.HAWKISH,
        'term_expiry': '2030-01-31',
        'position': 'Board of Governors',
    },
    'KUGLER': {
        'name': 'Adriana Kugler',
        'role': 'Governor',
        'weight': SpeakerWeight.HIGH,
        'voting': True,
        'bias': SpeakerBias.DOVISH,
        'term_expiry': '2036-01-31',
        'position': 'Board of Governors',
    },

    # Federal Reserve Bank Presidents
    'WILLIAMS': {
        'name': 'John Williams',
        'role': 'President',
        'weight': SpeakerWeight.HIGH,
        'voting': True,  # NY Fed always votes
        'bias': SpeakerBias.CENTRIST,
        'position': 'New York Fed',
    },
    'GOOLSBEE': {
        'name': 'Austan Goolsbee',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': True,  # 2024 voter
        'bias': SpeakerBias.DOVISH,
        'position': 'Chicago Fed',
    },
    'BOSTIC': {
        'name': 'Raphael Bostic',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': True,  # 2024 voter
        'bias': SpeakerBias.CENTRIST,
        'position': 'Atlanta Fed',
    },
    'MESTER': {
        'name': 'Loretta Mester',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': True,  # 2024 voter
        'bias': SpeakerBias.HAWKISH,
        'position': 'Cleveland Fed',
    },
    'BARKIN': {
        'name': 'Thomas Barkin',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.CENTRIST,
        'position': 'Richmond Fed',
    },
    'DALY': {
        'name': 'Mary Daly',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.DOVISH,
        'position': 'San Francisco Fed',
    },
    'HARKER': {
        'name': 'Patrick Harker',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.CENTRIST,
        'position': 'Philadelphia Fed',
    },
    'LOGAN': {
        'name': 'Lorie Logan',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.HAWKISH,
        'position': 'Dallas Fed',
    },
    'KASHKARI': {
        'name': 'Neel Kashkari',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.DOVISH,
        'position': 'Minneapolis Fed',
    },
    'SCHMID': {
        'name': 'Alberto Schmid',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.UNKNOWN,  # Too new to classify
        'position': 'St. Louis Fed',
    },
    'COLLINS': {
        'name': 'Susan Collins',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.CENTRIST,
        'position': 'Boston Fed',
    },
    'COOK': {
        'name': 'Jeffrey Cook',
        'role': 'President',
        'weight': SpeakerWeight.MEDIUM,
        'voting': False,
        'bias': SpeakerBias.CENTRIST,
        'position': 'Kansas City Fed',
    }
}

# Speech Types and Their Importance
SPEECH_TYPES = {
    'FOMC_STATEMENT': {
        'weight': SpeakerWeight.VERY_HIGH,
        'description': 'Official FOMC meeting statement',
    },
    'FOMC_MINUTES': {
        'weight': SpeakerWeight.VERY_HIGH,
        'description': 'Detailed minutes from FOMC meetings',
    },
    'SEMI_ANNUAL': {
        'weight': SpeakerWeight.VERY_HIGH,
        'description': 'Semi-annual monetary policy testimony to Congress',
    },
    'PREPARED_SPEECH': {
        'weight': SpeakerWeight.HIGH,
        'description': 'Prepared speech on monetary policy or economy',
    },
    'Q_AND_A': {
        'weight': SpeakerWeight.MEDIUM,
        'description': 'Q&A session following prepared remarks',
    },
    'INTERVIEW': {
        'weight': SpeakerWeight.MEDIUM,
        'description': 'Media interview',
    },
}

# Important Regular Communications
REGULAR_COMMUNICATIONS = {
    'FOMC_MEETING': {
        'frequency': '6-weeks',
        'components': ['statement', 'projections', 'press_conference'],
        'importance': SpeakerWeight.VERY_HIGH,
    },
    'MINUTES': {
        'frequency': '6-weeks',
        'delay_days': 21,  # Released 21 days after meeting
        'importance': SpeakerWeight.HIGH,
    },
    'TESTIMONY': {
        'frequency': 'semi-annual',
        'description': 'Humphrey-Hawkins Testimony',
        'importance': SpeakerWeight.VERY_HIGH,
    },
}

# Voting Rotation for Regional Fed Presidents
VOTING_ROTATION = {
    '2024': {
        'rotating_voters': ['BOSTIC', 'GOOLSBEE', 'MESTER', 'WILLIAMS'],
    },
    '2025': {
        'rotating_voters': ['BARKIN', 'DALY', 'HARKER', 'LOGAN'],
    },
}

# Common Policy Themes to Track
POLICY_THEMES = {
    'INFLATION': [
        'price stability',
        'inflation expectations',
        'transitory',
        'persistent',
    ],
    'EMPLOYMENT': [
        'maximum employment',
        'labor market',
        'wage growth',
        'job market',
    ],
    'FINANCIAL_STABILITY': [
        'financial conditions',
        'market functioning',
        'systemic risk',
        'financial stability',
    ],
    'GROWTH': [
        'economic growth',
        'GDP',
        'economic activity',
        'outlook',
    ],
}

def get_speaker_weight(speaker_id: str) -> SpeakerWeight:
    """Get the weight/importance of a speaker"""
    return FOMC_MEMBERS.get(speaker_id, {}).get('weight', SpeakerWeight.LOW)

def is_voter(speaker_id: str, year: str) -> bool:
    """Check if a speaker is a current FOMC voter"""
    speaker = FOMC_MEMBERS.get(speaker_id)
    if not speaker:
        return False
        
    # Board members always vote
    if speaker['position'] == 'Board of Governors':
        return True
        
    # NY Fed president always votes
    if speaker['position'] == 'New York Fed':
        return True
        
    # Check rotating voters
    return speaker_id in VOTING_ROTATION.get(year, {}).get('rotating_voters', [])

def get_bias(speaker_id: str) -> SpeakerBias:
    """Get the historical bias of a speaker"""
    return FOMC_MEMBERS.get(speaker_id, {}).get('bias', SpeakerBias.UNKNOWN)