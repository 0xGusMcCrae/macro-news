# Macro Market Monitor

A comprehensive market monitoring system that analyzes macroeconomic data releases, Federal Reserve communications, and market conditions to provide automated insights and alerts.

## Features

### Data Collection
- **Economic Data**: Automated collection of key economic releases (NFP, CPI, GDP, etc.) from FRED and BLS
- **Fed Communications**: Monitors and analyzes Federal Reserve speeches, statements, and testimony
- **Market Data**: Real-time tracking of equities, bonds, currencies, and other market indicators
- **Bond Markets**: Comprehensive fixed income monitoring including spreads, yields, and credit conditions

### Analysis
- **Fed Analysis**: 
  - Hawkish/dovish scoring of Fed communications
  - Forward guidance extraction
  - Strategic communication analysis
  - Policy bias detection

- **Market Analysis**:
  - Cross-asset correlations
  - Regime detection
  - Risk metrics
  - Anomaly detection
  - Leading indicators

- **Economic Analysis**:
  - Release impact assessment
  - Trend analysis
  - Fed policy implications
  - Market implications

### Notifications
- Daily market updates via email
- Real-time alerts for significant developments
- Customizable notification thresholds
- Detailed analysis reports

## Setup

### Prerequisites
- Python 3.10+
- SQLite
- Redis (optional, falls back to SQLite for caching)

### Installation
1. Clone the repository:
```bash
git clone https://github.com/yourusername/macro-monitor.git
cd macro-monitor
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables in a `.env` file:
```
FRED_API_KEY=your_fred_key
BLS_API_KEY=your_bls_key
ANTHROPIC_API_KEY=your_claude_key
SMTP_SERVER=your_smtp_server
SMTP_PORT=587
SENDER_EMAIL=your_email
SENDER_PASSWORD=your_password
RECIPIENT_EMAIL=recipient_email
```

### Configuration
- Adjust collection frequencies in `config/settings.py`
- Customize analysis thresholds and parameters
- Configure notification preferences
- Set up logging preferences

## Usage

### Running the Monitor
```bash
python -m macro_monitor.main
```

The system will:
- Monitor markets during trading hours
- Process economic releases as they occur
- Analyze Fed communications
- Send daily updates

## License
[MIT](https://choosealicense.com/licenses/mit/)