import asyncio
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from notifiers.market_newsletter import MarketNewsletterComposer
import json

# Sample test data
test_data = {
    'market_data': {
        'data': {
            'bonds': [],  # Empty for now, focusing on economic data
        },
        'regime': {
            'risk_environment': 'neutral',
            'volatility_regime': 'elevated',
            'liquidity_conditions': 'normal',
            'correlation_regime': 'risk_off'
        }
    },
    'economic_data': {
        'significant_releases': [
            {
                'indicator': 'CPI',
                'value': '3.4%',
                'expected': '3.2%',
                'previous': '3.1%',
                'components': {
                    'Core CPI (YoY)': '3.9%',
                    'Core CPI (MoM)': '0.3%',
                    'Shelter': '0.4%',
                    'Transportation': '-0.2%',
                    'Food': '0.2%'
                }
            },
            {
                'indicator': 'Retail Sales',
                'value': '0.3%',
                'expected': '0.1%',
                'previous': '-0.2%',
                'components': {
                    'Core Retail Sales': '0.4%',
                    'Auto Sales': '-0.1%',
                    'Restaurant Sales': '0.5%'
                }
            }
        ]
    },
    'fed_analysis': {
        'significant_communications': [
            {
                'speaker': 'Jerome Powell',
                'title': 'Press Conference Following FOMC Meeting',
                'analysis': {
                    'key_themes': [
                        'Inflation remains elevated',
                        'Labor market showing resilience',
                        'Need to see sustained progress on inflation'
                    ],
                    'policy_bias': 'hawkish',
                    'forward_guidance': 'Policy likely to remain restrictive for some time'
                }
            }
        ]
    }
}

async def test_newsletter():
    try:
        # Get the API key from environment variable
        from config.settings import API_KEYS
        
        # Initialize the composer
        composer = MarketNewsletterComposer(API_KEYS)
        
        # Create test output directory if it doesn't exist
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        # Generate the newsletter
        newsletter = await composer.compose_newsletter(
            test_data['market_data'],
            test_data['economic_data'],
            test_data['fed_analysis']
        )
        
        # Save the output
        output_file = output_dir / 'test_newsletter.html'
        output_file.write_text(newsletter)
        
        print(f"Newsletter generated and saved to {output_file}")
        
    except Exception as e:
        print(f"Error testing newsletter: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_newsletter())