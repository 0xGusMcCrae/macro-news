from typing import Dict, Any
import anthropic
from datetime import datetime
import logging
import asyncio
from functools import partial

logger = logging.getLogger(__name__)

class MarketNewsletterComposer:
    def __init__(self, config: Dict[str, str]):
        self.client = anthropic.Client(api_key=config['anthropic'])

    async def compose_newsletter(self,
                                market_data: dict,
                                economic_data: dict,
                                fed_analysis: dict) -> str:
        """Have Claude compose a market newsletter from the data"""
        
        # Format the data into a readable context
        context = self._format_data_for_prompt(market_data, economic_data, fed_analysis)
        today = datetime.now().strftime('%B %d, %Y')
        
        prompt = f"""You are an expert financial analyst writing today's ({today}) market newsletter. 
    Below is the market data, analyzed in the context of recent trends and broader market conditions.

    {context}

    Write an HTML-formatted market newsletter with semantic HTML tags and proper structure. 
    The newsletter should:

    1. Start with a main <h1> headline capturing the key market theme
    2. Include an executive summary in a highlighted box
    3. Break down analysis into clear sections with <h2> headers for:
    - Key Market Movements
    - Market Regime Analysis 
    - Economic Impact (if data available)
    - Fed Implications (if communications available)
    4. End with key takeaways in bullet points

    Use appropriate HTML elements like <p>, <ul>, <li>, <strong>, etc. Proper formatting will be 
    applied via CSS - just focus on semantic HTML structure.

    Write in a professional but engaging style. Focus on explaining "why" things are happening 
    and what it means for markets going forward. Use specific data points to support your analysis."""

        try:
            # Run the API call in a thread pool since it's blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(self.client.messages.create,
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=4000,
                    temperature=0.3,
                    messages=[{"role": "user", "content": prompt}]
                )
            )
            
            # Extract the content from the response
            content = response.content
            newsletter_html = ""
            
            # Handle different response formats
            if isinstance(content, str):
                newsletter_html = content
            elif hasattr(content, 'text'):
                newsletter_html = content.text
            elif isinstance(content, list) and len(content) > 0:
                newsletter_html = content[0].text if hasattr(content[0], 'text') else str(content[0])
            else:
                newsletter_html = str(content)
            
            # Wrap the content in our styled template
            final_html = self._format_newsletter_html(newsletter_html)
            return final_html
            
        except Exception as e:
            logger.error(f"Error composing newsletter: {str(e)}")
            logger.error(f"Response content type: {type(response.content)}")
            logger.error(f"Response content: {response.content}")
            raise

    def _format_data_for_prompt(self,
                               market_data: dict,
                               economic_data: dict,
                               fed_analysis: dict) -> str:
        """Format the raw data into a readable context for Claude"""
        context = []
        
        # Add market data
        if market_data.get('data'):
            context.append("Market Data:")
            for asset_class, assets in market_data['data'].items():
                if assets:
                    context.append(f"\n{asset_class.upper()}:")
                    if isinstance(assets, list):
                        for asset in assets:
                            if asset.get('symbol') and asset.get('price'):
                                context.append(f"- {asset['symbol']}: {asset['price']} ({asset.get('change', 0):.2f}%)")
                    elif isinstance(assets, dict):
                        for name, data in assets.items():
                            if isinstance(data, dict) and data.get('price'):
                                context.append(f"- {name}: {data['price']} ({data.get('change', 0):.2f}%)")

        # Add market regime
        if market_data.get('regime'):
            context.append("\nMarket Regime:")
            for key, value in market_data['regime'].items():
                context.append(f"- {key.replace('_', ' ').title()}: {value}")

        # Add economic releases
        if economic_data.get('significant_releases'):
            context.append("\nSignificant Economic Releases:")
            for release in economic_data['significant_releases']:
                context.append(f"- {release['indicator']}: Actual {release['value']} vs Expected {release.get('expected', 'N/A')}")

        # Add Fed communications
        if fed_analysis.get('significant_communications'):
            context.append("\nFed Communications:")
            for comm in fed_analysis['significant_communications']:
                context.append(f"- {comm['speaker']}: {comm['title']}")
                if comm.get('analysis'):
                    if comm['analysis'].get('key_themes'):
                        context.append(f"  Key themes: {', '.join(comm['analysis']['key_themes'])}")

        return "\n".join(context)

    def _format_newsletter_html(self, content: str) -> str:
        """Format the newsletter content into HTML with styling"""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ 
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 1000px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .section {{
                    margin: 30px 0;
                    padding: 20px;
                    background-color: #fff;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .highlight {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-left: 4px solid #007bff;
                    margin: 10px 0;
                }}
                h1, h2, h3 {{ color: #2c3e50; }}
                .footer {{
                    margin-top: 30px;
                    color: #666;
                    font-size: 0.8em;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <div class="section">
            {content}
            </div>
            <div class="footer">
                <p>Generated by Market Monitor at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </body>
        </html>
        """
        return html