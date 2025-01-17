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
        """Compose analysis of significant market data and economic releases"""
        
        # Format the data into a readable context
        context = self._format_data_for_prompt(market_data, economic_data, fed_analysis)
        today = datetime.now().strftime('%B %d, %Y')

        # Only generate analysis if we have significant data to analyze
        if not context:
            return self._format_newsletter_html(
                "<h1>Market Monitor Update</h1>"
                "<p>No significant economic releases or market events to analyze at this time.</p>"
            )
        
        prompt = f"""As a market analyst, analyze today's ({today}) economic releases, market data, and Fed communications.
Focus on providing specific, data-driven insights about significant events and their implications.

{context}

Important Guidelines:
1. Only analyze data that's actually provided - do not make assumptions or general market commentary without supporting data
2. For economic releases:
   - Compare actual numbers vs expectations and previous readings
   - Break down key components driving the numbers
   - Explain implications for Fed policy and rate expectations
   - Analyze specific implications for risk assets (equities, credit, etc.)
   - Consider how the data affects different market sectors and risk sentiment
3. For Fed communications:
   - Focus on any shifts in policy stance or forward guidance
   - Analyze impact on rate expectations and market positioning
   - Explicitly discuss implications for risk assets and risk-taking behavior
   - Consider how policy views affect different market sectors (growth vs value, duration sensitivity, etc.)
4. For each major data point or communication:
   - Start with the raw data/facts
   - Then analyze policy implications
   - Finally, provide clear view on risk asset implications
5. Use specific numbers and data points to support analysis
6. If certain types of data aren't available, simply omit those sections

Format the response in clear HTML with proper semantic structure, using h1, h2, p, and ul/li tags as appropriate.
Focus on substance over style - only include sections where you have meaningful data to analyze."""

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(self.client.messages.create,
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=4000,
                    temperature=0.2,  # Lower temperature for more focused analysis
                    messages=[{"role": "user", "content": prompt}]
                )
            )
            
            content = response.content
            newsletter_html = ""
            
            if isinstance(content, str):
                newsletter_html = content
            elif hasattr(content, 'text'):
                newsletter_html = content.text
            elif isinstance(content, list) and len(content) > 0:
                newsletter_html = content[0].text if hasattr(content[0], 'text') else str(content[0])
            else:
                newsletter_html = str(content)
            
            final_html = self._format_newsletter_html(newsletter_html)
            return final_html
            
        except Exception as e:
            logger.error(f"Error composing analysis: {str(e)}")
            raise

    def _format_data_for_prompt(self,
                               market_data: dict,
                               economic_data: dict,
                               fed_analysis: dict) -> str:
        """Format the data focusing on significant releases and events"""
        context = []
        
        # Economic Releases
        if economic_data.get('significant_releases'):
            context.append("ECONOMIC RELEASES:")
            for release in economic_data['significant_releases']:
                context.append(f"\n{release['indicator']} Release:")
                context.append(f"- Actual: {release['value']}")
                context.append(f"- Expected: {release.get('expected', 'N/A')}")
                context.append(f"- Previous: {release.get('previous', 'N/A')}")
                if release.get('components'):
                    context.append("\nComponents:")
                    for component, value in release['components'].items():
                        context.append(f"- {component}: {value}")

        # Fed Communications
        if fed_analysis.get('significant_communications'):
            context.append("\nFED COMMUNICATIONS:")
            for comm in fed_analysis['significant_communications']:
                context.append(f"\nSpeaker: {comm['speaker']}")
                context.append(f"Title: {comm['title']}")
                if comm.get('analysis'):
                    if comm['analysis'].get('key_themes'):
                        context.append(f"Key Themes: {', '.join(comm['analysis']['key_themes'])}")
                    if comm['analysis'].get('policy_bias'):
                        context.append(f"Policy Bias: {comm['analysis']['policy_bias']}")
                    if comm['analysis'].get('forward_guidance'):
                        context.append(f"Forward Guidance: {comm['analysis']['forward_guidance']}")

        # Only include market regime data if we have significant economic/Fed news
        if context and market_data.get('regime'):
            context.append("\nMARKET CONTEXT:")
            for key, value in market_data['regime'].items():
                formatted_key = key.replace('_', ' ').title()
                context.append(f"- {formatted_key}: {value}")

        return "\n".join(context)

    def _format_newsletter_html(self, content: str) -> str:
        """Format the newsletter with clean, professional styling"""
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
                .content {{
                    background-color: #fff;
                    padding: 30px;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                h1, h2 {{ 
                    color: #2c3e50;
                    margin-top: 1.5em;
                    margin-bottom: 0.5em;
                }}
                h1 {{ font-size: 24px; }}
                h2 {{ font-size: 20px; }}
                .highlight {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-left: 4px solid #007bff;
                    margin: 15px 0;
                }}
                ul {{
                    padding-left: 20px;
                    margin: 10px 0;
                }}
                li {{ margin: 5px 0; }}
                .data-point {{
                    font-family: monospace;
                    background: #f5f5f5;
                    padding: 2px 4px;
                    border-radius: 3px;
                }}
                .footer {{
                    margin-top: 30px;
                    padding-top: 20px;
                    border-top: 1px solid #eee;
                    color: #666;
                    font-size: 0.8em;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <div class="content">
                {content}
            </div>
            <div class="footer">
                <p>Generated by Market Monitor at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </body>
        </html>
        """
        return html