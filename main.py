from datetime import datetime, time, timedelta
import asyncio
import openai
import anthropic
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
import logging
import pytz

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('macro_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MacroBot:
    def __init__(self):
        self.perplexity_client = openai.OpenAI(
            api_key=os.getenv('PERPLEXITY_API_KEY'),
            base_url="https://api.perplexity.ai"
        )
        self.claude_client = anthropic.Client(api_key=os.getenv('ANTHROPIC_API_KEY'))
        
        # Email settings
        self.smtp_server = os.getenv('SMTP_SERVER')
        self.smtp_port = int(os.getenv('SMTP_PORT'))
        self.sender_email = os.getenv('SENDER_EMAIL')
        self.sender_password = os.getenv('SENDER_PASSWORD')
        self.recipient_email = os.getenv('RECIPIENT_EMAIL')

    async def get_macro_events(self):
        """Query Perplexity for recent macro events"""
        try:
            response = self.perplexity_client.chat.completions.create(
                model="sonar-pro",
                messages=[{
                    "role": "user",
                    "content": """Provide a detailed summary of significant U.S. economic data releases 
                    and important market events from the last 24 hours. Focus ONLY on:

                    1. Key U.S. economic data releases (like NFP, CPI, PCE, GDP, Unemployment Rate, Wage Growth, etc.) with:
                       - Actual numbers
                       - Expected numbers
                       - Previous numbers
                       - Any notable components or sub-indices

                    2. Federal Reserve communications or policy changes

                    3. Major Chinese economic data or significant policy changes

                    4. Market-critical earnings (limited to mega-cap tech or other companies that 
                       can move the broader market)

                    Exclude:
                    - Other countries' central bank decisions
                    - Regular corporate earnings
                    - Minor economic data
                    - Market index movements unless truly exceptional

                    Format in clear, structured text with actual numbers and comparisons. Be sure to include the raw economic data in your response.
                    
                    Use the following as an example:
                    '1. Key U.S. Economic Data Releases
                    * Non-Farm Payrolls (NFP) for January 2025:
                    * Actual: 143,000 jobs added
                    * Expected: 169,000 jobs
                    * Previous: (Revised) 150,000 jobs
                    * Unemployment Rate: Decreased to 4.0% from the previous 4.1%
                    * Average Hourly Earnings: Increased by 0.5% month-over-month; 4.1% year-over-year
                    * Notable Sectors:
                        * Healthcare: Strong job growth
                        * Retail: Positive gains
                        * Government: Increased employment
                        * Mining: Decline in jobs
                    2. Federal Reserve Communications or Policy Changes
                    * Chicago Fed President Austan Goolsbee: Emphasized a cautious approach to interest rate cuts due to uncertainties from recent policy changes, despite favorable economic indicators.
                    reuters.com

                    Dallas Fed President Lorie Logan: Indicated a preference to maintain current interest rates for an extended period, even if inflation approaches the 2% target, citing the need for a cooling labor market before considering rate reductions.
                    marketwatch.com

                    Fed Vice Chair Philip Jefferson: Expressed contentment with the current restrictive monetary policy stance and advocated for patience to assess the impacts of recent administrative policies before making further adjustments.
                    reuters.com

                    3. Major Chinese Economic Data or Significant Policy Changes
                    * Trade Tariffs: China imposed a 10% tariff on U.S. crude oil imports in response to U.S. tariffs, potentially reducing U.S. crude exports to China in 2025.
                    reuters.com
                    4. Market-Critical Earnings
                    * Amazon (AMZN):
                    * Q4 2024 Results:
                        * Earnings Per Share (EPS): $1.86
                        * Revenue: $187.8 billion
                        * Analyst Expectations: EPS of $1.80; Revenue of $187.3 billion
                    * Q1 2025 Guidance:
                        * Revenue Forecast: $153.3 billion
                        * Operating Income Forecast: $16 billion
                        * Analyst Expectations: Revenue of $158.6 billion; Operating Income of $18.4 billion
                    * Notable Segment:
                        * Amazon Web Services (AWS): Revenue of $28.79 billion, slightly below expectations but a 19% year-over-year increase.
                    * Market Reaction: Stock declined over 5% in after-hours trading due to the conservative guidance.'"""
                }]
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error getting macro events: {e}")
            return None

    async def analyze_events(self, events_text):
        """Have Claude analyze the macro events"""
        try:
            response = self.claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4000,
                temperature=0.3,
                messages=[{
                    "role": "user",
                    "content": f"""As a market analyst, analyze these economic events and their implications:

                    {events_text}

                    Provide a comprehensive analysis including:
                    1. Economic data
                    2. Key takeaways and market impact
                    3. Implications for Fed policy and rate expectations
                    4. Effects on different asset classes
                    5. Changes to the macro narrative
                    
                    Format your response in clear HTML with proper structure using h1, h2, p tags etc.
                    
                    Use the following as an example: 
                    'Economic Analysis Report - February 2025
                    1. Key Takeaways and Market Impact
                    Labor Market Analysis
                    The January 2025 jobs report reveals a moderating but still resilient labor market. The lower-than-expected NFP (143,000 vs 169,000) combined with the unemployment rate decrease to 4.0% suggests a "soft landing" scenario is still feasible. However, the 0.5% monthly wage growth indicates persistent wage pressures that could complicate the Fed's inflation targeting efforts.

                    Sectoral Shifts
                    The continued strength in healthcare and retail employment, coupled with government sector expansion, indicates broad-based labor market resilience. The decline in mining sector jobs might signal some weakness in commodity-related industries, potentially linked to global demand concerns.

                    Corporate Performance
                    Amazon's Q4 2024 results exceeded expectations, but the conservative Q1 2025 guidance suggests corporate caution about near-term growth prospects. The AWS segment's performance indicates continued but moderating cloud computing growth, reflecting broader enterprise IT spending patterns.

                    2. Implications for Fed Policy and Rate Expectations
                    The unified messaging from Fed officials (Goolsbee, Logan, and Jefferson) suggests a higher-for-longer interest rate environment, despite market expectations for early rate cuts. Key factors influencing this stance include:

                    Persistent wage growth at 4.1% YoY, significantly above levels consistent with 2% inflation
                    Labor market resilience as evidenced by the declining unemployment rate
                    Need to assess the full impact of previous policy tightening
                    Market expectations for rate cuts may need to be adjusted, with the likelihood of cuts being pushed further into 2025.

                    3. Effects on Asset Classes
                    Equities
                    The mixed economic signals and hawkish Fed stance could lead to increased market volatility. Growth stocks, particularly in the technology sector, may face pressure due to the higher-for-longer rate environment. Value stocks, especially in healthcare and consumer staples, might outperform.

                    Fixed Income
                    Bond yields likely to remain elevated, with the yield curve potentially steepening if market participants push back rate cut expectations. Credit spreads might widen modestly as corporate guidance remains conservative.

                    Commodities
                    The new Chinese tariffs on U.S. crude oil could reshape global energy trade flows and potentially pressure U.S. oil prices. This development, combined with mining sector job losses, suggests potential headwinds for commodity markets.

                    4. Changes to the Macro Narrative
                    The current data is shifting the macro narrative in several key ways:

                    The "soft landing" scenario remains possible but requires threading an increasingly narrow needle between labor market strength and inflation control
                    U.S.-China trade tensions are evolving beyond tariffs to include strategic commodities, potentially creating new inflationary pressures
                    Corporate conservatism, as evidenced by Amazon's guidance, suggests businesses are preparing for a period of moderate growth rather than sharp contraction
                    These developments suggest a complex economic environment in 2025, characterized by resilient but moderating growth, persistent inflation pressures, and evolving global trade dynamics.'"""
                }]
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Error analyzing events: {e}")
            return None

    async def send_email(self, subject, html_content):
        """Send email with analysis"""
        try:
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = self.sender_email
            msg['To'] = self.recipient_email
            msg.attach(MIMEText(html_content, 'html'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
                
            logger.info("Email sent successfully")
        except Exception as e:
            logger.error(f"Error sending email: {e}")

    async def run_daily_update(self):
        """Run the daily macro update process"""
        logger.info("Starting daily update")
        events = await self.get_macro_events()
        if events:
            logger.info("Got macro events, analyzing...")
            analysis = await self.analyze_events(events)
            if analysis:
                subject = f"Daily Macro Update - {datetime.now().strftime('%Y-%m-%d')}"
                logger.info("Analysis complete, sending email...")
                await self.send_email(subject, analysis)
                logger.info("Daily update complete")
            else:
                logger.error("Failed to get analysis from Claude")
        else:
            logger.error("Failed to get events from ChatGPT")

    async def run(self):
        """Main loop - run daily at 9am ET"""
        logger.info("Starting MacroBot")
        while True:
            try:
                now = datetime.now(pytz.timezone('America/New_York'))
                target_time = time(12, 53)  # 9:00 AM ET
                
                # If it's past 9am, wait until tomorrow
                if now.time() >= target_time:
                    tomorrow = now.date() + timedelta(days=1)
                    next_run = datetime.combine(tomorrow, target_time)
                    next_run = pytz.timezone('America/New_York').localize(next_run)
                else:
                    next_run = datetime.combine(now.date(), target_time)
                    next_run = pytz.timezone('America/New_York').localize(next_run)

                # Sleep until next run time
                sleep_seconds = (next_run - now).total_seconds()
                logger.info(f"Sleeping until {next_run}")
                await asyncio.sleep(sleep_seconds)
                
                # Run the update
                await self.run_daily_update()
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                # Sleep for a minute before retrying
                await asyncio.sleep(60)

if __name__ == "__main__":
    bot = MacroBot()
    asyncio.run(bot.run())