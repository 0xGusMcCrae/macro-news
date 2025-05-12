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
            est = pytz.timezone('US/Eastern')
            current_time = datetime.now(est)
            response = self.perplexity_client.chat.completions.create(
                model="sonar-pro",
                messages=[{
                    "role": "user",
                    "content": f"""Provide a concise summary of TODAY'S U.S. macroeconomic developments only:

                                    1. Economic data releases: List each indicator released today with actual figures, previous period values, and consensus expectations (e.g., "CPI: +0.4% m/m actual vs +0.3% expected, prior +0.2%")

                                    2. Fed communications: Summarize only the most market-moving points from any Fed speeches today using direct quotes when possible

                                    3. Treasury/rates: Note any significant Treasury auction results or unusual market movements

                                    4. Policy changes: Briefly describe any new tariffs, trade policies, or fiscal announcements made today

                                    Include ONLY items that were actually released or occurred TODAY. For each data point, include the specific numbers and their significance in 1-2 sentences maximum. If nothing significant was released in a category, simply state "No major releases today."

                                    Please format your response with proper HTML tags."""
                }]
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error getting macro events: {e}")
            return None

    async def analyze_events(self, events_text):
        """Analyze the macro events"""
        try:
            response = self.perplexity_client.chat.completions.create(
                model="sonar-reasoning-pro",
                max_tokens=5000,
                temperature=0.1,
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
            return response.choices[0].message.content
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

    async def run_update(self, is_morning=True):
        """Run the macro update process"""
        time_of_day = "Morning" if is_morning else "Evening"
        logger.info(f"Starting {time_of_day.lower()} update")
        events = await self.get_macro_events()
        if events:
            subject = f"{time_of_day} Macro Update - {datetime.now().strftime('%Y-%m-%d')}"
            html_content = f"""
            <h1>Economic Analysis Report - {datetime.now().strftime('%B %Y')}</h1>
            
            <h2>Economic Events</h2>
            <div class="events-text">
                {events}
            </div>
            
            <h2>Analysis</h2>
            {await self.analyze_events(events)}
            """
            await self.send_email(subject, html_content)
            logger.info(f"{time_of_day} update complete")
        else:
            logger.error(f"Failed to get events from Perplexity for {time_of_day.lower()} update")

    async def run(self):
        """Main loop - run daily at 9am and 7pm ET"""
        logger.info("Starting MacroBot")
        while True:
            try:
                now = datetime.now(pytz.timezone('America/New_York'))
                morning_time = time(9, 0)  # 9:00 AM ET
                evening_time = time(19, 0)  # 7:00 PM ET
                
                # Determine which update should run next
                if now.time() < morning_time:
                    # Before 9am, run morning update today
                    next_run = datetime.combine(now.date(), morning_time)
                    next_run = pytz.timezone('America/New_York').localize(next_run)
                    is_morning = True
                elif now.time() < evening_time:
                    # Between 9am and 7pm, run evening update today
                    next_run = datetime.combine(now.date(), evening_time)
                    next_run = pytz.timezone('America/New_York').localize(next_run)
                    is_morning = False
                else:
                    # After 7pm, run morning update tomorrow
                    tomorrow = now.date() + timedelta(days=1)
                    next_run = datetime.combine(tomorrow, morning_time)
                    next_run = pytz.timezone('America/New_York').localize(next_run)
                    is_morning = True

                # Sleep until next run time
                sleep_seconds = (next_run - now).total_seconds()
                logger.info(f"Sleeping until {'morning' if is_morning else 'evening'} update at {next_run}")
                await asyncio.sleep(sleep_seconds)
                
                # Run the update
                await self.run_update(is_morning)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                # Sleep for a minute before retrying
                await asyncio.sleep(60)

if __name__ == "__main__":
    bot = MacroBot()
    asyncio.run(bot.run())