from typing import List, Dict, Optional, Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime
import logging
from pathlib import Path
import asyncio
from functools import partial
import pandas as pd
from .market_newsletter import MarketNewsletterComposer

logger = logging.getLogger(__name__)

class EmailNotifier:
    """Handles email notifications for daily market updates"""
    
    def __init__(self, config: Dict[str, str], newsletter_composer: MarketNewsletterComposer):
        self.smtp_server = config['smtp_server']
        self.smtp_port = config['smtp_port']
        self.sender_email = config['sender_email']
        self.sender_password = config['sender_password']
        self.recipient_email = config['recipient_email']
        self.newsletter_composer = newsletter_composer

    async def send_daily_update(self,
                              market_data: Dict[str, Any],
                              economic_data: Dict[str, Any],
                              fed_analysis: Dict[str, Any]) -> None:
        """Send daily market update email"""
        try:
            subject = f"Market Monitor Daily Update - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Have Claude compose the newsletter
            html_content = await self.newsletter_composer.compose_newsletter(
                market_data,
                economic_data,
                fed_analysis
            )
            
            # Send email asynchronously
            await self._send_email_async(
                subject=subject,
                html_content=html_content,
                attachments=self._create_data_attachments(market_data)
            )
            
        except Exception as e:
            logger.error(f"Error preparing daily update: {str(e)}")
            raise

    def _send_email(self,
                   subject: str,
                   html_content: str,
                   attachments: Optional[List[MIMEApplication]] = None) -> None:
        """Send email synchronously"""
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = self.recipient_email

        # Add HTML content
        msg.attach(MIMEText(html_content, 'html'))

        # Add attachments if any
        if attachments:
            for attachment in attachments:
                msg.attach(attachment)

        # Send email
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.send_message(msg)

    async def _send_email_async(self,
                              subject: str,
                              html_content: str,
                              attachments: Optional[List[MIMEApplication]] = None) -> None:
        """Send email asynchronously"""
        loop = asyncio.get_event_loop()
        
        try:
            await loop.run_in_executor(
                None,
                partial(self._send_email, subject, html_content, attachments)
            )
            logger.info(f"Successfully sent email: {subject}")
        except Exception as e:
            logger.error(f"Error in email sending: {str(e)}")
            raise

    def _create_data_attachments(self, 
                               market_data: Dict[str, Any]) -> List[MIMEApplication]:
        """Create Excel attachments from market data"""
        attachments = []
        
        try:
            # Create Excel file with multiple sheets
            with pd.ExcelWriter('market_data.xlsx', engine='xlsxwriter') as excel_buffer:
                # Process market data by asset class
                if 'data' in market_data:
                    for asset_class, assets in market_data['data'].items():
                        # Handle different data structures
                        if isinstance(assets, list):
                            df = pd.DataFrame(assets)
                        elif isinstance(assets, dict):
                            rows = []
                            for name, data in assets.items():
                                if isinstance(data, dict):
                                    data['symbol'] = name
                                    rows.append(data)
                            df = pd.DataFrame(rows)
                        else:
                            continue
                            
                        if not df.empty:
                            df.to_excel(excel_buffer, sheet_name=str(asset_class)[:31])  # Excel sheet name length limit

                # Add regime data if available
                if 'regime' in market_data:
                    regime_df = pd.DataFrame([market_data['regime']])
                    regime_df.to_excel(excel_buffer, sheet_name='Market_Regime')

            # Add Excel file as attachment
            with open('market_data.xlsx', 'rb') as f:
                excel_attachment = MIMEApplication(f.read(), _subtype='xlsx')
                excel_attachment.add_header(
                    'Content-Disposition',
                    'attachment',
                    filename='market_data.xlsx'
                )
                attachments.append(excel_attachment)
                
        except Exception as e:
            logger.error(f"Error creating Excel attachment: {str(e)}")
        
        return attachments