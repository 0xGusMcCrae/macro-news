from typing import List, Dict, Optional, Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import jinja2
from datetime import datetime
import logging
from pathlib import Path
import asyncio
from functools import partial
import pandas as pd

logger = logging.getLogger(__name__)

class EmailNotifier:
    """Handles email notifications and alerts"""
    
    def __init__(self, config: Dict[str, str]):
        self.smtp_server = config['smtp_server']
        self.smtp_port = config['smtp_port']
        self.sender_email = config['sender_email']
        self.sender_password = config['sender_password']
        self.recipient_email = config['recipient_email']
        
        # Set up jinja2 template environment
        template_dir = Path(__file__).parent / 'templates'
        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(template_dir),
            autoescape=jinja2.select_autoescape(['html'])
        )

    async def send_daily_update(self,
                              market_data: Dict[str, Any],
                              economic_data: Dict[str, Any],
                              fed_analysis: Dict[str, Any]) -> None:
        """Send daily market update email"""
        
        subject = f"Market Monitor Daily Update - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Load and render template
        template = self.jinja_env.get_template('daily_update.html')
        html_content = template.render(
            market_data=market_data,
            economic_data=economic_data,
            fed_analysis=fed_analysis,
            date=datetime.now().strftime('%Y-%m-%d')
        )
        
        # Send email asynchronously
        await self._send_email_async(
            subject=subject,
            html_content=html_content,
            attachments=self._create_data_attachments(market_data)
        )

    async def send_alert(self,
                        alert_type: str,
                        message: str,
                        data: Optional[Dict[str, Any]] = None,
                        importance: str = 'medium') -> None:
        """Send alert email"""
        
        subject_prefix = {
            'high': '🚨 URGENT: ',
            'medium': '⚠️ ',
            'low': 'Info: '
        }.get(importance, '')
        
        subject = f"{subject_prefix}Market Monitor Alert - {alert_type}"
        
        template = self.jinja_env.get_template('alert.html')
        html_content = template.render(
            alert_type=alert_type,
            message=message,
            data=data,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        await self._send_email_async(subject=subject, html_content=html_content)

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
            logger.error(f"Failed to send email: {str(e)}")
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

    def _create_data_attachments(self, 
                               market_data: Dict[str, Any]) -> List[MIMEApplication]:
        """Create Excel attachments from market data"""
        attachments = []
        
        # Create Excel file with multiple sheets
        excel_buffer = pd.ExcelWriter('market_data.xlsx', engine='xlsxwriter')
        
        for category, data in market_data.items():
            df = pd.DataFrame(data)
            df.to_excel(excel_buffer, sheet_name=category)
        
        excel_buffer.save()
        
        # Add Excel file as attachment
        with open('market_data.xlsx', 'rb') as f:
            excel_attachment = MIMEApplication(f.read(), _subtype='xlsx')
            excel_attachment.add_header(
                'Content-Disposition',
                'attachment',
                filename='market_data.xlsx'
            )
            attachments.append(excel_attachment)
            
        return attachments