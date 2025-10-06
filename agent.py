"""
Daily Market Summary Email Agent
Fetches market data, generates summary, and sends email report
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import matplotlib.pyplot as plt
import io
import schedule
import time
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketDataCollector:
    """Fetches and processes market data from Yahoo Finance"""
    
    INDICES = {
        "Nifty 50": "^NSEI",
        "Sensex": "^BSESN",
        "Nifty Bank": "^NSEBANK",
        "Nifty Next 50": "^NSMIDCP",
        "S&P 500": "^GSPC",
        "Dow Jones": "^DJI",
        "Nasdaq Composite": "^IXIC",
        "Russell 2000": "^RUT",
        "Nasdaq 100": "^NDX"
    }
    
    def fetch_market_data(self) -> pd.DataFrame:
        """Fetch current and previous day data for all indices"""
        data = []
        
        for name, symbol in self.INDICES.items():
            try:
                ticker = yf.Ticker(symbol)
                # Get last 5 days to ensure we have previous close
                hist = ticker.history(period="5d")
                
                if len(hist) >= 2:
                    current_price = hist['Close'].iloc[-1]
                    previous_close = hist['Close'].iloc[-2]
                    change = current_price - previous_close
                    pct_change = (change / previous_close) * 100
                    
                    data.append({
                        'Index': name,
                        'Symbol': symbol,
                        'Current': round(current_price, 2),
                        'Previous': round(previous_close, 2),
                        'Change': round(change, 2),
                        'Change %': round(pct_change, 2)
                    })
                    logger.info(f"Fetched data for {name}: {pct_change:.2f}%")
                else:
                    logger.warning(f"Insufficient data for {name}")
                    
            except Exception as e:
                logger.error(f"Error fetching {name}: {str(e)}")
                
        return pd.DataFrame(data)


class MarketAnalyzer:
    """Analyzes market data and generates insights"""
    
    def analyze(self, df: pd.DataFrame) -> Dict:
        """Generate market insights from data"""
        if df.empty:
            return {}
        
        # Identify best and worst performers
        best_performer = df.loc[df['Change %'].idxmax()]
        worst_performer = df.loc[df['Change %'].idxmin()]
        
        # Calculate average changes
        avg_change = df['Change %'].mean()
        
        # Separate Indian and US markets
        indian_indices = df[df['Index'].str.contains('Nifty|Sensex')]
        us_indices = df[df['Index'].str.contains('S&P|Dow|Nasdaq|Russell')]
        
        return {
            'best_performer': best_performer,
            'worst_performer': worst_performer,
            'avg_change': avg_change,
            'indian_avg': indian_indices['Change %'].mean() if not indian_indices.empty else 0,
            'us_avg': us_indices['Change %'].mean() if not us_indices.empty else 0,
            'positive_count': len(df[df['Change %'] > 0]),
            'negative_count': len(df[df['Change %'] < 0]),
            'total_count': len(df)
        }


class ContentGenerator:
    """Generates human-readable market summary"""
    
    def generate_summary(self, df: pd.DataFrame, insights: Dict) -> str:
        """Create narrative summary from data"""
        if df.empty or not insights:
            return "Unable to generate market summary due to data unavailability."
        
        # Market sentiment
        sentiment = "positive" if insights['avg_change'] > 0 else "negative" if insights['avg_change'] < -0.3 else "mixed"
        
        # Build summary
        summary_parts = []
        
        # Opening line
        date_str = datetime.now().strftime("%B %d, %Y")
        summary_parts.append(f"📊 Market Summary for {date_str}\n")
        
        # Overall sentiment
        pos_count = insights['positive_count']
        neg_count = insights['negative_count']
        summary_parts.append(
            f"Markets showed {sentiment} sentiment today with {pos_count} indices up and {neg_count} down."
        )
        
        # Regional performance
        if insights['indian_avg'] != 0:
            direction = "gained" if insights['indian_avg'] > 0 else "declined"
            summary_parts.append(
                f"Indian markets {direction} with an average change of {insights['indian_avg']:.2f}%."
            )
        
        if insights['us_avg'] != 0:
            direction = "advanced" if insights['us_avg'] > 0 else "retreated"
            summary_parts.append(
                f"US markets {direction} with an average change of {insights['us_avg']:.2f}%."
            )
        
        # Best and worst performers
        best = insights['best_performer']
        worst = insights['worst_performer']
        summary_parts.append(
            f"\n🔥 Top Performer: {best['Index']} (+{best['Change %']:.2f}%)"
        )
        summary_parts.append(
            f"📉 Worst Performer: {worst['Index']} ({worst['Change %']:.2f}%)"
        )
        
        return "\n".join(summary_parts)
    
    def create_html_table(self, df: pd.DataFrame) -> str:
        """Generate HTML table for email"""
        if df.empty:
            return "<p>No data available</p>"
        
        html = """
        <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
            <thead>
                <tr style="background-color: #2c3e50; color: white;">
                    <th style="padding: 12px; text-align: left; border: 1px solid #ddd;">Index</th>
                    <th style="padding: 12px; text-align: right; border: 1px solid #ddd;">Current</th>
                    <th style="padding: 12px; text-align: right; border: 1px solid #ddd;">Change</th>
                    <th style="padding: 12px; text-align: right; border: 1px solid #ddd;">Change %</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for _, row in df.iterrows():
            color = "#27ae60" if row['Change %'] > 0 else "#e74c3c" if row['Change %'] < 0 else "#95a5a6"
            arrow = "▲" if row['Change %'] > 0 else "▼" if row['Change %'] < 0 else "•"
            
            html += f"""
                <tr style="border-bottom: 1px solid #ddd;">
                    <td style="padding: 10px; border: 1px solid #ddd;">{row['Index']}</td>
                    <td style="padding: 10px; text-align: right; border: 1px solid #ddd;">{row['Current']:,.2f}</td>
                    <td style="padding: 10px; text-align: right; border: 1px solid #ddd; color: {color};">
                        {arrow} {abs(row['Change']):.2f}
                    </td>
                    <td style="padding: 10px; text-align: right; border: 1px solid #ddd; color: {color}; font-weight: bold;">
                        {row['Change %']:+.2f}%
                    </td>
                </tr>
            """
        
        html += """
            </tbody>
        </table>
        """
        return html


class ChartGenerator:
    """Creates visual charts for market data"""
    
    def create_performance_chart(self, df: pd.DataFrame) -> bytes:
        """Generate bar chart of market performance"""
        if df.empty:
            return None
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Create color list based on positive/negative
        colors = ['#27ae60' if x > 0 else '#e74c3c' for x in df['Change %']]
        
        # Create bar chart
        bars = ax.barh(df['Index'], df['Change %'], color=colors, alpha=0.8)
        
        # Styling
        ax.set_xlabel('Change (%)', fontsize=12, fontweight='bold')
        ax.set_title('Market Performance Overview', fontsize=14, fontweight='bold', pad=20)
        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        
        # Add value labels
        for bar, value in zip(bars, df['Change %']):
            x_pos = value + (0.1 if value > 0 else -0.1)
            ax.text(x_pos, bar.get_y() + bar.get_height()/2, 
                   f'{value:+.2f}%', 
                   va='center', ha='left' if value > 0 else 'right',
                   fontweight='bold', fontsize=9)
        
        plt.tight_layout()
        
        # Save to bytes
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        return buf.getvalue()


class EmailNotifier:
    """Sends email notifications"""
    
    def __init__(self, smtp_server: str, smtp_port: int, sender_email: str, sender_password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
    
    def send_email(self, recipient_email: str, subject: str, html_content: str, chart_data: bytes = None):
        """Send HTML email with optional chart attachment"""
        try:
            msg = MIMEMultipart('related')
            msg['From'] = self.sender_email
            if isinstance(recipient_email, list):
                msg['To'] = ", ".join(recipient_email)
            else:
                msg['To'] = recipient_email            
            msg['Subject'] = subject
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Add chart if provided
            if chart_data:
                image = MIMEImage(chart_data, name='market_chart.png')
                image.add_header('Content-ID', '<chart>')
                msg.attach(image)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                
                if isinstance(recipient_email, list):
                    server.send_message(msg, to_addrs=recipient_email)
                else:
                    server.send_message(msg)

            logger.info(f"Email sent successfully to {recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False


class MarketSummaryAgent:
    """Main agent orchestrating the market summary workflow"""
    
    def __init__(self, email_config: Dict, recipient_email: str):
        self.collector = MarketDataCollector()
        self.analyzer = MarketAnalyzer()
        self.content_gen = ContentGenerator()
        self.chart_gen = ChartGenerator()
        self.notifier = EmailNotifier(**email_config)
        self.recipient_email = recipient_email
    
    def run(self):
        """Execute the complete market summary workflow"""
        try:
            logger.info("Starting market summary generation...")
            
            # 1. Collect data
            df = self.collector.fetch_market_data()
            if df.empty:
                logger.error("No market data collected")
                return False
            
            # 2. Analyze data
            insights = self.analyzer.analyze(df)
            
            # 3. Generate content
            text_summary = self.content_gen.generate_summary(df, insights)
            html_table = self.content_gen.create_html_table(df)
            
            # 4. Create chart
            chart_data = self.chart_gen.create_performance_chart(df)
            
            # 5. Build email
            html_body = f"""
            <html>
                <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
                    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px; color: white; margin-bottom: 20px;">
                        <h1 style="margin: 0; font-size: 28px;">📈 Daily Market Brief</h1>
                        <p style="margin: 5px 0 0 0; opacity: 0.9;">{datetime.now().strftime("%A, %B %d, %Y")}</p>
                    </div>
                    
                    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                        <pre style="font-family: Arial, sans-serif; white-space: pre-wrap; margin: 0;">{text_summary}</pre>
                    </div>
                    
                    <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">Market Performance</h2>
                    {html_table}
                    
                    <div style="margin: 30px 0;">
                        <img src="cid:chart" style="max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                    </div>
                    
                    <div style="margin-top: 30px; padding: 20px; background-color: #ecf0f1; border-radius: 8px; text-align: center;">
                        <p style="margin: 0; color: #7f8c8d; font-size: 14px;">
                            Generated by Your Market Summary Agent 🤖<br>
                            Have a great trading day!
                        </p>
                    </div>
                </body>
            </html>
            """
            
            # 6. Send email
            subject = f"Daily Market Brief – {datetime.now().strftime('%d %b %Y')}"
            success = self.notifier.send_email(
                self.recipient_email,
                subject,
                html_body,
                chart_data
            )
            
            if success:
                logger.info("Market summary sent successfully!")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in workflow: {str(e)}")
            return False


# Configuration and Scheduling
def main():
    """Main function to set up and run the agent"""
    
    # Email configuration (UPDATE THESE WITH YOUR CREDENTIALS)
    EMAIL_CONFIG = {
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'sender_email': 'shielu2003@gmail.com',
        'sender_password': 'rere kxly osjb odfy'
    }
    
    RECIPIENT_EMAIL = [
        'prashant04vasu12b@gmail.com',
        'khushigupta9280@gmail.com'
        ]
    
    # Create agent
    agent = MarketSummaryAgent(EMAIL_CONFIG, RECIPIENT_EMAIL)
    
    # Schedule daily at 9:00 AM
    schedule.every().day.at("09:00").do(agent.run)
    
    logger.info("Market Summary Agent started. Scheduled for 9:00 AM daily.")
    logger.info("Running initial test...")
    
    # Run once immediately for testing
    agent.run()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()