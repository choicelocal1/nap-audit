import os
import time
import json
import traceback
import tempfile
import pandas as pd
from datetime import datetime
from celery import Celery
from nap import NAPAuditor
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Initialize Celery with Redis configuration
redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# Fix for Heroku Redis SSL
if redis_url.startswith('rediss://'):
    redis_url += '?ssl_cert_reqs=CERT_NONE'

app = Celery('tasks', broker=redis_url, backend=redis_url)

# Configure Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour max per task
    task_soft_time_limit=3500,  # 58 minutes soft limit
    result_expires=3600,  # Results expire after 1 hour
)

# Load SMTP configuration
SMTP_EMAIL = os.environ.get("SMTP_EMAIL")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")

def send_email(to_email, subject, body, attachment=None, attachment_filename=None):
    """Sends an email with an optional attachment."""
    if not all([SMTP_EMAIL, SMTP_PASSWORD, to_email]):
        print("SMTP configuration or recipient email is missing. Cannot send email.")
        return False
        
    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_EMAIL
        msg['To'] = to_email
        msg['Subject'] = subject
    
        msg.attach(MIMEText(body, 'plain'))
    
        if attachment and attachment_filename:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={attachment_filename}')
            msg.attach(part)
    
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(SMTP_EMAIL, SMTP_PASSWORD)
            smtp.send_message(msg)
            print("Email sent successfully.")
            return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

@app.task(bind=True)
def process_audit_batch(self, batch_data):
    """Process a batch of businesses"""
    try:
        business_names = batch_data['business_names']
        batch_number = batch_data['batch_number']
        total_batches = batch_data['total_batches']
        
        print(f"Processing batch {batch_number}/{total_batches} with {len(business_names)} businesses")
        
        auditor = NAPAuditor()
        results = []
        
        for i, business_name in enumerate(business_names):
            try:
                print(f"Batch {batch_number} - Processing {i+1}/{len(business_names)}: {business_name}")
                auditor.process_business(str(business_name))
                time.sleep(1)  # Rate limiting
            except Exception as e:
                print(f"Error processing {business_name}: {str(e)}")
                # Add error result
                results.append({
                    'Business Name Input': business_name,
                    'GBP Business Name': 'Error',
                    'GBP Address': 'Error',
                    'GBP Website URL': 'Error',
                    'GBP Phone Number': 'Error',
                    'Website Name': 'Error',
                    'Website Address': 'Error',
                    'Website Phone Number': 'Error',
                    'Yext Name': 'Error',
                    'Yext Address': 'Error',
                    'Yext Phone Number': 'Error',
                    'Schema Name': 'Error',
                    'Schema Address': 'Error',
                    'Schema Phone Number': 'Error',
                    'Match Status': 'Error',
                    'Action Needed': f'Error during processing: {str(e)}'
                })
        
        # Add any error results to auditor results
        auditor.results.extend(results)
        
        # Return the results for this batch
        return {
            'batch_number': batch_number,
            'results': auditor.results
        }
        
    except Exception as e:
        print(f"Batch {batch_data.get('batch_number', 'unknown')} failed: {str(e)}")
        raise

@app.task
def combine_and_send_results(batch_results, email_address, output_filename_prefix, total_businesses):
    """Combine all batch results and send email"""
    try:
        # Combine all results
        all_results = []
        for batch in sorted(batch_results, key=lambda x: x['batch_number']):
            all_results.extend(batch['results'])
        
        if not all_results:
            send_email(
                email_address,
                "NAP Audit - No Results",
                "The audit completed but no results were generated.",
            )
            return
        
        # Create Excel file
        output_df = pd.DataFrame(all_results)
        today_date = datetime.now().strftime('%m%d%Y')
        output_filename = f"{output_filename_prefix}_{today_date}.xlsx"
        
        temp_excel_path = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
        output_df.to_excel(temp_excel_path, index=False)
        
        # Read file for attachment
        with open(temp_excel_path, "rb") as f:
            file_data = f.read()
        
        # Calculate statistics
        total_processed = len(all_results)
        all_good_count = sum(1 for r in all_results if r.get('Match Status') == 'All Good')
        needs_update_count = sum(1 for r in all_results if r.get('Match Status') not in ['All Good', 'Error'])
        error_count = sum(1 for r in all_results if r.get('Match Status') == 'Error')
        
        # Send email
        body = f"""Hello,

Your NAP audit is complete. The results are attached.

Audit Summary:
- Total businesses requested: {total_businesses}
- Total businesses processed: {total_processed}
- All good: {all_good_count}
- Needs updates: {needs_update_count}
- Errors: {error_count}
- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

Thank you!
NAP Audit System"""
        
        send_email(
            email_address,
            "NAP Audit Results - Complete",
            body,
            file_data,
            output_filename
        )
        
        # Cleanup
        os.remove(temp_excel_path)
        
    except Exception as e:
        error_msg = f"Failed to combine results and send email: {str(e)}"
        print(error_msg)
        send_email(
            email_address,
            "NAP Audit - Error",
            f"An error occurred while processing your results:\n\n{error_msg}",
        )