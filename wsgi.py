import os
import csv
import json
import time
import re
import requests
import pandas as pd
import openpyxl
import tempfile
import smtplib
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from flask import Flask, request, jsonify
from urllib.parse import urlparse
from io import StringIO
from celery import Celery, chord

# =========================================================================
# FLASK APPLICATION SETUP
# =========================================================================

app = Flask(__name__)

# Initialize Celery with Redis configuration
redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# Fix for Heroku Redis SSL
if redis_url.startswith('rediss://'):
    redis_url += '?ssl_cert_reqs=CERT_NONE'

celery = Celery('tasks', broker=redis_url, backend=redis_url)

# Load configuration from environment variables
SMTP_EMAIL = os.environ.get("SMTP_EMAIL")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
YEXT_API_KEY = os.environ.get("YEXT_API_KEY")
YEXT_BASE_URL = os.environ.get("YEXT_BASE_URL")
API_PASSWORD = os.environ.get("API_PASSWORD")

# Batch size for processing
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))  # Process 50 businesses per batch

# Import tasks after Celery is configured
from tasks import process_audit_batch, combine_and_send_results

# =========================================================================
# EMAIL SENDER
# =========================================================================

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

def send_error_notification(email_address, error_type, error_details, request_data=None):
    """Send error notification email"""
    subject = f"NAP Audit Error: {error_type}"
    
    body = f"""Hello,

An error occurred during the NAP audit process.

Error Type: {error_type}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

Error Details:
{error_details}

"""
    
    if request_data:
        body += f"""Request Data:
- URL: {request_data.get('url', 'Not provided')}
- Email: {request_data.get('email', 'Not provided')}
- Filename: {request_data.get('filename', 'Not provided')}
- Password: {'Provided' if request_data.get('password') else 'Not provided'}

"""
    
    body += """Please check the application logs for more details.

Thank you,
NAP Audit System"""
    
    send_email(email_address, subject, body)

# =========================================================================
# GOOGLE SHEETS HELPER FUNCTION
# =========================================================================

def get_sheet_data(url):
    """Downloads a Google Sheet as a CSV from a public URL."""
    try:
        # Extract the spreadsheet ID from the URL
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split('/')
        spreadsheet_id = path_parts[3]

        if not spreadsheet_id:
            return None
            
        # Create the export URL for CSV format
        csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"
        
        response = requests.get(csv_url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error downloading Google Sheet: {e}")
        return None

# =========================================================================
# API ENDPOINT
# =========================================================================

@app.route('/audit', methods=['POST'])
def run_audit_endpoint():
    """API endpoint to trigger the NAP audit using Celery tasks."""
    request_data = {}
    
    try:
        data = request.get_json()
        request_data = data if data else {}
        email_address = request_data.get('email', SMTP_EMAIL)
        
        # 1. Validate the password
        if not data or 'password' not in data or data['password'] != API_PASSWORD:
            error_msg = 'Unauthorized. Invalid or missing password.'
            if email_address and email_address != SMTP_EMAIL:
                send_error_notification(
                    email_address, 
                    "Authentication Failed", 
                    error_msg,
                    request_data
                )
            return jsonify({'error': error_msg}), 403

        # 2. Check for other required fields
        required_fields = ['url', 'email', 'filename']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            error_msg = f'Missing fields: {", ".join(missing_fields)}'
            send_error_notification(
                email_address, 
                "Missing Required Fields", 
                error_msg,
                request_data
            )
            return jsonify({'error': f'Missing fields. Required fields are: {required_fields}'}), 400

        google_sheet_url = data['url']
        output_filename_prefix = data['filename']
        
        # 3. Download the Google Sheet data
        csv_data = get_sheet_data(google_sheet_url)
        if not csv_data:
            error_msg = "Failed to download Google Sheet. Check the URL and ensure the sheet is publicly accessible."
            send_error_notification(
                email_address, 
                "Google Sheet Download Failed", 
                error_msg,
                request_data
            )
            return jsonify({"status": "error", "message": error_msg}), 500

        # 4. Read the business names
        df = pd.read_csv(StringIO(csv_data))
        business_names = df.iloc[:, 0].tolist()
        
        if not business_names:
            error_msg = "No business names found in the spreadsheet."
            send_error_notification(
                email_address, 
                "Empty Spreadsheet", 
                error_msg,
                request_data
            )
            return jsonify({"status": "error", "message": error_msg}), 400

        # 5. Split into batches and create Celery tasks
        batches = []
        total_businesses = len(business_names)
        total_batches = (total_businesses + BATCH_SIZE - 1) // BATCH_SIZE
        
        for i in range(0, total_businesses, BATCH_SIZE):
            batch = business_names[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            
            batch_data = {
                'business_names': batch,
                'batch_number': batch_number,
                'total_batches': total_batches
            }
            batches.append(process_audit_batch.s(batch_data))
        
        # 6. Create a chord: all batches run in parallel, then combine results
        callback = combine_and_send_results.s(
            email_address=email_address,
            output_filename_prefix=output_filename_prefix,
            total_businesses=total_businesses
        )
        
        job = chord(batches)(callback)
        
        # 7. Return immediately with job ID
        return jsonify({
            "status": "accepted",
            "message": f"Audit started with {total_businesses} businesses split into {total_batches} batches. Results will be sent to {email_address} when complete.",
            "job_id": str(job.id),
            "total_businesses": total_businesses,
            "total_batches": total_batches,
            "batch_size": BATCH_SIZE
        }), 202

    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        error_traceback = traceback.format_exc()
        print(error_msg)
        print(error_traceback)
        
        try:
            if 'email_address' in locals() and email_address:
                send_error_notification(
                    email_address, 
                    "Unexpected Error", 
                    f"{error_msg}\n\nTraceback:\n{error_traceback}",
                    request_data
                )
        except:
            pass
            
        return jsonify({"status": "error", "message": error_msg}), 500

@app.route('/status/<job_id>', methods=['GET'])
def check_status(job_id):
    """Check the status of a job"""
    try:
        from celery.result import AsyncResult
        result = AsyncResult(job_id, app=celery)
        
        if result.ready():
            return jsonify({
                "status": "completed",
                "ready": True
            })
        elif result.failed():
            return jsonify({
                "status": "failed",
                "ready": True,
                "error": str(result.info)
            })
        else:
            return jsonify({
                "status": "processing",
                "ready": False
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/')
def home():
    """Simple welcome message for the root URL."""
    return "The NAP Auditor is running. Use the /audit endpoint to send data."

if __name__ == '__main__':
    app.run(debug=True)