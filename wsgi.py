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
from nap import NAPAuditor # Import the NAPAuditor class
from urllib.parse import urlparse
from io import StringIO

# =========================================================================
# FLASK APPLICATION SETUP
# =========================================================================

app = Flask(__name__)

# Load configuration from environment variables for security and Heroku deployment
SMTP_EMAIL = os.environ.get("SMTP_EMAIL")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
YEXT_API_KEY = os.environ.get("YEXT_API_KEY")
YEXT_BASE_URL = os.environ.get("YEXT_BASE_URL")

# Define the correct password as an environment variable for security
API_PASSWORD = os.environ.get("API_PASSWORD")

# =========================================================================
# EMAIL SENDER
# =========================================================================

def send_email(to_email, subject, body, attachment=None, attachment_filename=None):
    """
    Sends an email with an optional attachment.
    """
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
    """
    Send error notification email
    """
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
    """
    Downloads a Google Sheet as a CSV from a public URL.
    
    Args:
        url (str): The Google Sheets URL.

    Returns:
        str or None: The CSV content as a string, or None on failure.
    """
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
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.text
    except Exception as e:
        print(f"Error downloading Google Sheet: {e}")
        return None

# =========================================================================
# API ENDPOINT
# =========================================================================
@app.route('/audit', methods=['POST'])
def run_audit_endpoint():
    """
    API endpoint to trigger the NAP audit.
    It now expects a JSON payload with 'url', 'email', 'filename', and 'password'.
    """
    request_data = {}
    
    try:
        data = request.get_json()
        request_data = data if data else {}
        email_address = request_data.get('email', SMTP_EMAIL)  # Default to SMTP_EMAIL if not provided
        
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

        # 4. Read the business names from the downloaded CSV content
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

        # 5. Run the audit
        auditor = NAPAuditor()
        for i, business_name in enumerate(business_names):
            print(f"Processing {i+1}/{len(business_names)}: {business_name}")
            auditor.process_business(str(business_name))
            time.sleep(1) # Delay to avoid API rate limits

        # 6. Save results to a temporary CSV file
        csv_path = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        if auditor.results:
            output_df = pd.DataFrame(auditor.results)
            output_df.to_csv(csv_path, index=False)
        else:
            error_msg = "NAP audit completed but no results were generated."
            send_error_notification(
                email_address, 
                "No Results Generated", 
                error_msg,
                request_data
            )
            return jsonify({"status": "error", "message": error_msg}), 500

        # 7. Convert to Excel and create the specific filename
        today_date = datetime.now().strftime('%m%d%Y')
        output_filename = f"{output_filename_prefix}_{today_date}.xlsx"
        temp_excel_path = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
        pd.read_csv(csv_path).to_excel(temp_excel_path, index=False)
        
        # 8. Read the created Excel file for email attachment
        with open(temp_excel_path, "rb") as f:
            file_data = f.read()

        # 9. Send the email with the Excel attachment
        email_sent = send_email(
            to_email=email_address,
            subject="NAP Audit Results",
            body=f"""Hello,

Your NAP audit is complete. The results are attached.

Audit Summary:
- Total businesses processed: {len(auditor.results)}
- Input file: {google_sheet_url}
- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

Thank you!
NAP Audit System""",
            attachment=file_data,
            attachment_filename=output_filename
        )
        
        # 10. Clean up temporary files
        os.remove(csv_path)
        os.remove(temp_excel_path)
        
        if email_sent:
            return jsonify({"status": "success", "message": f"Audit complete. Results sent to {email_address}."}), 200
        else:
            error_msg = "NAP audit completed, but failed to send the results via email. Check SMTP credentials."
            send_error_notification(
                email_address, 
                "Email Send Failed", 
                error_msg,
                request_data
            )
            return jsonify({"status": "error", "message": error_msg}), 500

    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        error_traceback = traceback.format_exc()
        print(error_msg)
        print(error_traceback)
        
        # Try to send error notification if we have an email address
        try:
            if 'email_address' in locals() and email_address:
                send_error_notification(
                    email_address, 
                    "Unexpected Error", 
                    f"{error_msg}\n\nTraceback:\n{error_traceback}",
                    request_data
                )
        except:
            pass  # Don't let error notification failure crash the endpoint
            
        return jsonify({"status": "error", "message": error_msg}), 500

@app.route('/')
def home():
    """
    Simple welcome message for the root URL.
    """
    return "The NAP Auditor is running. Use the /audit endpoint to send data."

if __name__ == '__main__':
    # This block is for local development only. Gunicorn will use the `app` instance.
    app.run(debug=True)