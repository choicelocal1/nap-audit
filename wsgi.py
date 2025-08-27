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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from flask import Flask, request, jsonify
from nap import NAPAuditor # Import the NAPAuditor class
from urllib.parse import urlparse

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
    try:
        data = request.get_json()
        
        # 1. Validate the password
        if not data or 'password' not in data or data['password'] != API_PASSWORD:
            return jsonify({'error': 'Unauthorized. Invalid or missing password.'}), 403

        # 2. Check for other required fields
        required_fields = ['url', 'email', 'filename']
        if not all(field in data for field in required_fields):
            return jsonify({'error': f'Missing fields. Required fields are: {required_fields}'}), 400

        google_sheet_url = data['url']
        email_address = data['email']
        output_filename_prefix = data['filename']
        
        # 3. Download the Google Sheet data
        csv_data = get_sheet_data(google_sheet_url)
        if not csv_data:
            return jsonify({"status": "error", "message": "Failed to download Google Sheet. Check the URL and sharing settings."}), 500

        # 4. Read the business names from the downloaded CSV content
        from io import StringIO
        df = pd.read_csv(StringIO(csv_data))
        business_names = df.iloc[:, 0].tolist()
        
        if not business_names:
            return jsonify({"status": "error", "message": "No business names found in the spreadsheet."}), 400

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
            return jsonify({"status": "error", "message": "NAP audit failed to produce a results file."}), 500

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
            body="Hello,\n\nYour NAP audit is complete. The results are attached.\n\nThank you!",
            attachment=file_data,
            attachment_filename=output_filename
        )
        
        # 10. Clean up temporary files
        os.remove(csv_path)
        os.remove(temp_excel_path)
        
        if email_sent:
            return jsonify({"status": "success", "message": f"Audit complete. Results sent to {email_address}."}), 200
        else:
            return jsonify({"status": "error", "message": "NAP audit completed, but failed to send the results via email. Check SMTP credentials."}), 500

    except Exception as e:
        error_msg = f"An unexpected error occurred during processing: {str(e)}"
        print(error_msg)
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
