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
# API ENDPOINT
# =========================================================================
@app.route('/audit', methods=['POST'])
def run_audit_endpoint():
    """
    API endpoint to trigger the NAP audit.
    It expects a JSON payload with a 'businesses' key and an 'email' key.
    """
    try:
        data = request.get_json()
        if not data or 'businesses' not in data or 'email' not in data:
            return jsonify({'error': 'Invalid request. Please send a JSON object with a "businesses" key and an "email" key.'}), 400

        business_names = data['businesses']
        email_address = data['email']
        
        auditor = NAPAuditor()

        # Process each business name provided in the payload
        for i, business_name in enumerate(business_names):
            print(f"Processing {i+1}/{len(business_names)}: {business_name}")
            auditor.process_business(str(business_name))
            time.sleep(1) # Delay to avoid API rate limits

        # Save results to a temporary CSV file
        csv_path = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        if auditor.results:
            output_df = pd.DataFrame(auditor.results)
            output_df.to_csv(csv_path, index=False)
        else:
            return jsonify({"status": "error", "message": "NAP audit failed to produce a results file."}), 500

        # Convert CSV to Excel
        output_filename = f"nap_audit_results_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
        temp_excel_path = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
        pd.read_csv(csv_path).to_excel(temp_excel_path, index=False)
        
        # Read the created Excel file for email attachment
        with open(temp_excel_path, "rb") as f:
            file_data = f.read()

        # Send the email with the Excel attachment
        email_sent = send_email(
            to_email=email_address,
            subject="NAP Audit Results",
            body="Hello,\n\nYour NAP audit is complete. The results are attached.\n\nThank you!",
            attachment=file_data,
            attachment_filename=output_filename
        )
        
        # Clean up temporary files
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
