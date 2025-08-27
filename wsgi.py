import os
import csv
import json
import time
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import HttpRequest
import openpyxl
from urllib.parse import urlparse
import difflib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException

from flask import Flask, request, jsonify
import tempfile
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# =========================================================================
# FLASK APPLICATION SETUP
# =========================================================================

app = Flask(__name__)

# Load configuration from environment variables for security and Heroku deployment
# These must be set on your Heroku app's config vars.
API_PASSWORD = os.environ.get("API_PASSWORD")
SMTP_EMAIL = os.environ.get("SMTP_EMAIL")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
YEXT_API_KEY = os.environ.get("YEXT_API_KEY")
YEXT_BASE_URL = os.environ.get("YEXT_BASE_URL")

# Disable SSL warnings for debugging
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================================
# EMAIL FUNCTION
# =========================================================================

def send_email(to_email, subject, body, attachment=None, attachment_filename=None):
    """Sends an email with an optional attachment."""
    msg = MIMEMultipart()
    msg['From'] = SMTP_EMAIL
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    if attachment and attachment_filename:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{attachment_filename}"')
        msg.attach(part)

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# =========================================================================
# NAP AUDITOR CLASS
# =========================================================================

class NAPAuditor:
    """
    A class to perform NAP (Name, Address, Phone) audits for businesses.
    It integrates with the Google Places API, website scraping, and Yext API
    to check for consistency.
    """
    def __init__(self):
        self.google_service = self.initialize_places_api()
        self.results = []
        # Setup Selenium WebDriver for Heroku's headless Chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Path to chromedriver on Heroku
        chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
        self.driver = webdriver.Chrome(
            executable_path=os.environ.get("CHROMEDRIVER_PATH"),
            chrome_options=chrome_options
        )
        # Timeout for page loads
        self.driver.set_page_load_timeout(30)

    def initialize_places_api(self):
        """
        Initializes the Google Places API client using credentials from
        the GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        try:
            if not GOOGLE_CREDENTIALS:
                raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
            
            credentials_info = json.loads(GOOGLE_CREDENTIALS)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            service = build('places', 'v1', credentials=credentials)
            return service
        except (ValueError, json.JSONDecodeError) as e:
            print(f"Error initializing Google Places API: {e}")
            return None

    def search_google_place(self, business_name):
        """Search for a place using the Google Places API."""
        if not self.google_service:
            print("Google Places service not initialized. Skipping search.")
            return None
        
        try:
            request_body = {
                "textQuery": business_name,
                "pageSize": 1,
                "fieldMask": {
                    "fieldPaths": [
                        "id", "displayName", "formattedAddress", "websiteUri",
                        "internationalPhoneNumber", "addressComponents",
                        "location"
                    ]
                }
            }
            request = self.google_service.places().searchText(body=request_body)
            response = request.execute()
            
            if response and response.get('places'):
                place = response['places'][0]
                return {
                    'name': place.get('displayName', {}).get('text'),
                    'address': place.get('formattedAddress'),
                    'website': place.get('websiteUri'),
                    'phone': place.get('internationalPhoneNumber')
                }
            return None
        except Exception as e:
            print(f"Error searching Google Place for '{business_name}': {e}")
            return None

    def search_website_with_selenium(self, url):
        """
        Scrape website for NAP information using Selenium.
        This is a more robust approach for modern, JavaScript-heavy sites.
        """
        try:
            self.driver.get(url)
            time.sleep(5)  # Allow time for dynamic content to load

            # Try to find common NAP elements
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Simple regex for phone numbers
            phone_number_match = re.search(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', page_source)
            phone_number = phone_number_match.group(0) if phone_number_match else 'N/A'

            # Address extraction from common elements like footer or contact sections
            address_text = 'N/A'
            footer_text = soup.find('footer')
            if footer_text:
                address_text = footer_text.get_text().strip()
            
            # Additional logic to search for Schema.org markup
            schema_data = None
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    schema_json = json.loads(script.string)
                    if isinstance(schema_json, dict) and schema_json.get('@type') == 'LocalBusiness':
                        schema_data = schema_json
                        break
                except json.JSONDecodeError:
                    continue

            return {
                'name': soup.title.string if soup.title else 'N/A',
                'address': address_text,
                'phone': phone_number,
                'schema': schema_data
            }
        except (WebDriverException, TimeoutException) as e:
            print(f"Selenium error searching website '{url}': {e}")
            return {'name': 'Error', 'address': 'Error', 'phone': 'Error', 'schema': None}
        except Exception as e:
            print(f"Error during website search '{url}': {e}")
            return {'name': 'Error', 'address': 'Error', 'phone': 'Error', 'schema': None}


    def search_yext(self, business_name):
        """Search Yext API for a business."""
        try:
            if not YEXT_API_KEY or not YEXT_BASE_URL:
                print("Yext API credentials not set. Skipping search.")
                return None
            url = f"{YEXT_BASE_URL}/me/entities?api_key={YEXT_API_KEY}&v=20211018&filter={json.dumps({'name': {'$eq': business_name}})}"
            response = requests.get(url, verify=False)
            data = response.json()
            if data.get('response', {}).get('entities'):
                entity = data['response']['entities'][0]
                return {
                    'name': entity.get('name'),
                    'address': f"{entity.get('address', {}).get('line1', '')}, {entity.get('address', {}).get('city', '')}, {entity.get('address', {}).get('region', '')}, {entity.get('address', {}).get('postalCode', '')}",
                    'phone': entity.get('mainPhone', {}).get('phoneNumber')
                }
            return None
        except Exception as e:
            print(f"Error searching Yext for '{business_name}': {e}")
            return None

    def process_business(self, business_name):
        """Main logic to process a single business and compare NAP data."""
        print(f"Processing business: {business_name}")
        
        gbp_data = self.search_google_place(business_name)
        
        if not gbp_data:
            self.results.append({
                'Business Name Input': business_name,
                'Match Status': 'No GBP Match',
                'Action Needed': 'Manual review required: No close Google Business Profile match found.'
            })
            return

        website_data = self.search_website_with_selenium(gbp_data['website']) if gbp_data.get('website') else {}
        yext_data = self.search_yext(business_name)
        
        # This section will now perform the full comparison logic
        match_status = 'All Good'
        action_needed = []
        
        # Simplified comparison for this example
        if not website_data:
            match_status = 'Needs Updates'
            action_needed.append("Update Website")
            
        if not yext_data:
            match_status = 'Needs Updates'
            action_needed.append("Update Yext")
            
        # Add results to the list
        self.results.append({
            'Business Name Input': business_name,
            'GBP Business Name': gbp_data.get('name'),
            'GBP Address': gbp_data.get('address'),
            'GBP Website URL': gbp_data.get('website'),
            'GBP Phone Number': gbp_data.get('phone'),
            'Website Name': website_data.get('name'),
            'Website Address': website_data.get('address'),
            'Website Phone Number': website_data.get('phone'),
            'Yext Name': yext_data.get('name'),
            'Yext Address': yext_data.get('address'),
            'Yext Phone Number': yext_data.get('phone'),
            'Schema Name': website_data.get('schema', {}).get('name'),
            'Schema Address': website_data.get('schema', {}).get('address', {}).get('streetAddress'),
            'Schema Phone Number': website_data.get('schema', {}).get('telephone'),
            'Match Status': match_status,
            'Action Needed': ', '.join(action_needed) if action_needed else 'None'
        })
        
    def run_audit(self, business_names):
        """Run the audit for a list of businesses."""
        self.results = []
        for business_name in business_names:
            self.process_business(business_name)
            time.sleep(1)
        return self.results

# =========================================================================
# FLASK ENDPOINTS
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
            return jsonify({'error': 'Invalid request. Please send a JSON object with "businesses" and "email" keys.'}), 400

        business_names = data['businesses']
        email_address = data['email']
        input_filename = "businesses.json"  # Dummy filename for email

        auditor = NAPAuditor()
        if not auditor.google_service:
            error_msg = 'Failed to initialize Google Places API. Check environment variables.'
            send_email(email_address, "NAP Audit Failed", error_msg)
            return jsonify({'status': 'error', 'message': error_msg}), 500

        results = auditor.run_audit(business_names)
        
        # Check if results were generated successfully
        if results:
            # Save results to a temporary CSV file
            output_filename = 'nap_audit_results.csv'
            temp_dir = tempfile.gettempdir()
            csv_path = os.path.join(temp_dir, output_filename)
            df = pd.DataFrame(results)
            df.to_csv(csv_path, index=False, encoding='utf-8')
            
            # Read the CSV file data
            with open(csv_path, 'rb') as f:
                file_data = f.read()
            
            # Send the email with the attached CSV
            send_email(
                to_email=email_address,
                subject="NAP Audit Results",
                body=f"Hello,\n\nYour NAP audit for {input_filename} is complete. The results are attached.\n\nThank you!",
                attachment=file_data,
                attachment_filename=output_filename
            )
            
            # Clean up temporary files
            os.remove(csv_path)

            return jsonify({
                "status": "success", 
                "message": f"Audit complete. Results sent to {email_address}."
            }), 200
        else:
            error_msg = "NAP audit failed to produce a results file."
            send_email(email_address, "NAP Audit Failed", error_msg)
            return jsonify({"status": "error", "message": error_msg}), 500

    except Exception as e:
        error_msg = f"An unexpected error occurred during processing: {str(e)}"
        print(error_msg)
        # Attempt to send an email with the error, using the provided email address
        try:
            # We use a fallback email in case the original email is invalid
            send_email(data.get('email', 'fallback_email@example.com'), "NAP Audit Failed", error_msg)
        except Exception as e:
            print(f"Failed to send error email: {e}")
            pass # Fail silently if the email send also fails
        
        return jsonify({"status": "error", "message": error_msg}), 500

@app.route('/')
def home():
    """Simple welcome message for the root URL."""
    return "The NAP Auditor is running. Use the /audit endpoint to send data."

if __name__ == '__main__':
    # This block is for local development only. Gunicorn will use the `app` instance.
    app.run(debug=True)
