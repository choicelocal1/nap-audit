#
# nap.py
#
# This script is designed to perform an automated NAP (Name, Address, Phone)
# audit for a list of businesses. It retrieves and compares NAP data from
# multiple sources: Google Places API, the business's own website, and a
# mock Yext API. The results are then compiled into an Excel file.
#
# Author: [Original Author Name - Omitted for Privacy]
# Version: 1.0
# Last Modified: [Date - Omitted for Privacy]
#
# =========================================================================
# IMPORTS
# =========================================================================
import os
import csv
import time
import json
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
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException

# Disable SSL warnings for debugging
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================================
# CONFIGURATION
# =========================================================================
# Note: For production use with a web server, these should be loaded from
# environment variables for security.
SERVICE_ACCOUNT_FILE = 'nightwatch-302222-b4d76c4c4d34.json'
YEXT_API_KEY = "7a2c551e133734c96da4f995aa5117df"
YEXT_BASE_URL = "https://api.yext.com/v2/accounts"

# =========================================================================
# CLASS DEFINITION
# =========================================================================
class NAPAuditor:
    """
    A class to perform NAP (Name, Address, Phone) audits for businesses.
    It integrates with the Google Places API, website scraping, and Yext API
    to check for consistency.
    """
    def __init__(self):
        """Initializes the NAPAuditor, setting up API services and result storage."""
        self.google_service = self.initialize_places_api()
        self.results = []
        
    def initialize_places_api(self):
        """
        Initializes the Google Places API client.

        Returns:
            googleapiclient.discovery.Resource: A service object for the
                                                 Places API, or None if
                                                 initialization fails.
        """
        try:
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            service = build('places', 'v1', credentials=credentials)
            print("Successfully initialized Google Places API client.")
            return service
        except FileNotFoundError:
            print(f"Error: Service account file '{SERVICE_ACCOUNT_FILE}' not found.")
            return None
        except Exception as e:
            print(f"Failed to initialize Google Places API: {e}")
            return None

    def search_google_places(self, query):
        """
        Searches Google Places API for a business based on a query.

        Args:
            query (str): The business name and location to search for.

        Returns:
            dict: The first place found, or an empty dictionary if no place
                  is found or an error occurs.
        """
        if not self.google_service:
            print("Google Places service not initialized. Skipping search.")
            return {}
        try:
            print(f"Searching Google Places for '{query}'...")
            request = self.google_service.places().search(
                query=query,
                fields="name,formattedAddress,formattedPhoneNumber,websiteUri"
            )
            response = request.execute()
            if response and 'places' in response and response['places']:
                place = response['places'][0]
                print("Found business on Google.")
                return {
                    'name': place.get('name'),
                    'address': place.get('formattedAddress'),
                    'phone': place.get('formattedPhoneNumber'),
                    'website': place.get('websiteUri')
                }
        except Exception as e:
            print(f"Error searching Google Places for '{query}': {e}")
        print("Business not found on Google.")
        return {}

    def normalize_string(self, text):
        """
        Normalizes a string for comparison by removing non-alphanumeric
        characters and extra spaces.

        Args:
            text (str): The string to normalize.

        Returns:
            str: The normalized string (lowercase, no special characters,
                 trimmed whitespace).
        """
        if not text:
            return ""
        # Remove non-alphanumeric characters and extra spaces
        return re.sub(r'[^a-zA-Z0-9\s]', '', text.strip().lower())

    def get_website_nap(self, url):
        """
        Scrapes the business's website for NAP information using Selenium.

        Args:
            url (str): The URL of the business website.

        Returns:
            dict: A dictionary containing the scraped address and phone number.
        """
        nap_data = {'website_address': None, 'website_phone': None}
        if not url or url == 'N/A':
            print("No website URL provided. Skipping website scraping.")
            return nap_data
        
        print(f"Scraping website: {url}...")
        
        # Configure Selenium for a headless browser
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        try:
            # Use the ChromeService to manage the driver executable
            service = ChromeService()
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(30)
            driver.get(url)
            
            # Use BeautifulSoup to parse the page source
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find an address that contains a 5-digit zip code
            address_tags = soup.find_all(text=re.compile(r'\d{5}'))
            if address_tags:
                nap_data['website_address'] = address_tags[0].strip()
                print("Found address on website.")
            
            # Find a phone number with a common format
            phone_tags = soup.find_all(text=re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'))
            if phone_tags:
                # Remove non-digit characters for normalization
                nap_data['website_phone'] = re.sub(r'\D', '', phone_tags[0])
                print("Found phone number on website.")
            
        except WebDriverException as e:
            print(f"Selenium error: {e}")
        except TimeoutException as e:
            print(f"Page load timeout: {e}")
        except NoSuchElementException:
            print("Element not found during scraping.")
        finally:
            # Ensure the driver is closed to free up resources
            if 'driver' in locals():
                driver.quit()
        
        return nap_data

    def get_yext_nap(self, business_name):
        """
        Fetches NAP information from a mock Yext API.

        Args:
            business_name (str): The name of the business to search for.

        Returns:
            dict: A dictionary containing the mock Yext address and phone number.
        """
        nap_data = {'yext_address': None, 'yext_phone': None}
        if not self.YEXT_API_KEY:
            print("Yext API key not configured. Skipping Yext check.")
            return nap_data
        
        try:
            # This is a mock API call as the full Yext API requires an account.
            # We are hardcoding a response to simulate success.
            print(f"Mocking Yext API call for '{business_name}'...")
            
            # Simulate a real-world response.
            mock_response = {
                'address': '5335 Far Hills Ave, Dayton, OH, 45429',
                'phone': '+19375281962'
            }
            
            # A real API call would look something like this:
            # params = {
            #     'api_key': self.YEXT_API_KEY,
            #     'v': '20230101',
            #     'business_name': business_name
            # }
            # response = requests.get(f"{self.YEXT_BASE_URL}/businesses", params=params)
            # response.raise_for_status()
            # data = response.json()
            # if data['response']['locations']:
            #     location = data['response']['locations'][0]
            #     nap_data['yext_address'] = location.get('address', 'N/A')
            #     nap_data['yext_phone'] = location.get('mainPhone', 'N/A')
            
            nap_data['yext_address'] = mock_response['address']
            nap_data['yext_phone'] = mock_response['phone']
            print("Yext data retrieved.")
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Yext API: {e}")
        return nap_data
        
    def process_business(self, business_name_input):
        """
        Performs the full NAP audit for a single business and appends the
        results to the `self.results` list.

        Args:
            business_name_input (str): The business name as provided in the
                                       input file.
        """
        print(f"Processing business: {business_name_input}")

        # 1. Get data from Google Places
        gbp_data = self.search_google_places(business_name_input)
        gbp_address = gbp_data.get('address', 'N/A')
        gbp_phone = gbp_data.get('phone', 'N/A')
        website_url = gbp_data.get('website', 'N/A')

        # 2. Scrape data from the website
        website_data = self.get_website_nap(website_url)
        website_address = website_data.get('website_address', 'N/A')
        website_phone = website_data.get('website_phone', 'N/A')

        # 3. Get data from Yext (mocked)
        yext_data = self.get_yext_nap(business_name_input)
        yext_address = yext_data.get('yext_address', 'N/A')
        yext_phone = yext_data.get('yext_phone', 'N/A')
        
        # 4. Compare data and determine action needed
        action_needed = []
        match_status = "All Good"

        # Compare GBP with Website
        if self.normalize_string(gbp_address) != self.normalize_string(website_address):
            action_needed.append("Update Website Address")
            match_status = "Needs Update"
        if self.normalize_string(gbp_phone) != self.normalize_string(website_phone):
            action_needed.append("Update Website Phone")
            match_status = "Needs Update"

        # Compare GBP with Yext
        if self.normalize_string(gbp_address) != self.normalize_string(yext_address):
            action_needed.append("Update Yext Address")
            match_status = "Needs Update"
        if self.normalize_string(gbp_phone) != self.normalize_string(yext_phone):
            action_needed.append("Update Yext Phone")
            match_status = "Needs Update"
            
        # 5. Store the results in the results list
        self.results.append({
            'Business Name Input': business_name_input,
            'GBP Business Name': gbp_data.get('name', 'N/A'),
            'GBP Address': gbp_address,
            'GBP Phone Number': gbp_phone,
            'Website Name': gbp_data.get('name', 'N/A'), # Placeholder for Website Name
            'Website Address': website_address,
            'Website Phone Number': website_phone,
            'Yext Name': gbp_data.get('name', 'N/A'), # Placeholder for Yext Name
            'Yext Address': yext_address,
            'Yext Phone Number': yext_phone,
            'Schema Name': 'N/A', # Placeholder
            'Schema Address': 'N/A', # Placeholder
            'Schema Phone Number': 'N/A', # Placeholder
            'Match Status': match_status,
            'Action Needed': ', '.join(action_needed) if action_needed else 'None',
            'Notes': '' # Placeholder for additional notes
        })
        
    def save_results(self):
        """
        Saves the accumulated audit results to a CSV file.
        """
        output_file = 'nap_audit_results.csv'
        
        if not self.results:
            print("No results to save.")
            return

        df = pd.DataFrame(self.results)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"\nResults saved to {output_file}")
        
        # Print summary
        total = len(self.results)
        all_good = sum(1 for r in self.results if r['Match Status'] == 'All Good')
        needs_update = total - all_good
        
        print(f"\nSummary:")
        print(f"Total businesses processed: {total}")
        print(f"All good: {all_good}")
        print(f"Needs updates: {needs_update}")

