import csv
import time
import json
import re
import os
import gc
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

# Configuration
SERVICE_ACCOUNT_FILE = 'nightwatch-302222-b4d76c4c4d34.json'
YEXT_API_KEY = os.environ.get("YEXT_API_KEY", "7a2c551e133734c96da4f995aa5117df")
YEXT_BASE_URL = os.environ.get("YEXT_BASE_URL", "https://api.yext.com/v2/accounts")

# Disable SSL warnings for debugging
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NAPAuditor:
    def __init__(self):
        self.google_service = self.initialize_places_api()
        self.results = []
        
    def initialize_places_api(self):
        """Initialize the Google Places API client"""
        # Check if it's JSON content or a file path
        service_account_info = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', SERVICE_ACCOUNT_FILE)
        
        try:
            # Try to parse as JSON first (for Heroku)
            service_account_data = json.loads(service_account_info)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_data,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
        except:
            # If not JSON, treat as file path (for local development)
            credentials = service_account.Credentials.from_service_account_file(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
        
        service = build('places', 'v1', credentials=credentials)
        return service
    
    def search_google_place(self, business_name):
        """
        Search for a place using the Google Places API.
        Returns a dictionary with a 'status' key to indicate if a match was found.
        """
        try:
            # New dynamic query creation to improve matching
            location_bias = ""
            search_query = business_name
            
            # Extract location from the business name for a more targeted search
            match = re.search(r'(\b\w+\b)\s*GA$', business_name, re.IGNORECASE)
            if match:
                city = match.group(1)
                location_bias = f" {city}, GA"
                # Create a less specific search query for better results
                search_query = re.sub(r'\s*\b\w+\b\s*GA$', '', business_name)
                
            elif "MidMO" in business_name or "mid mo" in business_name.lower():
                location_bias = " Missouri"
            elif any(state in business_name for state in [" CA", " NY", " TX", " FL", " OH", " VA"]):
                for state in [" CA", " NY", " TX", " FL", " OH", " VA"]:
                    if state in business_name:
                        location_bias = state
                        break
            
            # The full query now includes the base name and the location bias
            full_query = search_query + location_bias
            
            request_body = {
                'textQuery': full_query,
                'maxResultCount': 10, # Increased from 3 to 10 for better results
                'languageCode': 'en-US',
                'regionCode': 'US'
            }
            
            request = self.google_service.places().searchText(body=request_body)
            
            # Add field mask
            original_uri = request.uri
            if '?' in original_uri:
                request.uri = original_uri + '&fields=places.id,places.displayName,places.formattedAddress,places.shortFormattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri'
            else:
                request.uri = original_uri + '?fields=places.id,places.displayName,places.formattedAddress,places.shortFormattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri'
            
            response = request.execute()
            
            if 'places' in response and len(response['places']) > 0:
                best_match = None
                best_score = 0
                
                for place in response['places']:
                    display_name = place.get('displayName', {}).get('text', '')
                    # Use the original business name for comparison
                    score = self.calculate_similarity_score(business_name, display_name)
                    
                    formatted_address = place.get('formattedAddress', '') or place.get('shortFormattedAddress', '')
                    if "MidMO" in business_name and "MO" in formatted_address:
                        score += 0.3
                    elif "MidMO" in business_name and "OH" in formatted_address:
                        score -= 0.5
                    
                    if score > best_score:
                        best_score = score
                        best_match = place
                
                # Use a stricter similarity threshold
                if best_match and best_score >= 0.7:
                    place = best_match
                    display_name = place.get('displayName', {}).get('text', '')
                    formatted_address = place.get('formattedAddress', '') or place.get('shortFormattedAddress', '')
                    phone_number = place.get('nationalPhoneNumber', '') or place.get('internationalPhoneNumber', '')
                    website_url = place.get('websiteUri', '')
                    
                    # Return a successful match
                    return {
                        'status': 'match',
                        'name': display_name,
                        'address': formatted_address,
                        'phone': phone_number,
                        'website': website_url
                    }
                else:
                    # No close match found, but we can return the closest one's name
                    closest_name = best_match.get('displayName', {}).get('text', 'No close match found') if best_match else 'None'
                    return {
                        'status': 'no_match',
                        'closest_match_name': closest_name
                    }
            else:
                # No places returned at all
                return {
                    'status': 'no_results'
                }
                
        except Exception as e:
            return {
                'status': 'error',
                'error_message': str(e)
            }
    
    def search_yext(self, business_name, gbp_website_url=None):
        """Search for business in Yext"""
        try:
            # Get all accounts
            response = requests.get(YEXT_BASE_URL, params={"api_key": YEXT_API_KEY, "limit": 50, "v": "20230821"})
            if response.status_code != 200:
                return None
                
            accounts = response.json()["response"]["accounts"]
            
            best_match = None
            best_score = 0
            
            # Normalize the search name for comparison
            search_name_normalized = business_name.lower().strip()
            
            # Search through all accounts for matching entity
            for account in accounts:
                limit = 50
                pageToken = None
                
                while True:
                    params = {"api_key": YEXT_API_KEY, "limit": limit, "v": "20230821"}
                    if pageToken:
                        params["pageToken"] = pageToken
                    
                    response2 = requests.get(
                        f"{YEXT_BASE_URL}/{account['accountId']}/entities",
                        params=params
                    )
                    
                    if response2.status_code != 200:
                        break
                        
                    response_data = response2.json()
                    if 'response' not in response_data or 'entities' not in response_data['response']:
                        break
                        
                    entities = response_data['response']['entities']
                    
                    # Check each entity for a match
                    for entity in entities:
                        entity_name = entity.get('name', '')
                        entity_name_normalized = entity_name.lower().strip()
                        
                        # --- Primary Matching: Name Similarity ---
                        score = self.calculate_similarity_score(business_name, entity_name)
                        
                        # If this is a better match than what we have, save it
                        if score > best_score and score >= 0.7: # Stricter threshold
                            best_score = score
                            
                            # Extract data from entity
                            address_parts = []
                            if 'address' in entity:
                                addr = entity['address']
                                if 'line1' in addr: address_parts.append(addr['line1'])
                                if 'city' in addr: address_parts.append(addr['city'])
                                if 'region' in addr: address_parts.append(addr['region'])
                                if 'postalCode' in addr: address_parts.append(addr['postalCode'])
                            address = ', '.join(address_parts)
                            phone = str(entity.get('mainPhone', ''))
                            website_url = entity.get('websiteUrl', {}).get('url', '')
                            
                            best_match = {
                                'name': entity_name,
                                'address': address,
                                'phone': phone,
                                'website': website_url,
                                'entity': entity
                            }
                        
                        # --- Secondary Matching: URL Comparison (for specific cases) ---
                        if best_score < 0.7 and gbp_website_url and entity_name_normalized == "comfort keepers home care":
                            yext_website_url = entity.get('websiteUrl', {}).get('url', '')
                            if yext_website_url:
                                gbp_url_path = urlparse(gbp_website_url).path
                                yext_url_path = urlparse(yext_website_url).path
                                
                                # Use a difflib to compare URL paths
                                url_score = difflib.SequenceMatcher(None, gbp_url_path, yext_url_path).ratio()
                                
                                if url_score >= 0.8: # High confidence URL match
                                    score = 0.85 # Assign a high score
                                    if score > best_score:
                                        best_score = score
                                        
                                        address_parts = []
                                        if 'address' in entity:
                                            addr = entity['address']
                                            if 'line1' in addr: address_parts.append(addr['line1'])
                                            if 'city' in addr: address_parts.append(addr['city'])
                                            if 'region' in addr: address_parts.append(addr['region'])
                                            if 'postalCode' in addr: address_parts.append(addr['postalCode'])
                                        address = ', '.join(address_parts)
                                        phone = str(entity.get('mainPhone', ''))
                                        website_url = entity.get('websiteUrl', {}).get('url', '')
                                        
                                        best_match = {
                                            'name': entity_name,
                                            'address': address,
                                            'phone': phone,
                                            'website': website_url,
                                            'entity': entity
                                        }

                    # If we found a match with a high score, we can stop searching this account
                    if best_score >= 0.7:
                        break
                    
                    # Check for more pages
                    if "pageToken" in response_data["response"]:
                        pageToken = response_data["response"]["pageToken"]
                    else:
                        break
                
                # If we found a match with a high score, we can stop searching all accounts
                if best_score >= 0.7:
                    break
            
            return best_match
            
        except Exception as e:
            return None
    
    def calculate_similarity_score(self, name1, name2):
        """Calculate similarity score between two business names"""
        if not name1 or not name2:
            return 0
            
        # Normalize names
        name1_lower = name1.lower().strip()
        name2_lower = name2.lower().strip()
        
        # New: Specific matching for "360 Painting" locations, ignoring special characters
        if '360 painting' in name1_lower or '360 painting' in name2_lower:
            # Remove all non-alphanumeric characters except spaces
            name1_clean = re.sub(r'[^a-z0-9\s]+', '', name1_lower)
            name2_clean = re.sub(r'[^a-z0-9\s]+', '', name2_lower)
            
            # Clean up extra spaces
            name1_clean = ' '.join(name1_clean.split())
            name2_clean = ' '.join(name2_clean.split())
        
        # For Home Helpers specific matching
        elif 'home helpers' in name1_lower and 'home helpers' in name2_lower:
            # Extract location parts for Home Helpers businesses
            # Remove common franchise terms
            franchise_terms = ['home helpers', 'homecare', 'home care', 'of', 'and', '-', '&']
            
            name1_clean = name1_lower
            name2_clean = name2_lower
            
            for term in franchise_terms:
                name1_clean = name1_clean.replace(term, ' ')
                name2_clean = name2_clean.replace(term, ' ')
            
            # Clean up extra spaces
            name1_clean = ' '.join(name1_clean.split())
            name2_clean = ' '.join(name2_clean.split())
            
            # Look for location matches
            name1_words = set(name1_clean.split())
            name2_words = set(name2_clean.split())
            
            # If they share location words, it's a better match
            common_words = name1_words.intersection(name2_words)
            if common_words:
                # Check for a contiguous phrase match if possible
                if any(word in name2_lower for word in name1_lower.split()):
                    return 0.9  # High score for Home Helpers with matching keywords
                
                # Check if any common word is a location (not just small words)
                location_words = [w for w in common_words if len(w) > 2]
                if location_words:
                    return 0.8  # High score for Home Helpers with matching locations
        
        # General matching for other businesses
        else:
            # Remove common suffixes and words
            stop_words = ['of', 'the', 'and', '&', '-', 'inc', 'llc', 'corp', 'corporation', 'company', 'co', 'ltd']
            
            name1_clean = name1_lower
            name2_clean = name2_lower
            
            for word in stop_words:
                name1_clean = re.sub(r'\b' + word + r'\b', ' ', name1_clean)
                name2_clean = re.sub(r'\b' + word + r'\b', ' ', name2_clean)
            
            # Remove extra spaces
            name1_clean = ' '.join(name1_clean.split())
            name2_clean = ' '.join(name2_clean.split())

        # Check if one contains the other
        if name1_clean in name2_clean or name2_clean in name1_clean:
            return 0.9
        
        # Check for word overlap
        words1 = set(name1_clean.split())
        words2 = set(name2_clean.split())
        
        # Remove very short words
        words1 = {w for w in words1 if len(w) > 2}
        words2 = {w for w in words2 if len(w) > 2}
        
        if words1 and words2:
            # Calculate Jaccard similarity
            intersection = len(words1.intersection(words2))
            union = len(words1.union(words2))
            word_score = intersection / union if union > 0 else 0
        else:
            word_score = 0
        
        # Also use sequence matcher for overall similarity
        sequence_score = difflib.SequenceMatcher(None, name1_clean, name2_clean).ratio()
        
        # Return weighted average
        final_score = (word_score * 0.6 + sequence_score * 0.4)
        
        return final_score
    
    def scrape_website_info(self, url):
        """Scrape NAP information from website using Selenium"""
        if not url:
            return None
            
        print(f"\n========== WEBSITE SCRAPING: {url} ==========")
        
        driver = None
        try:
            # Set up Chrome options for headless mode with memory optimizations
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--remote-debugging-port=9222")
            
            # Memory-saving options
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-javascript")
            chrome_options.add_argument("--memory-pressure-off")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-features=TranslateUI")
            chrome_options.add_argument("--disable-ipc-flooding-protection")
            chrome_options.add_argument("--single-process")
            chrome_options.add_argument("--no-zygote")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-dev-tools")
            chrome_options.add_argument("--disable-logging")
            chrome_options.add_argument("--log-level=3")
            
            # Memory limits
            chrome_options.add_argument("--js-heap-size=256")
            chrome_options.add_argument("--memory-model=low")
            
            # Set page load strategy to eager (don't wait for all resources)
            chrome_options.page_load_strategy = 'eager'
            
            # For Heroku deployment with Chrome for Testing
            chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN", "/app/.chrome-for-testing/chrome-linux64/chrome")
            
            # Initialize the WebDriver
            # On Heroku, we need to specify the chromedriver path
            chromedriver_path = os.environ.get("CHROMEDRIVER_PATH", "/app/.chrome-for-testing/chromedriver-linux64/chromedriver")
            
            if os.path.exists(chromedriver_path):
                service = ChromeService(chromedriver_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                driver = webdriver.Chrome(options=chrome_options)
            
            # Set timeout
            driver.set_page_load_timeout(15)
            
            # Get the page
            driver.get(url)
            
            # Wait for a few seconds to let the page load
            time.sleep(3)  # Reduced from 5 seconds
            
            # Get the full page source
            html_text = driver.page_source
            print(f"Status: 200 (using Selenium)")
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_text, 'html.parser')
            
            # Prepare data object
            website_data = {
                'name': '',
                'address': '',
                'phone': '',
                'schema': self.extract_schema_data(soup)
            }
            
            # Get title
            title = soup.find('title')
            if title:
                website_data['name'] = title.get_text(strip=True)
            
            # Get all visible text (remove scripts and styles)
            for script in soup(["script", "style"]):
                script.decompose()
            
            page_text = soup.get_text()
            
            # Try to get address from schema.org first
            if website_data['schema'].get('address'):
                website_data['address'] = website_data['schema']['address']
            else:
                # Fallback to more robust regex for address
                address_pattern = re.compile(
                    r'(\d+\s+[\w\s.]+(?:Rd|St|Ave|Blvd|Pkwy|Pl|Dr|Ln|Ct)\.?)'  # Street Address
                    r'[^a-zA-Z]*'  # Optional non-alpha characters
                    r'((?:[A-Za-z\s]+)\s*,\s*([A-Z]{2})\s*(\d{5})'  # City, ST ZIP
                    r'|' # OR
                    r'([A-Za-z\s]+)\s*,\s*([A-Z]{2})\s*(\d{5}))',  # City, ST ZIP (no street)
                    re.MULTILINE
                )
                
                # Check for a match
                match = address_pattern.search(page_text)
                if match:
                    # Take the full matched string as the address
                    full_address = match.group(0).strip()
                    # Clean up extra spaces, newlines, and trailing commas
                    full_address = re.sub(r'[\r\n]+', ' ', full_address)
                    full_address = re.sub(r'\s{2,}', ' ', full_address)
                    full_address = full_address.strip(', ')
                    website_data['address'] = full_address
                else:
                    # Fallback to simpler city, state, zip match
                    simple_pattern = r'([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5})'
                    simple_matches = re.findall(simple_pattern, page_text)
                    if simple_matches:
                        city, state, zip_code = simple_matches[0]
                        website_data['address'] = f"{city.strip()}, {state} {zip_code}"
            
            # Try to get phone from schema.org first
            if website_data['schema'].get('phone'):
                website_data['phone'] = website_data['schema']['phone']
            else:
                # Fallback to more robust regex for phone
                phone_patterns = [
                    # Matches (123) 456-7890 or (123)456-7890
                    r'\(?\d{3}\)?[-\s.]?\d{3}[-\s.]?\d{4}',
                    # Matches 123-456-7890
                    r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',
                    # Matches 1234567890
                    r'\b\d{10}\b',
                    # Matches tel: links
                    r'href=["\']?tel:([^"\'>\s]+)'
                ]
                
                for pattern in phone_patterns:
                    match = re.search(pattern, page_text)
                    if match:
                        website_data['phone'] = match.group(0)
                        # Normalize the phone number
                        website_data['phone'] = self.normalize_phone(website_data['phone'])
                        break
            
            print(f"\nFINAL RESULTS:")
            print(f"  Name: {website_data['name']}")
            print(f"  Address: {website_data['address']}")
            print(f"  Phone: {website_data['phone']}")
            print("=" * 50)
            
            return website_data
            
        except (WebDriverException, TimeoutException, NoSuchElementException) as e:
            print(f"EXCEPTION: Selenium Error - {type(e).__name__}: {str(e)}")
            return None
        except Exception as e:
            print(f"EXCEPTION: {type(e).__name__}: {str(e)}")
            return None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                # Force garbage collection
                gc.collect()
    
    def extract_schema_data(self, soup):
        """Extract schema.org LocalBusiness data from page"""
        schema_data = {
            'name': 'Not available',
            'address': 'Not available',
            'phone': 'Not available',
            'formatting_error': ''
        }
        
        try:
            schema_scripts = soup.find_all('script', type='application/ld+json')
            
            for script in schema_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        data = data[0] if data else {}
                    if '@type' in data and 'Business' in str(data['@type']):
                        schema_data['name'] = str(data.get('name', 'Not available'))
                        if 'address' in data:
                            addr = data['address']
                            if isinstance(addr, dict):
                                address_parts = []
                                if 'streetAddress' in addr:
                                    address_parts.append(str(addr['streetAddress']))
                                if 'addressLocality' in addr:
                                    address_parts.append(str(addr['addressLocality']))
                                if 'addressRegion' in addr:
                                    address_parts.append(str(addr['addressRegion']))
                                if 'postalCode' in addr:
                                    address_parts.append(str(addr['postalCode']))
                                schema_data['address'] = ', '.join(address_parts)
                            elif isinstance(addr, str):
                                schema_data['address'] = addr
                        
                        phone_fields = ['telephone', 'telePhone', 'phone']
                        for field in phone_fields:
                            if field in data:
                                schema_data['phone'] = str(data[field])
                                break
                        
                        errors = []
                        if 'telePhone' in data:
                            errors.append("'telePhone' should be 'telephone'")
                        if '@context' not in data:
                            errors.append("Missing @context")
                        if '@type' not in data:
                            errors.append("Missing @type")
                            
                        schema_data['formatting_error'] = '; '.join(errors)
                        
                        break
                        
                except json.JSONDecodeError:
                    schema_data['formatting_error'] = 'Invalid JSON in schema.org script'
                except Exception as e:
                    schema_data['formatting_error'] = f'Error parsing schema: {str(e)}'
                    
        except Exception as e:
            schema_data['formatting_error'] = f'Error extracting schema: {str(e)}'
            
        return schema_data
    
    def normalize_phone_number(self, phone):
        """
        Normalizes phone numbers to a consistent format.
        Removes all non-digit characters and prepends '+1' if it's a 10-digit number.
        """
        if not phone:
            return None
        
        # Remove all non-digits
        digits_only = re.sub(r'\D', '', str(phone))
        
        # If the number is 10 digits long, prepend '+1'
        if len(digits_only) == 10:
            return f'+1{digits_only}'
        
        return phone

    def normalize_address(self, address):
        """
        Normalizes addresses by removing extraneous spaces and commas.
        """
        if not address:
            return None
        
        # Replace multiple spaces with a single space and strip leading/trailing spaces
        normalized = re.sub(r'\s{2,}', ' ', str(address)).strip()
        
        # Remove leading/trailing commas
        normalized = normalized.strip(',').strip()
        
        # Replace any instances of multiple commas with a single comma
        normalized = re.sub(r',{2,}', ',', normalized)
        
        return normalized
        
    def determine_match_status(self, gbp_data, website_data, yext_data):
        """Determine the match status and action needed with detailed comparison"""
        issues = []
        actions = []
        
        # Handle "No GBP Match" scenario
        if gbp_data.get('status') in ['no_match', 'no_results', 'error']:
            match_status = 'No GBP Match'
            action_needed = ""
            if gbp_data.get('status') == 'no_results':
                action_needed = "No results returned from Places API for this search query."
            elif gbp_data.get('status') == 'no_match' and gbp_data.get('closest_match_name'):
                action_needed = f"There was no close match to a GBP entry, the closest match was '{gbp_data['closest_match_name']}'."
            else:
                action_needed = "Manual review required: No close Google Business Profile match found."
            
            return match_status, action_needed
        
        # Normalize all data for comparison
        gbp_name = gbp_data.get('name', '') if gbp_data else ''
        gbp_address = self.normalize_address(gbp_data.get('address', '')) if gbp_data else ''
        gbp_phone = self.normalize_phone_number(gbp_data.get('phone', '')) if gbp_data else ''
        
        website_address = self.normalize_address(website_data.get('address', '')) if website_data else ''
        website_phone = self.normalize_phone_number(website_data.get('phone', '')) if website_data else ''
        
        yext_address = self.normalize_address(yext_data.get('address', '')) if yext_data else ''
        yext_phone = self.normalize_phone_number(yext_data.get('phone', '')) if yext_data else ''
        
        schema_address = self.normalize_address(website_data.get('schema', {}).get('address', '')) if website_data else ''
        schema_phone = self.normalize_phone_number(website_data.get('schema', {}).get('phone', '')) if website_data else ''
        
        # Define 'missing' strings
        missing_address = ['Not available (no GBP website found)', 'Website scraping failed', 'Not available (no match)', 'Not available', '']
        missing_phone = ['Not available (no GBP website found)', 'Website scraping failed', 'Not available (no match)', 'Not available', '']
        
        # Compare Website data with GBP data
        if website_data:
            if website_address in missing_address:
                issues.append('Update Website Address')
                actions.append(f"Website Address Missing - GBP: '{gbp_address}'")
            elif gbp_address and website_address and gbp_address != website_address:
                issues.append('Update Website Address')
                actions.append(f"Address Mismatch - GBP: '{gbp_address}' vs Website: '{website_address}'")
                
            if website_phone in missing_phone:
                issues.append('Update Website Phone')
                actions.append(f"Website Phone Missing - GBP: '{gbp_phone}'")
            elif gbp_phone and website_phone and gbp_phone != website_phone:
                issues.append('Update Website Phone')
                actions.append(f"Phone Mismatch - GBP: '{gbp_phone}' vs Website: '{website_phone}'")
                
            # Check schema
            if website_data.get('schema'):
                if website_data['schema'].get('formatting_error') not in ['Not available (no GBP website found)', 'Website scraping failed', 'Not available'] and website_data['schema'].get('formatting_error') != '':
                    issues.append('Fix Schema Formatting')
                    actions.append(f"Fix schema.org formatting errors: {website_data['schema'].get('formatting_error', 'No errors specified.')}")
                
                if schema_address in missing_address:
                    issues.append('Update Schema Address')
                    actions.append(f"Schema Address Missing - GBP: '{gbp_address}'")
                elif gbp_address and schema_address and gbp_address != schema_address:
                    issues.append('Update Schema Address')
                    actions.append(f"Schema Address Mismatch - GBP: '{gbp_address}' vs Schema: '{schema_address}'")
                    
                if schema_phone in missing_phone:
                    issues.append('Update Schema Phone')
                    actions.append(f"Schema Phone Missing - GBP: '{gbp_phone}'")
                elif gbp_phone and schema_phone and gbp_phone != schema_phone:
                    issues.append('Update Schema Phone')
                    actions.append(f"Schema Phone Mismatch - GBP: '{gbp_phone}' vs Schema: '{schema_phone}'")
        
        # Compare Yext data with GBP data
        if yext_data and gbp_data:
            if yext_address in missing_address:
                issues.append('Update Yext Address')
                actions.append(f"Yext Address Missing - GBP: '{gbp_address}'")
            elif gbp_address and yext_address and gbp_address != yext_address:
                issues.append('Update Yext Address')
                actions.append(f"Yext Address Mismatch - GBP: '{gbp_address}' vs Yext: '{yext_address}'")
            
            if yext_phone in missing_phone:
                issues.append('Update Yext Phone')
                actions.append(f"Yext Phone Missing - GBP: '{gbp_phone}'")
            elif gbp_phone and yext_phone and gbp_phone != yext_phone:
                issues.append('Update Yext Phone')
                actions.append(f"Yext Phone Mismatch - GBP: '{gbp_phone}' vs Yext: '{yext_phone}'")
                
        if not issues:
            return "All Good", "All NAP information is consistent."
        
        match_status = " / ".join(sorted(list(set(issues))))
        action_needed = " | ".join(sorted(list(set(actions))))
        
        return match_status, action_needed

    def process_business(self, business_name):
        """Processes a single business for NAP consistency"""
        print(f"\nProcessing '{business_name}'...")
        
        # Initialize data holders with "Not available" messages
        gbp_data = {
            'name': 'Not available (no close match)',
            'address': 'Not available (no close match)',
            'website': 'Not available (no close match)',
            'phone': 'Not available (no close match)',
            'status': 'initial'
        }
        yext_data = {
            'name': 'Not available (no match)',
            'address': 'Not available (no match)',
            'phone': 'Not available (no match)'
        }
        website_data = {
            'name': 'Not available (no GBP website found)',
            'address': 'Not available (no GBP website found)',
            'phone': 'Not available (no GBP website found)',
            'schema': {
                'name': 'Not available',
                'address': 'Not available',
                'phone': 'Not available',
                'formatting_error': ''
            }
        }
        
        # Search for the business on Google Business Profile (GBP)
        raw_gbp_data = self.search_google_place(business_name)
        
        # Update gbp_data based on search results
        if raw_gbp_data.get('status') == 'match':
            gbp_data = {
                'name': raw_gbp_data.get('name', 'Not available'),
                'address': raw_gbp_data.get('address', 'Not available'),
                'website': raw_gbp_data.get('website', 'Not available'),
                'phone': raw_gbp_data.get('phone', 'Not available'),
                'status': 'match'
            }
            gbp_website_url = gbp_data.get('website', None)
        else:
            gbp_data['status'] = raw_gbp_data.get('status')
            gbp_data['closest_match_name'] = raw_gbp_data.get('closest_match_name')
            gbp_website_url = None
        
        print(f"GBP Status: {gbp_data.get('status')}")
        
        # Search Yext regardless of GBP result
        raw_yext_data = self.search_yext(business_name, gbp_data.get('website'))
        if raw_yext_data:
            yext_data = {
                'name': raw_yext_data.get('name', 'Not available'),
                'address': raw_yext_data.get('address', 'Not available'),
                'phone': raw_yext_data.get('phone', 'Not available'),
            }
            print(f"Yext Found: {yext_data['name']}")
        else:
            print("Yext: No match found.")
            
        # Scrape website information if a GBP website URL was found
        if gbp_website_url and gbp_website_url != 'Not available (no close match)':
            raw_website_data = self.scrape_website_info(gbp_website_url)
            if raw_website_data:
                # Update website_data with scraped info
                website_data = {
                    'name': raw_website_data.get('name', 'Not available'),
                    'address': raw_website_data.get('address', 'Not available'),
                    'phone': raw_website_data.get('phone', 'Not available'),
                    'schema': {
                        'name': raw_website_data.get('schema', {}).get('name', 'Not available'),
                        'address': raw_website_data.get('schema', {}).get('address', 'Not available'),
                        'phone': raw_website_data.get('schema', {}).get('phone', 'Not available'),
                        'formatting_error': raw_website_data.get('schema', {}).get('formatting_error', 'Not available')
                    }
                }
            else:
                # If scrape failed, explicitly populate with failure messages
                website_data = {
                    'name': 'Website scraping failed',
                    'address': 'Website scraping failed',
                    'phone': 'Website scraping failed',
                    'schema': {
                        'name': 'Website scraping failed',
                        'address': 'Website scraping failed',
                        'phone': 'Website scraping failed',
                        'formatting_error': 'Website scraping failed'
                    }
                }
        else:
            print("Skipping website scrape as no GBP website URL was found.")
            
        # Determine the match status and actions needed
        match_status, action_needed = self.determine_match_status(gbp_data, website_data, yext_data)
        
        # Record the results
        self.results.append({
            'Business Name Input': business_name,
            'GBP Business Name': gbp_data['name'],
            'GBP Address': gbp_data['address'],
            'GBP Website URL': gbp_data['website'],
            'GBP Phone Number': gbp_data['phone'],
            'Website Name': website_data['name'],
            'Website Address': website_data['address'],
            'Website Phone Number': website_data['phone'],
            'Yext Name': yext_data['name'],
            'Yext Address': yext_data['address'],
            'Yext Phone Number': yext_data['phone'],
            'Schema Name': website_data['schema']['name'],
            'Schema Address': website_data['schema']['address'],
            'Schema Phone Number': website_data['schema']['phone'],
            'Match Status': match_status,
            'Action Needed': action_needed
        })

    def normalize_phone(self, phone):
        """
        Normalizes phone numbers to a consistent format.
        Removes all non-digit characters and prepends '+1' for a 10-digit number.
        This is a temporary alias for normalize_phone_number to avoid changing the caller.
        """
        return self.normalize_phone_number(phone)
    
    def process_input_file(self, file_path):
        """
        Reads a list of business names from an Excel file and processes each one.
        """
        try:
            # Read business names from the Excel file
            df = pd.read_excel(file_path)
            business_names = df[df.columns[0]].tolist()
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
            return
        except Exception as e:
            print(f"Error reading Excel file: {str(e)}")
            return
        
        print(f"Found {len(business_names)} businesses to process")
        
        # Process each business
        for i, business_name in enumerate(business_names):
            print(f"\nProcessing {i+1}/{len(business_names)}")
            self.process_business(str(business_name))
        
        # Save results
        self.save_results()
    
    def save_results(self):
        """Save results to CSV file"""
        output_file = 'nap_audit_results.csv'
        
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


def main():
    """Main function to run the NAP audit"""
    print("NAP Audit Application")
    print("=" * 50)
    
    # Initialize auditor
    auditor = NAPAuditor()
    
    # Process the input file
    input_file = 'business_names_veryshort.xlsx'  # Change this to your input file name
    
    print(f"Reading business names from {input_file}...")
    auditor.process_input_file(input_file)
    #small change
    print("\nProcessing complete.")

if __name__ == '__main__':
    main()