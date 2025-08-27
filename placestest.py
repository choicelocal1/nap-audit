import time
import json
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build

# IMPORTANT: You must have a service account JSON file
# with access to the Google Places API.
SERVICE_ACCOUNT_FILE = 'nightwatch-302222-b4d76c4c4d34.json'

class GPBSearchTester:
    """
    A simple class to perform and print results from the
    Google Places API's Text Search feature.
    """
    def __init__(self):
        """Initializes the Places API service."""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            self.google_service = build('places', 'v1', credentials=credentials)
        except Exception as e:
            print(f"ERROR: Could not initialize Google Places API. Make sure your "
                  f"service account file is correct and has the necessary permissions.")
            print(f"Details: {e}")
            self.google_service = None

    def search_and_print(self, business_name):
        """
        Searches for a business and prints the full JSON response.

        Args:
            business_name (str): The business name to search for.
        """
        if not self.google_service:
            print("API service is not initialized. Exiting.")
            return

        print(f"Searching Google Places for: '{business_name}'")
        print("=" * 50)
        
        try:
            # Construct the search request.
            # We will ask for a few more results (e.g., 5) to see if the match
            # is ranked lower.
            request_body = {
                'textQuery': business_name,
                'maxResultCount': 5,
                'languageCode': 'en-US',
                'regionCode': 'US'
            }
            
            # Use a field mask to get the data we need.
            request = self.google_service.places().searchText(body=request_body)
            original_uri = request.uri
            if '?' in original_uri:
                request.uri = original_uri + '&fields=places.id,places.displayName,places.formattedAddress'
            else:
                request.uri = original_uri + '?fields=places.id,places.displayName,places.formattedAddress'

            # Execute the request and get the response.
            response = request.execute()

            # Print the raw JSON response in a readable format.
            print("Raw JSON Response from Google Places API:")
            print(json.dumps(response, indent=2))
            
            print("\n" + "=" * 50)
            print("Analysis:")
            
            # Check for places and analyze the display names.
            places = response.get('places', [])
            if not places:
                print(f"No places were found for the query: '{business_name}'")
            else:
                for i, place in enumerate(places, 1):
                    display_name = place.get('displayName', {}).get('text', 'N/A')
                    address = place.get('formattedAddress', 'N/A')
                    
                    # Print the key information for easier reading.
                    print(f"\nResult {i}:")
                    print(f"  Display Name: {display_name}")
                    print(f"  Address: {address}")
                    
                    # A basic check for the special character.
                    if '°' in display_name and '360 Painting' in business_name:
                        print("  Note: Found the '°' symbol in the display name.")
            
        except Exception as e:
            print(f"An unexpected error occurred during the search: {e}")

if __name__ == "__main__":
    tester = GPBSearchTester()
    
    # This is the specific business name you asked about.
    target_business_name = "360 Painting of North Georgia"
    
    tester.search_and_print(target_business_name)