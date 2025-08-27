import csv
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import HttpRequest
import pandas as pd

# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'nightwatch-302222-b4d76c4c4d34.json'

# Initialize the Google Places API client
def initialize_places_api():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    
    # Build the Places API service
    service = build('places', 'v1', credentials=credentials)
    return service

def search_place(service, query):
    """
    Search for a place using the Google Places API (New)
    """
    try:
        # Create the search request body
        request_body = {
            'textQuery': query,
            'maxResultCount': 1,  # Get only the top result
            'languageCode': 'en-US',
            'regionCode': 'US'
        }
        
        # Create the request
        request = service.places().searchText(body=request_body)
        
        # Modify the URI to include the fieldMask as a query parameter
        original_uri = request.uri
        if '?' in original_uri:
            request.uri = original_uri + '&fields=places.id,places.displayName,places.formattedAddress,places.shortFormattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri'
        else:
            request.uri = original_uri + '?fields=places.id,places.displayName,places.formattedAddress,places.shortFormattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri'
        
        # Execute the request
        response = request.execute()
        
        if 'places' in response and len(response['places']) > 0:
            place = response['places'][0]
            
            # Extract the place details
            display_name = place.get('displayName', {}).get('text', 'N/A')
            
            # Get formatted address
            formatted_address = place.get('formattedAddress', 'N/A')
            if formatted_address == 'N/A':
                # Try short formatted address as fallback
                formatted_address = place.get('shortFormattedAddress', 'N/A')
            
            # Get phone number
            phone_number = place.get('nationalPhoneNumber', 'N/A')
            if phone_number == 'N/A':
                # Try international phone number as fallback
                phone_number = place.get('internationalPhoneNumber', 'N/A')
            
            # Get website URL
            website_url = place.get('websiteUri', 'N/A')
            
            return {
                'found': True,
                'name': display_name,
                'address': formatted_address,
                'phone': phone_number,
                'website': website_url
            }
        else:
            return {
                'found': False,
                'name': 'Not Found',
                'address': 'Not Found',
                'phone': 'Not Found',
                'website': 'Not Found'
            }
            
    except Exception as e:
        print(f"Error searching for {query}: {str(e)}")
        return {
            'found': False,
            'name': f'Error: {str(e)}',
            'address': 'Error',
            'phone': 'Error',
            'website': 'Error'
        }

def main():
    # Read the list of Home Helpers locations from the file
    with open('paste.txt', 'r') as file:
        locations = [line.strip() for line in file if line.strip()]
    
    # Initialize the API service
    print("Initializing Google Places API...")
    service = initialize_places_api()
    
    # Prepare results list
    results = []
    
    # Process each location
    print(f"Processing {len(locations)} locations...")
    for i, location in enumerate(locations):
        print(f"Processing {i+1}/{len(locations)}: {location}")
        
        # Search for the place
        result = search_place(service, location)
        
        # Add to results
        results.append({
            'Original Name': location,
            'Returned Name': result['name'],
            'Returned Address': result['address'],
            'Phone Number': result['phone'],
            'Website': result['website']
        })
        
        # Rate limiting - Google Places API has quotas
        # Add a small delay between requests to avoid hitting rate limits
        time.sleep(0.5)
    
    # Save results to CSV
    output_file = 'home_helpers_addresses.csv'
    print(f"\nSaving results to {output_file}...")
    
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"Done! Results saved to {output_file}")
    
    # Print summary
    found_count = sum(1 for r in results if r['Returned Name'] != 'Not Found' and not r['Returned Name'].startswith('Error'))
    print(f"\nSummary:")
    print(f"Total locations processed: {len(locations)}")
    print(f"Successfully found: {found_count}")
    print(f"Not found or errors: {len(locations) - found_count}")

if __name__ == "__main__":
    main()