import requests
import json
import csv
from datetime import datetime
from collections import defaultdict
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import re
from urllib.parse import urlparse

# Function to extract location from URL
def extract_location_from_url(url):
    """Extract city/state/location info from URL"""
    if not url:
        return ""
    
    # Parse the URL
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        # Remove common words and clean up
        path_parts = path.strip('/').split('/')
        
        # Skip common directory names
        skip_words = {'offices', 'locations', 'branches', 'stores', 'office', 
                     'location', 'branch', 'store', 'home', 'index', 'html', 
                     'php', 'aspx', 'htm', 'shtml'}
        
        location_parts = []
        for part in path_parts:
            # Clean up the part
            cleaned = part.replace('-', ' ').replace('_', ' ').strip()
            
            # Skip if it's a common word or too short
            if cleaned and cleaned not in skip_words and len(cleaned) > 2:
                # Skip if it looks like a file extension
                if not cleaned.startswith('.') and '.' not in cleaned[-5:]:
                    location_parts.append(cleaned.title())
        
        return ' '.join(location_parts)
    except:
        return ""

# Function to create enhanced entity name
def create_enhanced_entity_name(entity_name, url, address_city=None, address_state=None):
    """Create a more descriptive entity name using URL and/or address info"""
    location_from_url = extract_location_from_url(url)
    
    # If we have address info, prefer that
    location_parts = []
    if address_city:
        location_parts.append(address_city)
    if address_state:
        location_parts.append(address_state)
    
    # If no address info or minimal info, use URL-derived location
    if not location_parts and location_from_url:
        location_parts = [location_from_url]
    elif len(location_parts) < 2 and location_from_url:
        # Add URL info if it provides additional detail
        url_parts = location_from_url.split()
        for part in url_parts:
            if part not in ' '.join(location_parts):
                location_parts.append(part)
    
    # Create enhanced name
    if location_parts:
        return f"{entity_name} {' '.join(location_parts)}"
    else:
        return entity_name

# API configuration
api_key = "7a2c551e133734c96da4f995aa5117df"
base_url = "https://api.yext.com/v2/accounts"

# Create a session with retry strategy
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Function to make API calls with timeout and error handling
def make_api_request(url, params, timeout=30):
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}/{max_attempts}. Waiting 5 seconds...")
            time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            print(f"  Connection error on attempt {attempt + 1}/{max_attempts}: {str(e)}")
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(5)
            else:
                raise
    
    raise Exception(f"Failed to get response after {max_attempts} attempts")

# Request all accounts
print("Fetching accounts...")
try:
    accounts_response = make_api_request(base_url, params={"api_key": api_key, "limit": 50, "v": "20230821"})
    accounts_data = accounts_response["response"]["accounts"]
except Exception as e:
    print(f"Failed to fetch accounts: {str(e)}")
    print("\nPossible issues:")
    print("1. Check your internet connection")
    print("2. Verify the API key is correct")
    print("3. Ensure the Yext API is accessible from your network")
    exit(1)

# Store all data
all_entities_with_full_account_data = []
duplicate_tracking = defaultdict(list)

print(f"Found {len(accounts_data)} accounts\n")

# First pass - collect all account fields
all_account_fields = set()
for account in accounts_data:
    all_account_fields.update(account.keys())

# Process each account
for account_idx, account in enumerate(accounts_data, 1):
    account_name = account.get('accountName', 'Unnamed')
    account_id = account['accountId']
    print(f"Processing account {account_idx}/{len(accounts_data)}: {account_name} (ID: {account_id})")
    
    limit = 50
    pageToken = None
    entity_count = 0
    page_count = 0
    
    while True:
        page_count += 1
        params = {"api_key": api_key, "limit": limit, "v": "20230821"}
        if pageToken:
            params["pageToken"] = pageToken

        try:
            print(f"  Fetching page {page_count}...")
            response_data = make_api_request(
                f"{base_url}/{account_id}/entities",
                params=params,
                timeout=60  # Longer timeout for entity requests
            )
        except Exception as e:
            print(f"  Error fetching entities for account {account_id}: {str(e)}")
            print(f"  Skipping remaining entities for this account")
            break
        
        if "response" not in response_data or "entities" not in response_data["response"]:
            print(f"  Warning: No entities found or error in response for account {account_id}")
            break
            
        entities = response_data["response"]["entities"]
        entity_count += len(entities)
        print(f"  Processing {len(entities)} entities...")

        for entity in entities:
            # Create a combined record with all account data prefixed with 'account_'
            combined_record = {}
            
            # Add all account fields with 'account_' prefix
            for key, value in account.items():
                if isinstance(value, (dict, list)):
                    combined_record[f'account_{key}'] = json.dumps(value)
                else:
                    combined_record[f'account_{key}'] = value
            
            # Add all entity fields
            for key, value in entity.items():
                combined_record[f'entity_{key}'] = value
            
            # Extract URL if it exists
            entity_url = ""
            if 'websiteUrl' in entity:
                if isinstance(entity['websiteUrl'], dict):
                    entity_url = entity['websiteUrl'].get('url', '')
                else:
                    entity_url = entity.get('websiteUrl', '')
            
            # Add some flattened versions of commonly needed fields
            entity_city = ""
            entity_state = ""
            if 'address' in entity:
                addr = entity['address']
                entity_city = addr.get('city', '')
                entity_state = addr.get('region', '')
                combined_record['entity_address_full'] = f"{addr.get('line1', '')} {addr.get('line2', '')} {entity_city} {entity_state} {addr.get('postalCode', '')}".strip()
                combined_record['entity_address_line1'] = addr.get('line1', '')
                combined_record['entity_address_city'] = entity_city
                combined_record['entity_address_region'] = entity_state
                combined_record['entity_address_postalCode'] = addr.get('postalCode', '')
            
            # Get original and enhanced entity names
            entity_name = entity.get('name', '')
            enhanced_name = create_enhanced_entity_name(entity_name, entity_url, entity_city, entity_state)
            
            # Add both original and enhanced names to the record
            combined_record['entity_name_original'] = entity_name
            combined_record['entity_name_enhanced'] = enhanced_name
            combined_record['entity_url_extracted'] = entity_url
            
            # Track duplicates
            entity_name = entity.get('name', '')
            if entity_name:
                duplicate_tracking[entity_name].append({
                    'account_id': account_id,
                    'account_name': account_name,
                    'entity_id': entity.get('id', 'N/A'),
                    'address': combined_record.get('entity_address_full', 'No address'),
                    'full_record': combined_record
                })
            
            all_entities_with_full_account_data.append(combined_record)

        # Check for more pages
        if "pageToken" in response_data.get("response", {}):
            pageToken = response_data["response"]["pageToken"]
            time.sleep(0.5)  # Small delay between pages to avoid rate limiting
        else:
            print(f"  Completed: Found {entity_count} entities in this account")
            break

print(f"\nTotal entities collected: {len(all_entities_with_full_account_data)}")

# Determine all fields
all_fields = set()
for record in all_entities_with_full_account_data:
    all_fields.update(record.keys())

# Define priority fields
priority_fields = [
    'account_accountName', 'account_accountId', 'entity_id', 'entity_name',
    'entity_name_original', 'entity_name_enhanced', 'entity_url_extracted',
    'entity_address_full', 'entity_address_line1', 'entity_address_city', 
    'entity_address_region', 'entity_address_postalCode', 'entity_mainPhone',
    'entity_websiteUrl', 'entity_emails', 'entity_description'
]

# Arrange fields
ordered_fields = [f for f in priority_fields if f in all_fields] + sorted([f for f in all_fields if f not in priority_fields])

# Write main CSV with all data
csv_filename = "yext_complete_account_entity_data.csv"
print(f"\nWriting main data file...")
with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=ordered_fields, extrasaction='ignore')
    writer.writeheader()
    
    for record in all_entities_with_full_account_data:
        # Flatten any remaining nested structures
        flattened_record = {}
        for key, value in record.items():
            if isinstance(value, dict):
                flattened_record[key] = json.dumps(value)
            elif isinstance(value, list):
                flattened_record[key] = ', '.join(str(v) for v in value)
            else:
                flattened_record[key] = value
        
        writer.writerow(flattened_record)

print(f"Complete data CSV created: {csv_filename}")

# Create detailed duplicate analysis
duplicates_filename = "yext_detailed_duplicate_analysis.csv"
print(f"\nCreating duplicate analysis...")
with open(duplicates_filename, "w", newline="", encoding="utf-8") as csvfile:
    duplicate_fields = [
        'entity_name', 'occurrence_count', 'account_ids', 'account_names', 
        'entity_ids', 'addresses', 'phones', 'websites', 'unique_accounts_count'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=duplicate_fields)
    writer.writeheader()
    
    # Analyze duplicates
    for name, occurrences in duplicate_tracking.items():
        if len(occurrences) > 1:
            account_ids = [o['account_id'] for o in occurrences]
            unique_accounts = set(account_ids)
            
            phones = []
            websites = []
            for occ in occurrences:
                record = occ['full_record']
                phone = record.get('entity_mainPhone', '')
                if phone:
                    phones.append(phone)
                    
                # Try different website field names
                website = record.get('entity_websiteUrl', '')
                
                # Handle different website data formats
                if isinstance(website, dict):
                    website = website.get('url', '')
                elif not website and 'entity_websiteUrl' in record:
                    try:
                        website_data = json.loads(record['entity_websiteUrl'])
                        if isinstance(website_data, dict):
                            website = website_data.get('url', '')
                        else:
                            website = str(website_data)
                    except:
                        pass
                
                # Only add non-empty string websites
                if website and isinstance(website, str):
                    websites.append(website)
            
            writer.writerow({
                'entity_name': name,
                'occurrence_count': len(occurrences),
                'account_ids': '; '.join(account_ids),
                'account_names': '; '.join([o['account_name'] for o in occurrences]),
                'entity_ids': '; '.join([o['entity_id'] for o in occurrences]),
                'addresses': ' | '.join([o['address'] for o in occurrences]),
                'phones': '; '.join(list(set(p for p in phones if isinstance(p, str)))),  # unique phones
                'websites': '; '.join(list(set(w for w in websites if isinstance(w, str)))),  # unique websites
                'unique_accounts_count': len(unique_accounts)
            })

# Create a pivot summary showing duplicates by account
pivot_filename = "yext_duplicate_pivot_by_account.csv"
print(f"Creating account summary...")
account_duplicate_summary = defaultdict(lambda: {'total_entities': 0, 'duplicate_entities': 0, 'unique_names': set()})

for record in all_entities_with_full_account_data:
    account_id = record.get('account_accountId', 'Unknown')
    account_name = record.get('account_accountName', 'Unknown')
    entity_name = record.get('entity_name', '')
    
    if entity_name:
        account_duplicate_summary[f"{account_name} ({account_id})"]["total_entities"] += 1
        account_duplicate_summary[f"{account_name} ({account_id})"]["unique_names"].add(entity_name)
        
        if len(duplicate_tracking[entity_name]) > 1:
            account_duplicate_summary[f"{account_name} ({account_id})"]["duplicate_entities"] += 1

with open(pivot_filename, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Account (ID)", "Total Entities", "Unique Names", "Entities with Duplicate Names", "Duplicate Rate"])
    
    for account_key, stats in sorted(account_duplicate_summary.items()):
        total = stats['total_entities']
        duplicates = stats['duplicate_entities']
        unique = len(stats['unique_names'])
        dup_rate = f"{(duplicates/total*100):.1f}%" if total > 0 else "0%"
        
        writer.writerow([account_key, total, unique, duplicates, dup_rate])

# Create enhanced names analysis file
enhanced_names_filename = "yext_enhanced_names_for_duplicates.csv"
print(f"\nCreating enhanced names analysis...")
with open(enhanced_names_filename, "w", newline="", encoding="utf-8") as csvfile:
    enhanced_fields = [
        'original_name', 'times_duplicated', 'enhanced_names', 'sample_urls', 'locations'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=enhanced_fields)
    writer.writeheader()
    
    # Find all duplicated names and show their enhanced versions
    for name, occurrences in duplicate_tracking.items():
        if len(occurrences) > 1:
            enhanced_versions = []
            urls = []
            locations = []
            
            for occ in occurrences:
                record = occ['full_record']
                enhanced = record.get('entity_name_enhanced', name)
                url = record.get('entity_url_extracted', '')
                location = f"{record.get('entity_address_city', '')} {record.get('entity_address_region', '')}".strip()
                
                if enhanced not in enhanced_versions:
                    enhanced_versions.append(enhanced)
                if url and url not in urls:
                    urls.append(url)
                if location and location not in locations:
                    locations.append(location)
            
            writer.writerow({
                'original_name': name,
                'times_duplicated': len(occurrences),
                'enhanced_names': ' | '.join(enhanced_versions[:5]),  # Show first 5
                'sample_urls': ' | '.join(urls[:3]),  # Show first 3 URLs
                'locations': ' | '.join(locations[:5])  # Show first 5 locations
            })

# Print comprehensive summary
print(f"\n{'='*80}")
print(f"DUPLICATE ANALYSIS SUMMARY")
print(f"{'='*80}")
print(f"Total accounts processed: {len(accounts_data)}")
print(f"Total entities: {len(all_entities_with_full_account_data)}")
print(f"Unique entity names: {len(duplicate_tracking)}")
print(f"Entity names appearing more than once: {sum(1 for occurrences in duplicate_tracking.values() if len(occurrences) > 1)}")
print(f"\nFiles created:")
print(f"1. {csv_filename} - Complete data dump with all account and entity fields")
print(f"2. {duplicates_filename} - Detailed analysis of each duplicated entity name")
print(f"3. {pivot_filename} - Summary by account showing duplicate rates")
print(f"4. {enhanced_names_filename} - Enhanced names for duplicated entities")
print(f"{'='*80}")

# Show which names appear across multiple accounts
cross_account_dupes = [(name, len(occs), len(set(o['account_id'] for o in occs))) 
                       for name, occs in duplicate_tracking.items() 
                       if len(set(o['account_id'] for o in occs)) > 1]

if cross_account_dupes:
    print(f"\nEntities appearing in MULTIPLE accounts ({len(cross_account_dupes)} found):")
    sorted_cross = sorted(cross_account_dupes, key=lambda x: x[2], reverse=True)
    for name, total_count, account_count in sorted_cross[:10]:
        print(f"  '{name}': {total_count} total occurrences across {account_count} different accounts")

print(f"\nCheck the CSV files for complete details!")