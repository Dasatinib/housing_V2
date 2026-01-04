import json
import glob
import os
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def get_listing_urls(f_mains):
    """
    Extracts all unique listing URLs from the HTML files in the specified directory.
    """
    urls = set()
    files = glob.glob(os.path.join(f_mains, '*'))
    
    for filepath in files:
        if not os.path.isfile(filepath) or os.path.basename(filepath).startswith('.'):
            continue
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
                script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
                if script_tag and script_tag.string:
                    data = json.loads(script_tag.string)
                    cache = data.get('props', {}).get('pageProps', {}).get('apolloCache', {})
                    for key, item in cache.items():
                        if key.startswith('Advert:'):
                            uri = item.get('uri')
                            if uri:
                                urls.add(f"https://www.bezrealitky.cz/nemovitosti-byty-domy/{uri}")
        except Exception:
            # Silent skip for non-parseable files
            pass
            
    return list(urls)

def extract_detail(f_listings, process_today_only):
    # Skip directories or hidden files

    files = glob.glob(os.path.join(f_listings, '*'))
    files = [file for file in files
             if os.path.isfile(file)
             and not os.path.basename(file).startswith('.')]
    
    if process_today_only:
        files = [file for file in files if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")]
        
    print(f"Scanning {len(files)} files in {f_listings}...")

    data = []
    for file in files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
                script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
                if not script_tag or not script_tag.string:
                    continue
                

                json_data = json.loads(script_tag.string)
                props = json_data.get('props', {}).get('pageProps', {})
                advert = props.get('origAdvert')
                
                if not advert:
                    continue
                    
                # Extract basic identifiers
                listing_id = advert.get('id')
                uri = advert.get('uri')
                url = f"https://www.bezrealitky.cz/nemovitosti-byty-domy/{uri}" if uri else None
                
                # Financials
                price_rent = advert.get('price')
                utility_charges = advert.get('utilityCharges')
                service_charges = advert.get('serviceCharges')
                fee = advert.get('fee') # Provize
                
                # Property Details
                description = advert.get('description')
                surface = advert.get('surface')
                
                disposition_raw = advert.get('disposition')
                disposition = disposition_raw
                if disposition_raw and disposition_raw.startswith('DISP_'):
                     disposition = disposition_raw.replace('DISP_', '').replace('_', '+').replace('KK', 'kk')
                
                address = advert.get('address')
                
                # Tags / Highlights
                tags = advert.get('tags', [])
                tags_str = ", ".join(tags) if tags else ""
                
                # Availability
                available_ts = advert.get('availableFrom')
                available_date = datetime.fromtimestamp(available_ts).strftime('%Y-%m-%d') if available_ts else None
                
                # Coordinates
                gps = advert.get('gps', {})
                lat = gps.get('lat') if gps else None
                lng = gps.get('lng') if gps else None

                res = {
                    'ID': listing_id,
                    'URL': url,
                    'Address': address,
                    'Disposition': disposition,
                    'Area (m2)': surface,
                    'Rent (CZK)': price_rent,
                    'Utilities (CZK)': utility_charges,
                    'Services (CZK)': service_charges,
                    'Fee': fee,
                    'Available From': available_date,
                    'Tags': tags_str,
                    'Description': description,
                    'Latitude': lat,
                    'Longitude': lng,
                    'Source File': os.path.basename(file)
                }

                if res:
                    data.append(res)
                else:
                    print("No valid listing details found.")
                    continue

        except Exception as e:
            # Fail silently for individual file errors to keep processing others
            # print(f"Error parsing {file}: {e}")
            continue
        
    if data:
        df = pd.DataFrame(data)
        print(f"Successfully extracted {len(df)} detailed records.")

        output_file = 'listings_details.csv'
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        
        # Show a snippet
        print(df[['ID', 'Disposition', 'Rent (CZK)', 'Description']].head())
    else:
        print("No data extracted.")
