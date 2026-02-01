import json
import glob
import os
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def trim_html(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Removes logo, menu, similar listings, and other unnecessary sections from the BeautifulSoup object to reduce file size.
    """
    # Remove Header and Footer
    for tag in soup.select("header, footer"):
        tag.decompose()
        
    # Remove Logo (redundant if header is removed, but kept for safety)
    for logo in soup.select(".Header_headerLogo__4edC_, .Footer_footerLogo__mHj_P"):
        logo.decompose()
        
    # Remove Menu (redundant if header is removed, but kept for safety)
    for nav in soup.find_all("nav"):
        nav.decompose()
        
    # Remove Similar Listings
    for h2 in soup.find_all("h2"):
        if "Podobn" in h2.get_text():
            section = h2.find_parent("section")
            if section:
                section.decompose()

    # Remove "V okolí nemovitosti najdete" (Neighborhood)
    # It seems to be in a section with id="mapa" based on the grep output
    neighborhood_section = soup.find("section", {"id": "mapa"})
    if neighborhood_section:
        neighborhood_section.decompose()
        
    # Remove "Rádi vám poradíme" (Contact Box)
    # Based on grep, it seems to be inside a div with class starting with ContactBox
    for contact_box in soup.select("div[class*='ContactBox']"):
        # The grep output showed it inside Footer_footerSideContent, 
        # but if we removed footer, we might have got it. 
        # However, checking if it exists elsewhere or if the footer removal missed it.
        # It's safer to target the specific class.
        contact_box.decompose()

    # Remove Promotional Cards
    for promo in soup.select("div[class*='PromoCard']"):
        promo.decompose()

    # Remove PWA/Apple icons and splash screens
    for link in soup.select("link[rel*='apple-touch-']"):
        link.decompose()
        
    # Remove Cookie/Consent banners if any
    for toast in soup.select(".toast-container"):
        toast.decompose()

    # Remove SVG icons (saves space, data is in text)
    for tag in soup.find_all("svg"):
        tag.decompose()
        
    # Remove noscript tags (usually tracking)
    for tag in soup.find_all("noscript"):
        tag.decompose()
        
    # Remove style tags (CSS not needed for data parsing)
    for tag in soup.find_all("style"):
        tag.decompose()
        
    # Remove all script tags EXCEPT __NEXT_DATA__
    for script in soup.find_all("script"):
        if script.get("id") != "__NEXT_DATA__":
            script.decompose()

    # Remove all link tags (stylesheets, preloads, icons)
    for link in soup.find_all("link"):
        link.decompose()

    # Remove all style tags (inline CSS)
    for style in soup.find_all("style"):
        style.decompose()

    # Remove style attributes from ALL tags (removes inline CSS and large Base64 images)
    for tag in soup.find_all(True):
        if tag.has_attr("style"):
            del tag["style"]

    # Remove srcset/imagesrcset from images
    for img in soup.find_all("img"):
        if img.has_attr("srcset"):
            del img["srcset"]
        if img.has_attr("imagesrcset"):
            del img["imagesrcset"]

    return soup

def get_listing_urls(f_mains) -> list:
    """
    Extracts all unique listing URLs from the HTML files in the specified directory.
    """
    urls = set()
    files = glob.glob(os.path.join(f_mains, '*'))
    files = [file for file in files if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")] # These urls are used for download. I only want to download listings from today's mains.
    
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

def extract_detail(f_listings, process_today_only) -> pd.DataFrame:
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


                if lat is not None:
                    lat = round(lat, 5)
                if lng is not None:
                    lng = round(lng, 5)
                
                filename = os.path.basename(file)
                filename_date = filename.split('_')[0]
                res = {
                    'listing_id': listing_id,
                    'URL': url,
                    'Address': address,
                    'Disposition': disposition,
                    'Area (m2)': surface,
                    'Rent (CZK)': price_rent,
                    'Utilities (CZK)': utility_charges,
                    'Services (CZK)': service_charges,
                    'Fee': fee,
                    'Available from': available_date,
                    'Tags': tags_str,
                    'Description': description,
                    'Latitude': lat,
                    'Longitude': lng,
                    'Source file': filename,
                    'bb_object_name': f"br/htmls/listings/{filename_date}/{listing_id}.html",
                    'Date obtained': datetime.strptime(os.path.basename(file)[:6], '%y%m%d').date()
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

        #        output_file = 'listings_details.csv'
        #        df.to_csv(output_file, index=False)
        #        print(f"Data saved to {output_file}")
        
        # Show a snippet
        print(df[['listing_id', 'Disposition', 'Rent (CZK)', 'Description']].head())

        return df

    else:
        print("No data extracted.")


# The below is very inefficient - it should be done along with the rest of the extraction
# START MODIFICATION: Add extract_images function
def extract_images(f_listings, process_today_only) -> pd.DataFrame:
    files = glob.glob(os.path.join(f_listings, '*'))
    files = [file for file in files
             if os.path.isfile(file)
             and not os.path.basename(file).startswith('.')]
    
    if process_today_only:
        files = [file for file in files if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")]
        
    print(f"Scanning {len(files)} files for images in {f_listings}...")

    data = []
    for file in files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
                script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
                if not script_tag or not script_tag.string:
                    continue
                
                json_data = json.loads(script_tag.string)
                page_props = json_data.get('props', {}).get('pageProps', {})
                advert = page_props.get('origAdvert')
                
                if not advert:
                    continue
                
                listing_id = advert.get('id')
                try:
                    folder_group = f"{str(listing_id)[:3]}/"
                except:
                    folder_group=""
                public_images = advert.get('publicImages', [])
                cache = page_props.get('apolloCache', {})
                
                # Extract date from filename: br_htmls_listings_260114_138.html
                # Split by '_' gives ['br', 'htmls', 'listings', '260114', '138.html']
                filename_parts = os.path.basename(file).split('_')
                date_obtained = None
                
                for img_ref in public_images:
                    img_obj = None
                    if '__ref' in img_ref:
                        img_obj = cache.get(img_ref['__ref'])
                    else:
                        img_obj = img_ref
                    
                    if not img_obj:
                        continue
                        
                    url = img_obj.get('url')
                    if not url:
                        # Search for any key starting with 'url'
                        for k, v in img_obj.items():
                            if k.startswith('url'):
                                url = v
                                break
                    
                    if url:
                        filename = os.path.basename(url)
                        data.append({
                            'listing_id': listing_id,
                            'filename': filename,
                            'object_name': f"br/images/{folder_group}{listing_id}/{filename}",
                            'url': url
                        })

        except Exception:
            continue
    
    if data:
        df = pd.DataFrame(data)
        print(f"Successfully extracted {len(df)} image records.")
        return df
    else:
        print("No image data extracted.")
        return pd.DataFrame()
# END MODIFICATION
