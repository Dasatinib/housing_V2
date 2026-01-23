# This version is for import

import asyncio
from nord_session import NordVPNSession
from html_operations import get_listing_urls
# from bezrealitky import get_page_n
from pathlib import Path
from datetime import datetime
import os
import json
from bs4 import BeautifulSoup

def get_page_n(content):
    soup = BeautifulSoup(content.content,"html.parser") #! Need to add content.html.html to extract directly from a request.
    # soup = BeautifulSoup(content, "html.parser") # Temp for offline file parsing
    target = soup.find("script", {"id":"__NEXT_DATA__"})
    if not target or not target.string:
        print("Error: Could not find __NEXT_DATA__ script tag.")
        return 0
    processed = json.loads(target.string)
    total_ads = processed.get('props', {}).get('pageProps', {}).get('apolloCache', {}).get('ROOT_QUERY', {})
    total_ads = total_ads.get(list(total_ads.keys())[2], {}).get('totalCount')
    pages_n = int(total_ads/15) + (total_ads%15>0)
    return pages_n

async def download_br(f_mains, f_listings):

    print("Initializing session...")
    nord = NordVPNSession()
    await nord.initialize()
    
    try:
        page_n=0
        try:
            first_url = "https://www.bezrealitky.cz/vyhledat?offerType=PRONAJEM&estateType=BYT&regionOsmIds=R51684&osm_value=%C4%8Cesko&location=exact&currency=CZK&page=1" # Replace with your target
            template_url = "https://www.bezrealitky.cz/vyhledat?offerType=PRONAJEM&estateType=BYT&regionOsmIds=R51684&osm_value=%C4%8Cesko&location=exact&currency=CZK&page="
            print(f"Getting page no. \n{first_url}")
            
            first_raw = await nord.get(first_url)
            page_n = get_page_n(first_raw)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            print(f"Number of pages to get: {page_n}")
        
        if page_n:
            try:
                for page in range(page_n):
                    page = page + 1
                    url = template_url+str(page)
                    page_raw = await nord.get(url)
                    if page_raw:
                        with open(f"{f_mains}/{datetime.today().strftime('%y%m%d')}_{page}", "wb+") as f:
                            f.write(page_raw.content)
                        print(f"Page {page} saved")
            except Exception as e:
                print(f"Error: {e}")
            finally:
                print("Mains job complete")

            urls = get_listing_urls(f_mains)
            print(f"There are {len(urls)} within f_main htmls that will be processed.")

            if urls:
                try:
                    for i, url in enumerate(urls, 1):
                        page_raw = await nord.get(url)
                        if page_raw:
                            with open(f"{f_listings}/{datetime.today().strftime('%y%m%d')}_{i}", "wb+") as f:
                                f.write(page_raw.content)
                            print(f"Listing {i} saved")
                except Exception as e:
                    print(f"Error {e}")
                finally:
                    print("Listings job complete")
    finally:
        await nord.close()

# At the final step of main
# I need to make a sql query to get all image links that have downloaded=0
# download them
# move them over to backblaze




async def download_br_images(undownloaded_images, f_images):

    print("Initializing session...")
    nord = NordVPNSession()
    await nord.initialize()
    
    try:
        for index in undownloaded_images.index:
            url=undownloaded_images.at[index,"url"]
            filename=undownloaded_images.at[index,"filename"]
            if url:
                try:
                    image = await nord.get(url)
                    if image:
                        with open(f"{f_images}/{filename}", "wb+") as f:
                            f.write(image.content)
                            undownloaded_images.at[index, "downloaded"] = 1
                        print(f"Image {filename} saved")
                except Exception as e:
                    print(f"Error downloading {url}: {e}")
        print("Images download job complete")
    finally:
        await nord.close()


