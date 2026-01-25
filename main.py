"""
Notes
- Currently only bezrealitky

Pseudocode
- Downloads mains and listings htmls into separate folders
- Gets processess listings into a df
- TODO uploads the df into SQL
"""

# My files
from downloadsV2 import download_br, download_br_images
from html_operations import extract_detail, extract_images
from sql_operations import perform_and_upload, get_undownloaded_images, update_undownloaded_images
from backblaze_operations import upload_file

# Not my files
import os
import glob
import asyncio
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

def main(run_download=False, 
         run_processing=False, 
         process_today_only=False, 
         run_sql=False,
         run_backblaze=False,
         download_images=True):
    
    print("Making sure folders exist")
    f_mains = os.getenv("FOLDER_MAINS")
    f_listings = os.getenv("FOLDER_LISTINGS")
    f_images = os.getenv("FOLDER_IMAGES")

    if not f_mains or not f_listings:
        print("Error: FOLDER_MAINS and FOLDER_LISTINGS must be set in the environment variables.")
        return

    Path(f_mains).mkdir(parents=True, exist_ok=True)
    Path(f_listings).mkdir(parents=True, exist_ok=True)
    print("Folders exist")
    
    if run_download:
        asyncio.run(download_br(f_mains, f_listings)) # This downloads all htmls for the day
    
    df_today = None
    if run_processing:
        print(f"Extracting (today's={process_today_only}) listings information from htmls.")
        df_today = extract_detail(f_listings, process_today_only)
        print("Listings information from htmls extracted successfully.")
        print("Extracting image information from htmls")
        df_today_images = extract_images(f_listings, process_today_only)
        print("Image extraction completed")

    ### SQL operations ###
    ssh_host = os.getenv("DB_SSH_HOST")
    ssh_username = os.getenv("DB_USR")
    ssh_pass = os.getenv("DB_PASS")
    ssh_pkey = os.getenv("DB_SSH_FILE")
    db_address = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME_MASTER")
    db_is_local = os.getenv("DB_IS_LOCAL")

    if run_sql:
        print("Initiating sql upload")
        perform_and_upload(df_today,
                           df_today_images,
                           ssh_host,
                           ssh_username,
                           ssh_pass,
                           ssh_pkey,
                           db_address,
                           db_name,
                           db_is_local)
        print("sql upload completed")

    ### Backblaze operations ###
    
    ENDPOINT_URL = os.getenv("B2_ENDPOINT_URL")
    KEY_ID = os.getenv("B2_KEY_ID")
    APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY")
    BUCKET_NAME = os.getenv("B2_BUCKET_NAME")

    if run_backblaze:
        
        daily_files_mains = glob.glob(os.path.join(f_mains, '*'))
        daily_files_mains = [file for file in daily_files_mains
                 if os.path.isfile(file)
   and not os.path.basename(file).startswith('.')]
        #daily_files_mains = [file for file in daily_files_mains if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")]
           
        for file in daily_files_mains:
            if upload_file(file, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name=f"br/htmls/mains/{os.path.basename(file)}"):
                os.remove(file)
                print(f"Uploaded and deleted file {file}.")

        daily_files_listings = glob.glob(os.path.join(f_listings, '*'))
        daily_files_listings = [file for file in daily_files_listings
                 if os.path.isfile(file)
                 and not os.path.basename(file).startswith('.')]
        # daily_files_listings = [file for file in daily_files_listings if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")]

        for file in daily_files_listings:
            if upload_file(file, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name=f"br/htmls/listings/{os.path.basename(file)}"):
                os.remove(file)
                print(f"Uploaded and deleted file {file}.")
    
    # Image operations

    if download_images:
        
        ## Get a table of images, filtered for download == 0
        undownloaded_images = get_undownloaded_images(ssh_host,
                       ssh_username,
                       ssh_pass,
                       ssh_pkey,
                       db_address,
                       db_name,
                       db_is_local)
        
        # FOR TESTING
        # undownloaded_images = undownloaded_images[:10]
        ###

        ## Download those images
        asyncio.run(download_br_images(undownloaded_images, f_images))

        ## Upload those images to B2
        for index in undownloaded_images.index:
            if undownloaded_images.at[index,"downloaded"]==1:
                listing_id=undownloaded_images.at[index,"listing_id"]
                filename=undownloaded_images.at[index,"filename"]
                object_name=undownloaded_images.at[index,"object_name"]
                file_path = f"{f_images}/{listing_id}-{filename}"
                
                if not os.path.exists(file_path):
                    print(f"File {file_path} not found, skipping upload.")
                    continue

                if upload_file(file_path, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name):
                    undownloaded_images.at[index,"downloaded"] = 3
                    os.remove(file_path)
            else:
                pass

        ## Update sql with image download statuses
        update_undownloaded_images(undownloaded_images,
                       ssh_host,
                       ssh_username,
                       ssh_pass,
                       ssh_pkey,
                       db_address,
                       db_name,
                       db_is_local)
        

        

if __name__ == "__main__":
    main()
