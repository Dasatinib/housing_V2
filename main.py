"""
Notes
- Currently only bezrealitky

Pseudocode
- Downloads mains and listings htmls into separate folders
- Gets processess listings into a df
- TODO uploads the df into SQL
"""

# My files
from downloadsV2 import download_br
from html_operations import extract_detail
from sql_operations import perform_and_upload
from backblaze_operations import upload_file

# Not my files
import os
import glob
import asyncio
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

def main(run_download=True, 
         run_processing=True, 
         process_today_only=True, 
         run_sql = True, 
         run_backblaze=True):
    
    print("Making sure folders exist")
    f_mains = os.getenv("FOLDER_MAINS")
    f_listings = os.getenv("FOLDER_LISTINGS")

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

    ### SQL operations ###
    
    ssh_host = os.getenv("DB_SSH_HOST")
    ssh_username = os.getenv("DB_USR")
    ssh_pass = os.getenv("DB_PASS")
    ssh_pkey = os.getenv("DB_SSH_FILE")
    db_address = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME_MASTER")
    db_is_local = os.getenv("DB_IS_LOCAL")

    if run_sql:
        perform_and_upload(df_today,
                           ssh_host,
                           ssh_username,
                           ssh_pass,
                           ssh_pkey,
                           db_address,
                           db_name,
                           db_is_local)

    ### Backblaze operations ###

    if run_backblaze:

        ENDPOINT_URL = os.getenv("B2_ENDPOINT_URL")
        KEY_ID = os.getenv("B2_KEY_ID")
        APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY")
        BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
        
        
        daily_files_mains = glob.glob(os.path.join(f_mains, '*'))
        daily_files_mains = [file for file in daily_files_mains
                 if os.path.isfile(file)
                 and not os.path.basename(file).startswith('.')]
        #daily_files_mains = [file for file in daily_files_mains if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")]
           
        for file in daily_files_mains:
            upload_file(file, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name=f"br/htmls/mains/{os.path.basename(file)}")
            os.remove(file)
            print(f"Uploaded and deleted file {file}.")

        daily_files_listings = glob.glob(os.path.join(f_listings, '*'))
        daily_files_listings = [file for file in daily_files_listings
                 if os.path.isfile(file)
                 and not os.path.basename(file).startswith('.')]
        # daily_files_listings = [file for file in daily_files_listings if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")]

        for file in daily_files_listings:
            upload_file(file, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name=f"br/htmls/listings/{os.path.basename(file)}")
            os.remove(file)
            print(f"Uploaded and deleted file {file}.")


if __name__ == "__main__":
    main()
