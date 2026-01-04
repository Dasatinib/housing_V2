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
from sql_operations import upload_externally

# Not my files
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

def main(run_download=True, run_processing=True, process_today_only=True):
    
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
    
    if run_processing:
        extract_detail(f_listings, process_today_only)

    ### SQL operations ###
    
    ssh_host = os.getenv("DB_SSH_HOST")
    ssh_username = os.getenv("DB_USR")
    ssh_pass = os.getenv("DB_PASS")
    ssh_pkey = os.getenv("DB_SSH_FILE")
    db_address = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME_MASTER")


if __name__ == "__main__":
    main()
