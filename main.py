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

if __name__ == "__main__":
    main()
