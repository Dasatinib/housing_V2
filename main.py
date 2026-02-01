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

def main(run_download=True,
         run_processing=True,
         process_today_only=True,
         run_sql=True,
         run_backblaze=True,
         download_images=True):

    print("Making sure folders exist")
    f_mains = os.getenv("FOLDER_MAINS")
    f_listings = os.getenv("FOLDER_LISTINGS")
    f_images = os.getenv("FOLDER_IMAGES")

    if not f_mains or not f_listings or not f_images:
        print("Error: FOLDER_MAINS, FOLDER_LISTINGS, and FOLDER_IMAGES must be set in the environment variables.")
        return

    Path(f_mains).mkdir(parents=True, exist_ok=True)
    Path(f_listings).mkdir(parents=True, exist_ok=True)
    Path(f_images).mkdir(parents=True, exist_ok=True)
    print("Folders exist")

    if run_download:
        asyncio.run(download_br(f_mains, f_listings)) # This downloads all htmls for the day

    df_today = None
    df_today_images = None
    if run_processing:
        print(f"Extracting (today's={process_today_only}) listings information from htmls.")
        df_today = extract_detail(f_listings, process_today_only)
        print("Listings information from htmls extracted successfully.")
        print("Extracting image information from htmls")
        df_today_images = extract_images(f_listings, process_today_only)

    ### SQL operations ###

    if run_sql:
        if df_today is not None and df_today_images is not None:
            print("Initiating sql upload")
            perform_and_upload(df_today, df_today_images)
            print("sql upload completed")
        else:
            print("Skipping SQL upload: No data available (run_processing might be False or failed).")

    ### Backblaze operations ###

    ENDPOINT_URL = os.getenv("B2_ENDPOINT_URL")
    KEY_ID = os.getenv("B2_KEY_ID")
    APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY")
    BUCKET_NAME = os.getenv("B2_BUCKET_NAME")

    DB_IS_LOCAL = os.getenv("DB_IS_LOCAL")

    if run_backblaze:

        daily_files_mains = glob.glob(os.path.join(f_mains, '*'))
        daily_files_mains = [file for file in daily_files_mains
                 if os.path.isfile(file)
   and not os.path.basename(file).startswith('.')]
        #daily_files_mains = [file for file in daily_files_mains if os.path.basename(file).startswith(f"{datetime.today().strftime('%y%m%d')}")]

        for file in daily_files_mains:
            # Extract date from filename (assuming YYMMDD_suffix format)
            file_date = os.path.basename(file).split('_')[0]
            if upload_file(file, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name=f"br/htmls/mains/{file_date}/{os.path.basename(file)}.html"):
                os.remove(file)
                print(f"Uploaded and deleted file {file}.html.")

        for index, listing in df_today.iterrows():
            if DB_IS_LOCAL=="true":
                file = f"./housing_V2/listings/{listing['Source file']}"
            else:
                file = f"listings/{listing['Source file']}"
            file_date=file.split('_')[0]
            # Extract date from filename (assuming YYMMDD_suffix format)
            if upload_file(file, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name=listing['bb_object_name']):
                os.remove(file)
                print(f"Uploaded and deleted file {file}.html.")

    # Image operations

    if download_images:

        ## Get a table of images, filtered for download == 0
        undownloaded_images = get_undownloaded_images()

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
        update_undownloaded_images(undownloaded_images)




if __name__ == "__main__":
    main()
