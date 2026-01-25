import pandas as pd
from sqlalchemy import create_engine, text, inspect
import sshtunnel
from datetime import datetime, timedelta
import os
from functools import wraps
from dotenv import load_dotenv
load_dotenv()

### Create variables
class DBconfig:
    def __init__(self):
        self.ssh_host = os.getenv("DB_SSH_HOST")
        self.ssh_username = os.getenv("DB_USR")
        self.ssh_pass = os.getenv("DB_PASS")
        self.ssh_pkey = os.getenv("DB_SSH_FILE")
        self.db_address = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_NAME_MASTER")
        self.db_is_local = os.getenv("DB_IS_LOCAL")

db_config = DBconfig()

# Define engine wrapper
def with_sql_engine(func):
    @wraps(func)
    def wrapper(*args,**kwargs):
        tunnel = None
        engine = None

        try:
            if db_config.db_is_local == "true":
                print("Initiating local upload")
                engine = create_engine(
                    f"mysql+pymysql://{db_config.ssh_username}:{db_config.ssh_pass}@{db_config.db_address}:3306/{db_config.db_name}"
                )
            else:
                tunnel = sshtunnel.SSHTunnelForwarder(
                        (db_config.ssh_host),
                        ssh_username=db_config.ssh_username,
                        ssh_pkey=db_config.ssh_pkey,
                        remote_bind_address=(db_config.db_address,3306)
                    )
                tunnel.start()
                engine = create_engine(
                    f"mysql+pymysql://{db_config.ssh_username}:{db_config.ssh_pass}@127.0.0.1:{tunnel.local_bind_port}/{db_config.db_name}"
                )
            return func(*args, engine=engine, **kwargs)
        finally:
            if tunnel:
                tunnel.stop()
    return wrapper


@with_sql_engine
def perform_and_upload(df_today, df_today_images, engine = None):
    sql_dedup_and_upload(engine, df_today, df_today_images)

def sql_dedup_and_upload(engine, df_today, df_today_images): # AI made this

    # 1. Upload today's data first
    df_today.to_sql("properties", engine, if_exists="append", index=False)
    print(f"✅ Successfully uploaded {len(df_today)} records to 'properties' table")
    df_today_images.to_sql("images_staging", engine, if_exists="replace", index=False)

    # --- PERFORMANCE FIX: Ensure Index Exists ---
    # The deduplication query relies heavily on partitioning by ID and ordering by Date.
    # Without an index, this causes a full table sort which hangs the script.
    try:
        with engine.connect() as conn:
            # check if index exists (simple check via exception or show index)
            # MySQL 5.7+ doesn't support "IF NOT EXISTS" well in create index usually, so we check first.
            # But 'inspector' is available from earlier.
            pass
    except Exception:
        pass
    
    ### Move images from staging to main table
    print("Moving images from staging to main table")
    with engine.begin() as conn:
        move_images = text("""
            INSERT IGNORE INTO images (listing_id, filename, object_name, url)
            SELECT listing_id, filename, object_name, url 
            FROM images_staging;
        """)
        conn.execute(move_images)
    print("Images moved")
    ###

    print("Initiating deduplication (SCD Logic)...")
    inspector = inspect(engine)
    existing_indices = [i['name'] for i in inspector.get_indexes('properties')]
    if 'idx_id_date' not in existing_indices:
        print("Creating index 'idx_id_date' on (ID, Date obtained) for performance...")
        with engine.connect() as conn:
            # Fix for BLOB/TEXT error: Specify key length for ID (e.g., 255 chars)
            conn.execute(text("CREATE INDEX idx_id_date ON properties (`listing_id`(255), `Date obtained`)"))
            conn.commit()
        print("Index created.")

    all_columns = [col['name'] for col in inspector.get_columns('properties')]
    
    # --- CONFIGURATION ---
    # You MUST define which column identifies the property (e.g., 'ID', 'ListingID', 'Ref')
    # If the price changes, the ID stays the same, but the attributes change.
    id_column = 'listing_id'  # <--- REPLACE THIS with your actual unique identifier column name -> Done by ID.
    
    # Columns to check for changes (Everything except Metadata and the ID)
    exclude_cols = ['Date obtained', 'Source file', 'bb_object_name', id_column]
    data_columns = [col for col in all_columns if col not in exclude_cols]
    
    # Build dynamic comparison string: p_curr.Price <=> p_prev.Price AND ...
    # (<=> is NULL-safe equality in MySQL)
    comparison_logic = " AND ".join([f"p_curr.`{col}` <=> p_prev.`{col}`" for col in data_columns])

    # --- THE QUERY ---
    # Logic: Delete a row IF:
    # 1. It has a previous record (It's not the first one)
    # 2. Its data is IDENTICAL to the previous record
    # 3. It is NOT the latest record (rn_desc > 1) -> This keeps the "7th" entry
    
    dedup_query = f"""
    DELETE target 
    FROM properties target
    -- 1. Calculate Previous Date and Recency Rank for every row
    INNER JOIN (
        SELECT 
            `{id_column}`,
            `Date obtained`,
            `Source file`,
            -- Get the date of the record immediately preceding this one
            LAG(`Date obtained`) OVER (
                PARTITION BY `{id_column}` 
                ORDER BY `Date obtained` ASC
            ) as prev_date,
            -- Rank by newest (1 = Latest/Today, 2 = Yesterday, etc.)
            ROW_NUMBER() OVER (
                PARTITION BY `{id_column}` 
                ORDER BY `Date obtained` DESC
            ) as rn_desc
        FROM properties
        -- Note: We look at the full history to ensure correct change tracking
    ) navigation ON target.`{id_column}` = navigation.`{id_column}` 
                 AND target.`Date obtained` = navigation.`Date obtained`
                 AND target.`Source file` = navigation.`Source file`
    
    -- 2. Join to the actual Property table again to get the DATA of the previous record
    INNER JOIN properties p_prev 
        ON p_prev.`{id_column}` = navigation.`{id_column}` 
        AND p_prev.`Date obtained` = navigation.prev_date
        
    -- 3. Alias the target as p_curr for readability in comparison
    INNER JOIN properties p_curr 
        ON p_curr.`{id_column}` = target.`{id_column}` 
        AND p_curr.`Date obtained` = target.`Date obtained`
        AND p_curr.`Source file` = target.`Source file`

    WHERE 
        -- CONDITION A: The data is exactly the same as the previous record
        ( {comparison_logic} )
        
        -- CONDITION B: It is NOT the absolute latest record
        -- (We want to keep the 7th entry even if it's same as 6th, to show it's still alive)
        AND navigation.rn_desc > 1
    """

    with engine.connect() as conn:
        result = conn.execute(text(dedup_query))
        conn.commit()
        deleted_count = result.rowcount

    print(f"🗑️ Removed {deleted_count} redundant intermediate records.")
    print("   (Kept: First appearance, Changes, and Latest status)")

@with_sql_engine
def get_undownloaded_images(engine=None):
    undownloaded_images_query = "SELECT id, url, filename, listing_id, downloaded, object_name FROM images WHERE downloaded=0;"
    undownloaded_images = pd.read_sql(undownloaded_images_query, engine)
    return undownloaded_images

@with_sql_engine
def update_undownloaded_images(undownloaded_images, engine=None):
    undownloaded_images=undownloaded_images[["id", "downloaded"]]
    undownloaded_images.to_sql("images_staging", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        move_images = text("""
            UPDATE images
            INNER JOIN images_staging
                ON images.id = images_staging.id
            SET images.downloaded = images_staging.downloaded;
        """)
        conn.execute(move_images)


