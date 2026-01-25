### TODO the dedup sql query checks ALL data. This is too much, once running, it can check only day before (or perhaps a two or three days before if the script crashes for a day)

import pandas as pd
from sqlalchemy import create_engine, text, inspect
import sshtunnel
from datetime import datetime, timedelta

### For and internal uploads ###

def perform_and_upload(df_today,
                       df_today_images,
                       ssh_host,
                       ssh_username,
                       ssh_pass,
                       ssh_pkey,
                       db_address,
                       db_name,
                       db_is_local):

    if db_is_local == "true":

        print("Initiating local upload")

        engine = create_engine(
            f"mysql+pymysql://{ssh_username}:{ssh_pass}@{db_address}:3306/{db_name}"
        )
        sql_dedup_and_upload(engine, df_today, df_today_images)

    else:

        print("Initiating remote upload to SQL")

        with sshtunnel.SSHTunnelForwarder(
                (ssh_host),
                ssh_username=ssh_username,
                ssh_pkey=ssh_pkey,
                remote_bind_address=(db_address,3306)) as tunnel:
            engine = create_engine(
                f"mysql+pymysql://{ssh_username}:{ssh_pass}@127.0.0.1:{tunnel.local_bind_port}/{db_name}"
            )
            sql_dedup_and_upload(engine, df_today, df_today_images)

def sql_dedup_and_upload(engine, df_today, df_today_images): # AI made this

    # 1. Upload today's data first
    df_today.to_sql("properties", engine, if_exists="append", index=False)
    print(f"✅ Successfully uploaded {len(df_today)} records to 'properties' table")
    df_today_images.to_sql("images_staging", engine, if_exists="replace", index=False)
    print("Moving to deduplication (SCD Logic)...")

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
    with engine.begin() as conn:
        move_images = text("""
            INSERT IGNORE INTO images (listing_id, filename, object_name, url)
            SELECT listing_id, filename, object_name, url 
            FROM images_staging;
        """)
        conn.execute(move_images)
    ###

    inspector = inspect(engine)
    existing_indices = [i['name'] for i in inspector.get_indexes('properties')]
    if 'idx_id_date' not in existing_indices:
        print("Creating index 'idx_id_date' on (ID, Date obtained) for performance...")
        with engine.connect() as conn:
            # Fix for BLOB/TEXT error: Specify key length for ID (e.g., 255 chars)
            conn.execute(text("CREATE INDEX idx_id_date ON properties (`listing_id`(255), `Date obtained`)"))
            conn.commit()
        print("Index created.")
    else:
        print("Index 'idx_id_date' already exists.")

    all_columns = [col['name'] for col in inspector.get_columns('properties')]
    
    # --- CONFIGURATION ---
    # You MUST define which column identifies the property (e.g., 'ID', 'ListingID', 'Ref')
    # If the price changes, the ID stays the same, but the attributes change.
    id_column = 'listing_id'  # <--- REPLACE THIS with your actual unique identifier column name -> Done by ID.
    
    # Columns to check for changes (Everything except Metadata and the ID)
    exclude_cols = ['Date obtained', 'Source file', id_column]
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


def get_undownloaded_images(ssh_host,
                       ssh_username,
                       ssh_pass,
                       ssh_pkey,
                       db_address,
                       db_name,
                       db_is_local):

    if db_is_local == "true":

        print("Connecting locally to SQL to get undownloaded images")

        engine = create_engine(
            f"mysql+pymysql://{ssh_username}:{ssh_pass}@{db_address}:3306/{db_name}"
        )
        
        # SQL QUERY GOES HERE. should return a list of links to download:

        undownloaded_images_query = "SELECT id, url, filename, listing_id, downloaded, object_name FROM images WHERE downloaded=0;"
        undownloaded_images = pd.read_sql(undownloaded_images_query, engine)
        return undownloaded_images


    else:

        print("Connecting remotely to SQL to get undownloaded images")

        with sshtunnel.SSHTunnelForwarder(
                (ssh_host),
                ssh_username=ssh_username,
                ssh_pkey=ssh_pkey,
                remote_bind_address=(db_address,3306)) as tunnel:
            engine = create_engine(
                f"mysql+pymysql://{ssh_username}:{ssh_pass}@127.0.0.1:{tunnel.local_bind_port}/{db_name}"
            )
            # SQL QUERY GOES HERE. should return a list of links to download:

            undownloaded_images_query = "SELECT id, url, filename, listing_id, downloaded, object_name FROM images WHERE downloaded=0;"
            undownloaded_images = pd.read_sql(undownloaded_images_query, engine)
            return undownloaded_images


def update_undownloaded_images(undownloaded_images,
                       ssh_host,
                       ssh_username,
                       ssh_pass,
                       ssh_pkey,
                       db_address,
                       db_name,
                       db_is_local):

    if db_is_local == "true":

        print("Connecting locally to SQL to update undownloaded images")

        engine = create_engine(
            f"mysql+pymysql://{ssh_username}:{ssh_pass}@{db_address}:3306/{db_name}"
        )
        
        # SQL QUERY GOES HERE. should return a list of links to download:

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

    else:

        print("Connecting remotely to SQL to update undownloaded images")

        with sshtunnel.SSHTunnelForwarder(
                (ssh_host),
                ssh_username=ssh_username,
                ssh_pkey=ssh_pkey,
                remote_bind_address=(db_address,3306)) as tunnel:
            engine = create_engine(
                f"mysql+pymysql://{ssh_username}:{ssh_pass}@127.0.0.1:{tunnel.local_bind_port}/{db_name}"
            )

            # SQL QUERY GOES HERE. should return a list of links to download:

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
