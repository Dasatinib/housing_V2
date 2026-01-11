import pandas as pd
from sqlalchemy import create_engine, text, inspect
import sshtunnel
from datetime import datetime, timedelta

### For external uploads ###

def perform_and_upload(df_today,
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
        sql_dedup_and_upload(engine, df_today)

    else:

        print("Initiating remote upload")

        with sshtunnel.SSHTunnelForwarder(
                (ssh_host),
                ssh_username=ssh_username,
                ssh_pkey=ssh_pkey,
                remote_bind_address=(db_address,3306)) as tunnel:
            engine = create_engine(
                f"mysql+pymysql://{ssh_username}:{ssh_pass}@127.0.0.1:{tunnel.local_bind_port}/{db_name}"
            )
            sql_dedup_and_upload(engine, df_today)

def sql_dedup_and_upload(engine, df_today):

    # Dedupliaction
    # Deduplication needs to be done on SQL side to enable direct deletion of older entries.
    today = datetime.today().strftime('%y%m%d')
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Upload today
    df_today.to_sql("properties", engine, if_exists="append", index=False)
    print(f"✅ Successfully uploaded {len(df_today)} records to 'properties' table")

    print("Moving to deduplication")
    inspector = inspect(engine)
    all_columns = [col['name'] for col in inspector.get_columns('properties')]
    dedup_columns = [col for col in all_columns if col not in ['Date obtained', 'Source file']]
    # Build the PARTITION BY clause dynamically
    partition_cols = ', '.join([f"`{col}`" for col in dedup_columns])
    # Deduplication query - keeps most recent entries
    dedup_query = f"""
    DELETE p1 FROM properties p1
    INNER JOIN (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {partition_cols}
                ORDER BY `Date obtained` DESC
            ) as rn
        FROM properties
        WHERE `Date obtained` >= CURDATE() - INTERVAL 1 DAY
    ) p2 ON p1.`Date obtained` = p2.`Date obtained`
        AND p1.`Source file` = p2.`Source file`
        AND {' AND '.join([f"p1.`{col}` <=> p2.`{col}`" for col in dedup_columns])}
    WHERE p2.rn > 1
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(dedup_query))
        conn.commit()
        deleted_count = result.rowcount
    print(f"✅ Successfully uploaded {len(df_today)} records to 'properties' table")
    print(f"🗑️ Removed {deleted_count} duplicate records (kept most recent)")
