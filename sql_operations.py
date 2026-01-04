import pandas as pd
from sqlalchemy import create_engine, text
import sshtunnel

### For external uploads ###

def upload_externally(dataframe, 
                      ssh_host, 
                      ssh_username, 
                      ssh_pass,
                      ssh_pkey,
                      db_address,
                      db_name):

    with sshtunnel.SSHTunnelForwarder(
            (ssh_host),
            ssh_username=ssh_username,
            ssh_pkey=ssh_pkey,
            remote_bind_address=(db_address,3306)) as tunnel:
        engine = create_engine(
            f"mysql+pymysql://{ssh_username}:{ssh_pass}@127.0.0.1:{tunnel.local_bind_port}/{db_name}"
        )

        # Insert data into mySQL table
        df.to_sql("properties", engine, if_exists="replace", index=False)
        print(f"✅ Successfully uploaded {len(df)} records to 'properties' table (SSH tunnel)")
