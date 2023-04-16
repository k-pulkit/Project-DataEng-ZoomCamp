#type: ignore

"""
This script contains the logic to 
read data from a csv file and write it to a target database
which in this case is a Postgres database
"""

import argparse
from tqdm import tqdm
from typing import Optional
import sys
import os
import pandas as pd
from subprocess import check_output
from sqlalchemy import create_engine, engine, inspect
from sqlalchemy.sql import text as t

def create_sql_engine(user, password, host, port, database) -> engine.Engine:
    "Connect to the database"
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    return engine

def test_db_connection(engine: engine.Engine) -> bool:
    try:
        connection = engine.connect()
        connection.execute(t("Select 1")).fetchall()
        connection.close()
        return True
    except Exception as e:
        print("Exception while connecting - ", e)
        return False
    
def check_table_exists(engine: engine.Engine, tablename: str) -> bool:
    ins = inspect(engine)
    # Check if the 'users' table exists
    if ins.has_table(tablename, schema="public"):
        return True
    else:
        return False
    
def drop_tables() -> None:  
    with engine.connect() as conn:
        conn.execute(t("DROP TABLE IF EXISTS ny_taxi_data"))
        conn.commit()

def create_table_if_not_exists(engine: engine.Engine, filepath: str, tablename: str) -> None:
    # get the columns
    cols = pd.read_csv(filepath, nrows=0).columns
    date_cols = cols[cols.str.contains("date")].tolist()
    # get the first 100 rows to infer the datatypes
    data_sample = pd.read_csv(filepath, nrows=100, parse_dates=date_cols).head()
    # create table scheme
    with engine.connect() as c:
        _ = pd.io.sql.get_schema(data_sample, tablename, con=engine).replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        c.execute(t(_))
        c.commit()

def main(params):
    url = params.url
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db_name
    tbl = params.table_name
    
    # create connection to the database
    engine: engine.Engine = create_sql_engine(user, password, host, port, db)
    
    # test the connection
    test_db: bool = test_db_connection(engine)
    if test_db is False:
        # exit from application if cannot connect to db
        sys.exit(1)
    
    # Download the file to a temporary location
    os.makedirs("/tmp", exist_ok=True)
    filename = 'download' + os.path.splitext(url)[1]
    filepath = f"/tmp/{filename}"
    os.system(f'wget -O /tmp/{filename} {url}')
    
    # check if table exists, then create schema in Postgres
    if not check_table_exists(engine, tbl):
        create_table_if_not_exists(engine, filepath, tbl)

    # Push the data to postgres    
    CHUNK_SIZE = 100000
    MAX_ROWS = int(check_output(f"zcat -f {filepath} | wc -l ", shell=True).split()[0]) - 1
    print(f"Total number of rows to ingest are {MAX_ROWS}")

    data_iterator = pd.read_csv(filepath, chunksize=CHUNK_SIZE, iterator=True)
    with engine.connect() as conn:
        with tqdm(total=MAX_ROWS, desc="Chunks read: ") as bar:
            for data in data_iterator:
                data.to_sql("ny_taxi_data", if_exists="append", index=False, con=conn)
                bar.update(data.shape[0])
            print("Done")
        # Commit all data to database
        conn.commit()
    
    # Download the file
    os.remove(filepath)

if __name__ == '__main__':

    # Define the arguments for the script
    # SOURCE - URL of the file to ingest
    # TARGET - user, password, host, port, database_name, table_name
    parser=argparse.ArgumentParser(description="Ingest CSV Data to postgres database")
    parser.add_argument("--url", type=str, help="URL of NY taxi data file to load to the target database")
    parser.add_argument("--user", type=str, help="username of the database")
    parser.add_argument("--password", type=str, help="password of the database")
    parser.add_argument("--host", type=str, help="host of the database, like localhost")
    parser.add_argument("--port", type=str, help="port of the database, like 5132")
    parser.add_argument("--db_name", type=str, help="database name")
    parser.add_argument("--table_name", type=str, help="tablename of the database")
    args = parser.parse_args()
    
    print(args)
    
    # call the main
    main(args)
