# Libraerias
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from time import time
from tqdm import tqdm
import pandas as pd
import argparse
import os


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    data_file_name = 'output.parquet'


    # Obtener el Parquet
    os.system(f"wget -c {url} -O {data_file_name}")
    df = pd.read_parquet(data_file_name, engine='pyarrow')
    ## Ingestando la data en chunks

    # Creando la tabla
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("Created ny_taxi_data table")

    # Ingesting data
    chunksize = 50_00
    max_size = len(df.index)
    last_run = False
    start = 0
    current = chunksize
    overage = 0

    # Progress bar
    t_start = time()
    with tqdm(total=max_size, unit='steps', unit_scale=True) as pbar:
        while last_run == False:
            if current > max_size:
                overage = current - max_size
                current = max_size
                chunksize -= overage
                last_run = True

            # Inserting chunks
            df.iloc[start:current].to_sql(name=table_name, con=engine, if_exists='append', method='multi')

            start = current
            current += chunksize
            pbar.update(chunksize)
        pbar.update(overage)
    t_end = time()

    print(f"Ingesta de data finalizada en la base de datos, {t_end - t_start:.3f} segundos")


if __name__ == '__main__':
# Parser 
    
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')
    
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password name for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write results to')
    parser.add_argument('--url', help='url of the Parquet file')
    
    args = parser.parse_args()
    
    main(args)





