# Libraerias
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from time import time
from tqdm import tqdm
import pandas as pd
import argparse
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    data_file_name = 'output.parquet'
    # Obtener el Parquet
    os.system(f"wget -c {url} -O {data_file_name}")
    df = pd.read_parquet(data_file_name, engine='pyarrow')
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=1)
def ingest_data(table_name,df):
    # Conexion con el bloque en prefect
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    ## Ingestando la data en chunks
    #Conexion a Postgres
    with connection_block.get_connection(begin=False) as engine:
        # Creando la tabla
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        print("Created ny_taxi_data table")

        # Insertando los datos
        chunksize = 50_00
        max_size = len(df.index)
        last_run = False
        start = 0
        current = chunksize
        overage = 0

        # Barra de progreso
        t_start = time()
        with tqdm(total=max_size, unit='steps', unit_scale=True) as pbar:
            while last_run == False:
                if current > max_size:
                    overage = current - max_size
                    current = max_size
                    chunksize -= overage
                    last_run = True

                # Insertando data por chunks (5000 en 5000)
                df.iloc[start:current].to_sql(name=table_name, con=engine, if_exists='append', method='multi')

                start = current
                current += chunksize
                pbar.update(chunksize)
            pbar.update(overage)
        t_end = time()

        print(f"Ingesta de data finalizada en la base de datos, {t_end - t_start:.3f} segundos")

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name:str):
    print(f"Logging subflow for: {table_name}")

@flow(name="Ingest flow")
def main_flow(table_name :str):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    log_subflow(table_name)
    raw_data = extract_data(url)
    data=transform_data(raw_data)
    ingest_data(table_name,data)

if __name__ == '__main__':
    main_flow("ny_taxi_data")





