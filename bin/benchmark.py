import csv
import duckdb
import lance
import os
import pandas as pd
import pyarrow as pa
import shutil
import subprocess
import time

# NOTE: the following variables should really be commandline options but I am too lazy
# to go through the work of doing that for now...
# export LANCE_KDB_DATA_PATH=/home/nico/Repos/lance-benchmark/data
DATA_DIR: str = os.getenv("LANCE_KDB_DATA_PATH")
LANCE_PATH: str = DATA_DIR + "/lance/"
LANCE_DATASET_PATH = LANCE_PATH + "trips.2022.01.lance"
CSV_PATH: str = DATA_DIR + "/csv/"

SHOULD_WRITE: bool = False
SHOULD_QUERY: bool = True

# NOTE: the following script assumes that a q writer process is running on port 8888.
# We use python to read parquet data in batches and PyQ to send data to our q writer
# process. We generate lance files in Python and kdb+ files in q.
# HACK: currently the data is hard coded to be yellowcab trip data from 01/2022
URL: str = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
)

if not LANCE_PATH:
    print("Please set LANCE_PATH")
    exit(1)

if SHOULD_WRITE:
    trips = pd.read_parquet(URL)

    # write csv data for q process
    print("writing csv dataset")
    st = time.time()
    with open(CSV_PATH + "trips.2022.01.csv", "w", newline="") as file:
        writer = csv.writer(file)
        header = ["id"] + list(trips.columns)
        writer.writerow(header)
        trips.to_csv(file, header=False, index=True)

    print(f"csv dataset write executed in {time.time() - st} seconds\n")

    # write kdb+ data
    q_process = subprocess.Popen(
        [os.environ["QHOME"] + "/l64/q", "load_data.q", "-q"],
    )
    q_process_result = q_process.wait()
    if q_process_result:
        print("Failed to write kdb+ data")
        exit(1)

    # write lance data
    print("writing lance dataset")
    st = time.time()
    shutil.rmtree(LANCE_DATASET_PATH, ignore_errors=True)
    # HACK: add ID column
    # NOTE: If the column name is changed to 'ID', then typing lowercase column name
    # 'id' in SQL queries will result in an Error: Invalid Error: ValueError: LanceError(IO): Column vendorid does not exist in the dataset. # noqa:501
    # In fact, it appears that any column names that contain uppercase letters cause the
    # same error. See: https://github.com/duckdb/duckdb/issues/2075.
    trips["id"] = trips.index.values
    print(trips)
    dataset = lance.write_dataset(trips, LANCE_DATASET_PATH)
    print(f"lance dataset write executed in {time.time() - st} seconds\n")

# -----------------
# Benchmark Queries
# -----------------
# NOTE: equivalent qsql queries can be found in query_data.q
QUERY1: str = "SELECT * FROM dataset WHERE id > -1"
QUERY2: str = (
    "SELECT VendorID, tpep_pickup_datetime, trip_distance FROM dataset WHERE "
    + "trip_distance > 10"
)
if SHOULD_QUERY:
    # query kdb+ data
    print("running benchmark queries on kdb+ dataset")
    q_process = subprocess.Popen(
        [os.environ["QHOME"] + "/l64/q", "query_data.q", "-q"],
    )
    q_process_result = q_process.wait()
    if q_process_result:
        print("Failed to query kdb+ dataset")
        exit(1)

    # query in-memory arrow data
    if not SHOULD_WRITE:
        trips = pd.read_parquet(URL)
        trips["id"] = trips.index.values

    dataset = pa.Table.from_pandas(trips)
    print("running benchmark queries on arrow dataset")
    st = time.time()
    print(duckdb.query(QUERY1).execute())
    min_delta = 1000 * (time.time() - st)
    print(f'First "{QUERY1}" executed in {min_delta} milliseconds')
    for _ in range(10):
        st = time.time()
        duckdb.query(QUERY1).execute()
        delta = 1000 * (time.time() - st)
        if delta < min_delta:
            min_delta = delta

    print(f"Minimum execution time: {min_delta} milliseconds")

    st = time.time()
    print(duckdb.query(QUERY2).execute())
    min_delta = 1000 * (time.time() - st)
    print(f'First "{QUERY2}" executed in {min_delta} milliseconds')
    for _ in range(10):
        st = time.time()
        duckdb.query(QUERY2).execute()
        delta = 1000 * (time.time() - st)
        if delta < min_delta:
            min_delta = delta

    print(f"Minimum execution time: {min_delta} milliseconds")

    # query lance data
    dataset = lance.dataset(
        LANCE_DATASET_PATH,
    )
    print("running benchmark queries on lance dataset")
    st = time.time()
    # print(duckdb.query(QUERY1).execute())
    print(
        dataset.to_table(
            filter="id > -1",
        )
    )
    min_delta = 1000 * (time.time() - st)
    print(f'First "{QUERY1}" executed in {min_delta} milliseconds')
    for _ in range(10):
        st = time.time()
        # duckdb.query(QUERY1).execute()
        dataset.to_table(
            filter="id > -1",
        )
        delta = 1000 * (time.time() - st)
        if delta < min_delta:
            min_delta = delta

    print(f"Minimum execution time: {min_delta} milliseconds")

    st = time.time()
    # print(duckdb.query(QUERY2).execute())
    print(
        dataset.to_table(
            columns=["VendorID", "tpep_pickup_datetime", "trip_distance"],
            filter="trip_distance > 10",
        )
    )
    min_delta = 1000 * (time.time() - st)
    print(f'First "{QUERY2}" executed in {min_delta} milliseconds')
    for _ in range(10):
        st = time.time()
        # duckdb.query(QUERY2).execute()
        dataset.to_table(
            columns=["VendorID", "tpep_pickup_datetime", "trip_distance"],
            filter="trip_distance > 10",
        )
        delta = 1000 * (time.time() - st)
        if delta < min_delta:
            min_delta = delta

    print(f"Minimum execution time: {min_delta} milliseconds")

exit(0)
