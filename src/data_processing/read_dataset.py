import pyarrow.parquet as pq

def load_parquet():
    file_path = "../dataset/original/yellow_tripdata_2025-07.parquet"
    print(f"Loading dataset: {file_path}")
    table = pq.read_table(file_path)
    df = table.to_pandas()
    print(f"Number of rows: {len(df):,}\n")
    return df