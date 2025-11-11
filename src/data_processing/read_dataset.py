import pyarrow.parquet as pq
import pandas as pd

def load_parquet():
    file_path = "../dataset/original/yellow_tripdata_2025-07.parquet"
    print(f"Loading dataset: {file_path}")
    table = pq.read_table(file_path)
    df = table.to_pandas()
    print(f"Number of rows: {len(df):,}\n")
    return df

def load_taxi_zones():
    taxi_zone_lookup_path = "../dataset/original/taxi_zone_lookup.csv"
    print(f"Loading Taxi Zone dataset {taxi_zone_lookup_path}")
    taxi_zone_df = pd.read_csv(taxi_zone_lookup_path)
    print(f"Number of Taxi Zones: {len(taxi_zone_df):,}\n")
    
    taxi_zone_df.loc[taxi_zone_df["Borough"] == "Unknown", ["Zone", "service_zone"]] = "Unknown"
    print("Corrected 'Unknown' borough zones.\n")
    taxi_zone_df.loc[taxi_zone_df["Zone"] == "Outside of NYC", ["Borough", "service_zone"]] = "Outside of NYC"
    print("Corrected 'Outside of NYC' zones.\n")
    return taxi_zone_df
