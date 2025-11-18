from read_dataset import load_parquet
from read_dataset import load_taxi_zones

def transform_dataset(limit = 15000):
    df = load_parquet()

    if len(df) > limit:
        df = df.sample(n=limit)
        print(f"Trimmed dataset to {len(df):,} rows.")
        
    before_drop = len(df)
    df = df.dropna(subset=["passenger_count"])
    dropped = before_drop - len(df)
    if dropped > 0:
        print(f"Dropped {dropped:,} rows with null 'passenger_count'.")

    ratecode_map = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride",
        99: "Null/unknown",
    }

    if "RatecodeID" in df.columns:
        df["RatecodeID"] = df["RatecodeID"].map(ratecode_map).astype(str)
        print("Converted 'RatecodeID' to descriptive strings.")

    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].fillna("N").astype(str)

    payment_type_map = {
        0: "Flex Fare trip",
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].astype(int, errors="ignore").map(payment_type_map).astype(str)
        print("Converted 'payment_type' to descriptive strings.")
        
    vendor_id_map = {
        1: "Creative Mobile Technologies, LLC",
        2: "Curb Mobility, LLC",
        7: "Helix"
    }
    
    if "VendorID" in df.columns:
        df["VendorID"] = df["VendorID"].astype(int, errors="ignore").map(vendor_id_map).astype(str)
        print("Converted 'vendor_id' to descriptive strings.")
        
    taxi_zones_df = load_taxi_zones()
    
    if "PULocationID" in df.columns:
        pu = taxi_zones_df.loc[:, ["LocationID", "Borough", "Zone", "service_zone"]].rename(
            columns={
                "LocationID": "PULocationID",
                "Borough": "PU_Borough",
                "Zone": "PU_Zone",
                "service_zone": "PU_Service_zone",
            }
        )
        df = df.merge(pu, how="left", on="PULocationID")
        print("Merged pickup location zones.")

    if "DOLocationID" in df.columns:
        do = taxi_zones_df.loc[:, ["LocationID", "Borough", "Zone", "service_zone"]].rename(
            columns={
                "LocationID": "DOLocationID",
                "Borough": "DO_Borough",
                "Zone": "DO_Zone",
                "service_zone": "DO_Service_zone",
            }
        )
        df = df.merge(do, how="left", on="DOLocationID")
        print("Merged dropoff location zones.")

    df = df.drop(columns=["PULocationID", "DOLocationID"], errors="ignore")    
    
    print(f"Final dataset shape: {df.shape[0]:,} rows × {df.shape[1]} columns.")
    return df
