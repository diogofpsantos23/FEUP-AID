from read_dataset import load_parquet

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

    if "RatecodeID" in df.columns:
        df["RatecodeID"] = df["RatecodeID"].fillna(99).astype(int)

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

    print(f"Final dataset shape: {df.shape[0]:,} rows × {df.shape[1]} columns.")
    return df
