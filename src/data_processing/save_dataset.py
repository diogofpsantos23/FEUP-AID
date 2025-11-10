import uuid

def save_dataset(df):
    filename = "yellow_tripdata_2025-07"
    uid = uuid.uuid4().hex[:8]
    output_path = f"../dataset/processed/{filename}_{uid}.csv"
    df.to_csv(output_path, index=False)
    print(f"Dataset saved to: {output_path}")
    return output_path
