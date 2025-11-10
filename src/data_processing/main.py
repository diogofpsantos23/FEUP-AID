from transform_dataset import transform_dataset
from save_dataset import save_dataset

def main():
    print("Starting data pipeline...")
    df = transform_dataset()
    save_dataset(df)

if __name__ == "__main__":
    main()
