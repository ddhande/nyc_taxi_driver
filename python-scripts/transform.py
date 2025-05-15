import pandas as pd
from sqlalchemy import create_engine
import sys
import os
from urllib.parse import quote

def transform_and_store(csv_path):
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        print(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path, low_memory=False)

        print("Columns in CSV:", df.columns.tolist())
        print("\nSample data:")
        print(df.head())

        # Check for and clean the pickup time column
        print("\nSample tpep_pickup_datetime values:")
        print(df['tpep_pickup_datetime'].head())
        print("\nTotal rows before transformation:", len(df))

        print("Transforming pickup datetime...")

        # Remove trailing '.0' if present
        df['tpep_pickup_datetime_clean'] = df['tpep_pickup_datetime'].astype(str).str.replace('.0$', '', regex=True)

        # Split time into hours and minutes
        time_parts = df['tpep_pickup_datetime_clean'].str.split(':', expand=True)
        df['hours'] = pd.to_numeric(time_parts[0], errors='coerce')
        df['minutes'] = pd.to_numeric(time_parts[1], errors='coerce')

        # Drop rows with parsing issues
        df = df.dropna(subset=['hours', 'minutes'])

        # Create datetime by adding timedelta to base date
        base_date = pd.to_datetime('2025-01-01')
        df['tpep_pickup_datetime'] = base_date + pd.to_timedelta(df['hours'], unit='h') + pd.to_timedelta(df['minutes'], unit='m')

        # Clean up temp columns
        df = df.drop(columns=['tpep_pickup_datetime_clean', 'hours', 'minutes'])

        # Check result
        print("Parsed tpep_pickup_datetime values:")
        print(df['tpep_pickup_datetime'].head())
        print("\nNaT count:", df['tpep_pickup_datetime'].isna().sum())

        if df['tpep_pickup_datetime'].isna().all():
            raise ValueError("All tpep_pickup_datetime values are invalid after parsing. Please fix the dataset.")

        df['date'] = df['tpep_pickup_datetime'].dt.date

        # Aggregate data
        transformed_df = df.groupby('date').agg({
            'trip_distance': 'sum',
            'fare_amount': 'mean',
            'passenger_count': 'sum'
        }).reset_index()

        print("Transformed data:")
        print(transformed_df.head())

        if transformed_df.empty:
            raise ValueError("Transformed DataFrame is empty. Check datetime parsing or input data.")

        print("Connecting to PostgreSQL...")
        password = "Rita@1975"
        encoded_password = quote(password)
        connection_string = f'postgresql://postgres:{encoded_password}@localhost:5432/nyc_taxi_driver'
        engine = create_engine(connection_string)

        print("Inserting data into daily_trips table...")
        transformed_df.to_sql('daily_trips', engine, if_exists='replace', index=False)
        print("Data transformed and stored successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    csv_path = "../data/yellow_tripdata_2023-01.csv"
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    print(f"Using CSV path: {csv_path}")
    transform_and_store(csv_path)
