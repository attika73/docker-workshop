import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import argparse
import os

# Parse command line arguments
parser = argparse.ArgumentParser(description='Ingest NYC taxi data into PostgreSQL')
parser.add_argument('--pg-user', default=os.getenv('POSTGRES_USER', 'root'), help='PostgreSQL user')
parser.add_argument('--pg-pass', default=os.getenv('POSTGRES_PASSWORD', 'root'), help='PostgreSQL password')
parser.add_argument('--pg-host', default=os.getenv('POSTGRES_HOST', 'localhost'), help='PostgreSQL host')
parser.add_argument('--pg-port', default=os.getenv('POSTGRES_PORT', '5432'), help='PostgreSQL port')
parser.add_argument('--pg-db', default=os.getenv('POSTGRES_DB', 'ny_taxi'), help='PostgreSQL database')
parser.add_argument('--target-table', default='yellow_taxi_data', help='Target table name')
parser.add_argument('--year', type=int, default=2021, help='Year to ingest')
parser.add_argument('--month', type=int, default=None, help='Specific month to ingest (1-12, optional)')
parser.add_argument('--chunksize', type=int, default=100000, help='Chunk size for reading CSV')
args = parser.parse_args()

# Configuration
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
if args.month:
    # Specific month requested
    months = [f'{args.year:04d}-{args.month:02d}']
else:
    # All months of the year
    months = [f'{args.year:04d}-{month:02d}' for month in range(1, 13)]

# Data types and parsing
dtype = {
    'VendorID': int,
    'passenger_count': int,
    'RatecodeID': int,
    'PULocationID': int,
    'DOLocationID': int,
    'payment_type': int,
    'trip_distance': float,
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'congestion_surcharge': float,
}

parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

# Database configuration
db_url = f'postgresql+psycopg2://{args.pg_user}:{args.pg_pass}@{args.pg_host}:{args.pg_port}/{args.pg_db}'
engine = create_engine(db_url)
print(f"Connecting to database: postgresql://{args.pg_user}@{args.pg_host}:{args.pg_port}/{args.pg_db}")

# Create table from first dataset
print("Loading initial dataset...")
df_init = pd.read_csv(f'{prefix}yellow_tripdata_2021-07.csv.gz')

try:
    with engine.connect() as conn:
        print("✓ Database connection successful!")
    
    # Create table with schema
    df_init.head(0).to_sql(name=args.target_table, con=engine, if_exists='replace')
    print(f"✓ Table '{args.target_table}' created successfully!")
except Exception as e:
    print(f"✗ Error: {type(e).__name__}")
    print(f"  Message: {str(e)}")
    exit(1)

# Ingest data by month
total_rows = 0
for month in tqdm(months, desc="Ingesting data by month"):
    url = f'{prefix}yellow_tripdata_{month}.csv.gz'
    
    try:
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=args.chunksize
        )
        
        for chunk in tqdm(df_iter, desc=f"  {month}", leave=False):
            chunk.to_sql(name=args.target_table, con=engine, if_exists='append', index=False)
            total_rows += len(chunk)
    except Exception as e:
        print(f"Warning: Could not load {month} - {str(e)}")
        continue

print(f"\n✓ Data ingestion complete!")
print(f"  Total rows inserted: {total_rows:,}")

# Display table schema
try:
    schema = pd.io.sql.get_schema(df_init, name=args.target_table, con=engine)
    print(f"\nTable '{args.target_table}' Schema:")
    print(schema)
except Exception as e:
    print(f"✗ Error retrieving schema: {type(e).__name__}")
