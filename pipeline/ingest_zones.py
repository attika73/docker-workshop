import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

# Parse command line arguments
parser = argparse.ArgumentParser(description='Ingest NYC taxi zones into PostgreSQL')
parser.add_argument('--pg-user', default=os.getenv('POSTGRES_USER', 'root'), help='PostgreSQL user')
parser.add_argument('--pg-pass', default=os.getenv('POSTGRES_PASSWORD', 'root'), help='PostgreSQL password')
parser.add_argument('--pg-host', default=os.getenv('POSTGRES_HOST', 'localhost'), help='PostgreSQL host')
parser.add_argument('--pg-port', default=os.getenv('POSTGRES_PORT', '5432'), help='PostgreSQL port')
parser.add_argument('--pg-db', default=os.getenv('POSTGRES_DB', 'ny_taxi'), help='PostgreSQL database')
parser.add_argument('--target-table', default='zones', help='Target table name')
args = parser.parse_args()

# Database configuration
db_url = f'postgresql+psycopg2://{args.pg_user}:{args.pg_pass}@{args.pg_host}:{args.pg_port}/{args.pg_db}'
engine = create_engine(db_url)
print(f"Connecting to database: postgresql://{args.pg_user}@{args.pg_host}:{args.pg_port}/{args.pg_db}")

# Load zones data from local CSV file
zones_file = os.path.join(os.path.dirname(__file__), 'taxi_zone_lookup.csv')

try:
    with engine.connect() as conn:
        print("✓ Database connection successful!")
    
    # Load zones data
    print(f"Loading zones data from {zones_file}...")
    df_zones = pd.read_csv(zones_file)
    
    # Display first few rows to verify data
    print("Sample zones data:")
    print(df_zones.head())
    print(f"Columns: {df_zones.columns.tolist()}")
    
    # Load zones into database
    df_zones.to_sql(name=args.target_table, con=engine, if_exists='replace', index=False)
    print(f"✓ Table '{args.target_table}' created successfully!")
    print(f"  Total rows inserted: {len(df_zones)}")
    
except Exception as e:
    print(f"✗ Error: {type(e).__name__}")
    print(f"  Message: {str(e)}")
    exit(1)

print(f"\n✓ Zones ingestion complete!")
