import psycopg2

# Connect to database
conn = psycopg2.connect(
    host="localhost",
    user="root",
    password="root",
    database="ny_taxi"
)

cur = conn.cursor()

# Execute the query
query = """
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"
FROM
    yellow_taxi_trips t 
    JOIN zones zpu ON t."PULocationID" = zpu."LocationID"		
    JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 10;
"""

try:
    cur.execute(query)
    results = cur.fetchall()
    
    # Print header
    print(f"{'Pickup DateTime':<20} {'Dropoff DateTime':<20} {'Total Amount':<12} {'Pickup Location':<30} {'Dropoff Location':<30}")
    print("=" * 112)
    
    # Print results
    for row in results:
        print(f"{str(row[0]):<20} {str(row[1]):<20} {str(row[2]):<12} {row[3]:<30} {row[4]:<30}")
    
    print(f"\nTotal rows returned: {len(results)}")
except Exception as e:
    print(f"Error: {e}")
finally:
    cur.close()
    conn.close()
