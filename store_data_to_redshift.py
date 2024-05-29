# Loading and preprocessing data and connecting to Redshift

import pandas as pd
import psycopg2

# Load the dataset
df = pd.read_csv('C:\Users\hello\OneDrive\Flight Dashboard\airline.csv.shuffle')

# Select relevant columns and preprocess
df = df[['AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'DEPARTURE_DELAY', 'ARRIVAL_DELAY', 'SCHEDULED_DEPARTURE', 'SCHEDULED_ARRIVAL']]

# Calculating average delays
average_delays = df.groupby('AIRLINE').agg({
    'DEPARTURE_DELAY': 'mean',
    'ARRIVAL_DELAY': 'mean'
}).reset_index()

# Connection details
dbname = 'dev'
user = 'admin'
password = 'Flightdashboard2'
host = 'my-flight-redshift-cluster.ctbvnucicxel.us-east-2.redshift.amazonaws.com'
port = '5439'

# Connect to Redshift and store the data
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS average_delays")
cur.execute("""
    CREATE TABLE average_delays (
        AIRLINE VARCHAR(10),
        DEPARTURE_DELAY FLOAT,
        ARRIVAL_DELAY FLOAT
    )
""")
for _, row in average_delays.iterrows():
    cur.execute("""
        INSERT INTO average_delays (AIRLINE, DEPARTURE_DELAY, ARRIVAL_DELAY)
        VALUES (%s, %s, %s)
    """, (row['AIRLINE'], row['DEPARTURE_DELAY'], row['ARRIVAL_DELAY']))
conn.commit()
cur.close()
conn.close()
