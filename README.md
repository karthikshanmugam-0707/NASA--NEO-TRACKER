import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_password",
        database="neo_db"
    )

import mysql.connector
from db_config import get_connection

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS asteroids (
        id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(255),
        is_potentially_hazardous BOOLEAN
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS close_approach_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        asteroid_id VARCHAR(20),
        approach_date DATE,
        relative_velocity_kmps FLOAT,
        miss_distance_km FLOAT,
        orbiting_body VARCHAR(50),
        FOREIGN KEY (asteroid_id) REFERENCES asteroids(id)
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables()

import requests
from db_config import get_connection
from datetime import datetime, timedelta

def fetch_and_insert(start_date, end_date):
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key=DEMO_KEY"
    data = requests.get(url).json()

    conn = get_connection()
    cursor = conn.cursor()

    for date in data['near_earth_objects']:
        for obj in data['near_earth_objects'][date]:
            asteroid_id = obj['id']
            name = obj['name']
            hazardous = obj['is_potentially_hazardous_asteroid']

            cursor.execute("INSERT IGNORE INTO asteroids (id, name, is_potentially_hazardous) VALUES (%s, %s, %s)",
                           (asteroid_id, name, hazardous))

            for approach in obj['close_approach_data']:
                rel_vel = float(approach['relative_velocity']['kilometers_per_second'])
                miss_dist = float(approach['miss_distance']['kilometers'])
                orbiting_body = approach['orbiting_body']
                date = approach['close_approach_date']

                cursor.execute("""
                INSERT INTO close_approach_data 
                (asteroid_id, approach_date, relative_velocity_kmps, miss_distance_km, orbiting_body) 
                VALUES (%s, %s, %s, %s, %s)
                """, (asteroid_id, date, rel_vel, miss_dist, orbiting_body))

    conn.commit()
    cursor.close()
    conn.close()

# Example call
if __name__ == "__main__":
    today = datetime.today().date()
    fetch_and_insert(str(today - timedelta(days=2)), str(today))

import streamlit as st
from db_config import get_connection
import pandas as pd

st.set_page_config(page_title="NASA NEO Dashboard", layout="wide")
st.title("🌍 NASA Near-Earth Objects Tracker")

conn = get_connection()

def run_query(query):
    return pd.read_sql(query, conn)

queries = {
    "1. All asteroids": "SELECT * FROM asteroids",
    "2. Potentially hazardous asteroids": "SELECT * FROM asteroids WHERE is_potentially_hazardous = 1",
    "3. All close approach data": "SELECT * FROM close_approach_data",
    "4. Closest approach": """
        SELECT name, MIN(miss_distance_km) as closest 
        FROM asteroids JOIN close_approach_data 
        ON asteroids.id = close_approach_data.asteroid_id 
        GROUP BY name ORDER BY closest LIMIT 1
    """,
    "5. Fastest asteroid": """
        SELECT name, MAX(relative_velocity_kmps) as max_speed 
        FROM asteroids JOIN close_approach_data 
        ON asteroids.id = close_approach_data.asteroid_id 
        GROUP BY name ORDER BY max_speed DESC LIMIT 1
    """,
    "6. Count of hazardous asteroids": "SELECT COUNT(*) FROM asteroids WHERE is_potentially_hazardous = 1",
    "7. Asteroids orbiting Earth": "SELECT * FROM close_approach_data WHERE orbiting_body = 'Earth'",
    "8. Average speed of asteroids": "SELECT AVG(relative_velocity_kmps) AS avg_speed FROM close_approach_data",
    "9. Most recent approach": "SELECT * FROM close_approach_data ORDER BY approach_date DESC LIMIT 1",
    "10. Asteroids with miss distance < 1 million km": "SELECT * FROM close_approach_data WHERE miss_distance_km < 1000000",
    "11. Top 5 fastest approaches": "SELECT * FROM close_approach_data ORDER BY relative_velocity_kmps DESC LIMIT 5",
    "12. Top 5 closest approaches": "SELECT * FROM close_approach_data ORDER BY miss_distance_km ASC LIMIT 5",
    "13. Count of asteroids by orbiting body": "SELECT orbiting_body, COUNT(*) FROM close_approach_data GROUP BY orbiting_body",
    "14. Hazardous asteroids approaching today": f"""
        SELECT * FROM asteroids 
        JOIN close_approach_data ON asteroids.id = close_approach_data.asteroid_id 
        WHERE is_potentially_hazardous = 1 AND approach_date = CURDATE()
    """,
    "15. Number of approaches per asteroid": """
        SELECT name, COUNT(*) AS approach_count 
        FROM asteroids JOIN close_approach_data 
        ON asteroids.id = close_approach_data.asteroid_id 
        GROUP BY name ORDER BY approach_count DESC
    """
}

query_name = st.selectbox("📌 Select a query to run", list(queries.keys()))
df = run_query(queries[query_name])
st.dataframe(df, use_container_width=True)
