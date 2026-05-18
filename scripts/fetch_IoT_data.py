import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import psycopg2

import sys

root = Path(__file__).parent.joinpath("../").resolve()
sys.path.append(str(root))
from utils import add_target


load_dotenv(Path(__file__).parent.parent / ".env")

DB_CONFIG = {
    "host": os.getenv("PSQL_SERVER"),
    "dbname": os.getenv("PSQL_DATABASE"),
    "user": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    "port": int(os.getenv("PSQL_PORT")),
    "sslmode": "require",
}


def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to database successfully")
        return conn
    except psycopg2.Error as exc:
        print(f"Connection failed, error: {exc}")
        raise Exception("Failed to connect to database")


def fetch_iot_data(conn):
    try:
        df = pd.read_sql_query(
            "SELECT time, temperature, humidity, wind_direction, wind_speed, precipitation, light "
            'FROM sep4dk1."Weather" ORDER BY time',
            conn,
        )
        print(f"Fetched {len(df)} rows")

        df = add_target(
            df,
            [
                "temperature",
                "humidity",
                "wind_direction",
                "wind_speed",
                "precipitation",
                "light",
            ],
            days_range=7,
        )
        write_to_parquet(df, Path(__file__).parent.parent / "data" / "IoT_data.parquet")

    except psycopg2.Error as exc:
        print(f"Query failed, error: {exc}")
        raise Exception("Failed to fetch IoT data")
    finally:
        conn.close()


def write_to_parquet(df: pd.DataFrame, output_path: Path):
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"Saved {len(df)} rows to {output_path}")
    except Exception as exc:
        print(f"Failed to write to parquet, error: {exc}")
        raise Exception("Failed to write IoT data to parquet")


def main():
    print("Connecting to PostgreSQL database...")
    print(f"   Host: {DB_CONFIG['host']}")
    print(f"   User: {DB_CONFIG['user']}")
    print(f"   Database: {DB_CONFIG['dbname']}")

    conn = connect_to_db()

    fetch_iot_data(conn)


if __name__ == "__main__":
    main()
