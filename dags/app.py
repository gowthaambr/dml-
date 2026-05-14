
# DAG: Spotify Tracks ETL Pipeline
# Tasks:
#   1) load_spotify_data   - read CSV, clean, transform  (Extract + Transform)
#   2) create_table        - create spotify_tracks table in Postgres (PostgresOperator)
#   3) insert_spotify_data - insert cleaned records into Postgres (Load)

from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

CSV_PATH = "/opt/airflow/data/spotify_tracks.csv"


def load_and_clean_spotify_data(ti):
    df = pd.read_csv(CSV_PATH)

    # Normalise column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Drop rows missing the key identifier fields
    df.dropna(subset=["id", "name"], inplace=True)
    df.drop_duplicates(subset=["id"], inplace=True)

    # Type coercions
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce").fillna(0).astype(int)
    df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce").fillna(0).astype(int)
    df["explicit"] = df["explicit"].astype(str).str.lower().map(
        {"true": True, "false": False, "1": True, "0": False}
    ).fillna(False)

    # Fill remaining nulls with empty string for text columns
    for col in ["genre", "artists", "album"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    ti.xcom_push(key="spotify_data", value=df.to_dict("records"))


def insert_spotify_data(ti):
    records = ti.xcom_pull(key="spotify_data", task_ids="load_spotify_data")
    if not records:
        raise ValueError("No Spotify data received from XCom")

    hook = PostgresHook(postgres_conn_id="spotify_connection")
    insert_sql = """
        INSERT INTO spotify_tracks (id, name, genre, artists, album, popularity, duration_ms, explicit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """
    for row in records:
        hook.run(
            insert_sql,
            parameters=(
                row["id"],
                row["name"],
                row["genre"],
                row["artists"],
                row["album"],
                row["popularity"],
                row["duration_ms"],
                row["explicit"],
            ),
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spotify_tracks_etl",
    default_args=default_args,
    description="ETL pipeline: load Spotify tracks CSV, clean, and store in Postgres",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

load_spotify_data_task = PythonOperator(
    task_id="load_spotify_data",
    python_callable=load_and_clean_spotify_data,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="spotify_connection",
    sql="""
        CREATE TABLE IF NOT EXISTS spotify_tracks (
            id          VARCHAR PRIMARY KEY,
            name        TEXT NOT NULL,
            genre       TEXT,
            artists     TEXT,
            album       TEXT,
            popularity  INTEGER,
            duration_ms INTEGER,
            explicit    BOOLEAN
        );
    """,
    dag=dag,
)

insert_spotify_data_task = PythonOperator(
    task_id="insert_spotify_data",
    python_callable=insert_spotify_data,
    dag=dag,
)

load_spotify_data_task >> create_table_task >> insert_spotify_data_task
