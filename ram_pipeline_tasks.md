# Spotify Tracks ETL Pipeline — Project Specification

## Task 1 — Infrastructure & Docker Compose

Modify `docker-compose.yaml` to define services on a shared internal bridge network:

- **Airflow** — DAG orchestration with CeleryExecutor, a shared `/dags` and `/data` volume.
- **Postgres** — Single instance serving as both the Airflow metadata database and the pipeline data store. Expose port `5432`.
- **Redis** — Celery broker. Expose port `6379`.
- **pgAdmin** — Expose on port `5050`. Set `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` via environment variables.

All services share a named Docker network. Use a `depends_on` chain: Airflow services depend on Postgres and Redis.

---

## Task 2 — Extraction & Transformation (Airflow DAG)

In the `load_and_clean_spotify_data` Python callable of the DAG:

1. Read the CSV from `/opt/airflow/data/spotify_tracks.csv` using `pandas.read_csv`.
2. Normalise column names to lowercase.
3. Drop rows missing `id` or `name`. Deduplicate on `id`.
4. Coerce types:
   - `popularity`, `duration_ms` → `int` (fill NaN with 0)
   - `explicit` → `bool` (map `"true"`/`"false"` strings)
   - `genre`, `artists`, `album` → `str`, strip whitespace, fill NaN with `""`
5. Push cleaned records to XCom as a list of dicts.

---

## Task 3 — Postgres Schema & Load

**DDL — run once via PostgresOperator:**

```sql
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
```

**Load task — idempotent insert:**

```sql
INSERT INTO spotify_tracks (id, name, genre, artists, album, popularity, duration_ms, explicit)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING
```

---

## DAG Flow

```
load_spotify_data >> create_table >> insert_spotify_data
```
