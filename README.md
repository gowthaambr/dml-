
# Spotify Tracks Data Pipeline
-----------

## Create a virtual environment and activate it (optional)
"""

    python -m venv venv
    source venv/bin/activate

"""


# Important links and Code
-----

## Install Airflow 
-----

Follow steps in the link - https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

## Install PGAdmin 
-----
Code to add in yaml file 


"""

    postgres:
        image: postgres:13
        environment:
          POSTGRES_USER: airflow
          POSTGRES_PASSWORD: airflow
          POSTGRES_DB: airflow
        volumes:
          - postgres-db-volume:/var/lib/postgresql/data
        healthcheck:
          test: ["CMD", "pg_isready", "-U", "airflow"]
          interval: 10s
          retries: 5
          start_period: 5s
        restart: always
        ports:
          - "5432:5432"
    
    pgadmin:

        container_name: pgadmin4_container2
        
        image: dpage/pgadmin4
        
        restart: always
        
        environment:
        
          PGADMIN_DEFAULT_EMAIL: admin@admin.com
          PGADMIN_DEFAULT_PASSWORD: root
          
        ports:
          - "5050:80"
"""

-----

# Pipeline Design

## ETL Flow

```
load_spotify_data (Extract + Transform) >> create_table >> insert_spotify_data (Load)
```

- **Extract + Transform**: Reads `spotify_tracks.csv`, drops nulls/duplicates, coerces types
- **Create Table**: Creates `spotify_tracks` table in Postgres if not exists
- **Load**: Inserts all cleaned records with `ON CONFLICT (id) DO NOTHING`

## Dataset columns

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR (PK) | Spotify track ID |
| name | TEXT | Track name |
| genre | TEXT | Music genre |
| artists | TEXT | Artist name(s) |
| album | TEXT | Album name |
| popularity | INTEGER | Popularity score (0-100) |
| duration_ms | INTEGER | Track duration in milliseconds |
| explicit | BOOLEAN | Explicit content flag |

-----
-----

# CI/CD Workflow Demo
- Automated with GitHub Actions
