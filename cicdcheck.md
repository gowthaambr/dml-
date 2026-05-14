# CI/CD Check Notes

## CI workflow
Runs on push/PR to `main`:
1. Checkout code
2. Build Docker Compose services
3. Start stack (`docker compose up -d`)
4. Validate DAGs: `airflow dags list` — must include `spotify_tracks_etl`

## CD workflow
Triggers after CI succeeds on `main` (self-hosted runner):
1. Checkout code at the triggering SHA
2. `docker compose down`
3. `docker compose up -d --build`

## Airflow connection required
Connection ID: `spotify_connection`  
Type: Postgres  
Host: `postgres` | Port: `5432` | Login: `airflow` | Password: `airflow` | Schema: `airflow`
