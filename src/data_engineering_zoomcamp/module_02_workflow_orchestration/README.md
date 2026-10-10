# Workflow Orchestration

Kestra workflows and supporting scripts for the Data Engineering Zoomcamp
Module 2 project.

## Workspace

- `docker-compose.yml` - local Kestra and PostgreSQL services
- `flows/` - Kestra flow definitions
- `scripts/` - supporting Python or shell scripts
- `notebooks/` - exploratory notebooks
- `data/` - local development data

## Start the local services

From this directory, run:

```powershell
docker compose up -d
```

Open Kestra at <http://localhost:18080>.
