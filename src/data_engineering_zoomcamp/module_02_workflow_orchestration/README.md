# Module 2: Workflow Orchestration with Apache Airflow

This workspace adapts [Data Engineering Zoomcamp Module 2](../../../02-workflow-orchestration/README.md) from Kestra to Apache Airflow. The goal is to learn how to schedule, observe, retry, and backfill reliable pipelines, building on the NYC Taxi ingestion work from Module 1.

The Kestra lessons provide the pipeline requirements; this guide translates their learning objectives into Airflow concepts and exercises.

## Current progress

- Docker Compose is configured for Airflow, defaulting to `apache/airflow:3.3.2` unless overridden by `AIRFLOW_IMAGE_NAME`.
- Two learning DAGs are implemented: `hello_airflow` and `tutorial`.
- [Kestra flows](flows/) remain as course references; Airflow does not execute them.
- Taxi ingestion, the Airflow Connection to the taxi database, and monthly backfills are upcoming exercises.

These are source/configuration findings, not proof of running containers or successful DAG executions. Verify runtime status using the checks below.

## Workspace

| Location | Purpose |
|---|---|
| [docker-compose.yml](docker-compose.yml) | Airflow services, Redis, and metadata PostgreSQL |
| [dags/](dags/) | Python DAG definitions |
| [config/](config/) | Mounted Airflow configuration |
| [logs/](logs/) | Runtime logs; do not commit generated logs |
| [plugins/](plugins/) | Optional Airflow extensions |
| [flows/](flows/) | Kestra course examples |
| [scripts/](scripts/) | Supporting code for future exercises |
| [notebooks/](notebooks/) | Exploratory work |
| [data/](data/) | Local development data |
| [pyproject.toml](pyproject.toml) | Host-side dependencies and development tools |

Compose mounts the DAG, log, configuration, and plugin directories. It does **not** currently mount the data or scripts directories. A host file is not automatically available to a worker.

Installing a package in the host virtual environment does not install it in the Airflow containers. Manage container dependencies separately, normally with an extended Airflow image when additional libraries are needed.

## Architecture

The current configuration uses `CeleryExecutor`.

| Service | Responsibility |
|---|---|
| `airflow-apiserver` | UI and API, published on host port 8080 |
| `airflow-dag-processor` | Parses DAG definitions |
| `airflow-scheduler` | Schedules runs and eligible task instances |
| `airflow-worker` | Executes tasks |
| `airflow-triggerer` | Handles deferred waiting for operators that support it |
| `redis` | Celery message broker |
| `postgres` | PostgreSQL 16 for Airflow metadata |
| `airflow-init` | Metadata initialization/migration and initial UI user creation |

The CLI debug service and Flower monitoring service use optional Compose profiles.

**Airflow metadata is separate from NYC Taxi data.** The `postgres` service uses the `airflow` database and named volume `postgres-db-volume`. This Compose file does not declare Module 1's taxi database or pgAdmin. Connecting to the taxi database requires an explicit network arrangement and Airflow Connection.

Inside an Airflow container, `localhost` refers to that container, not the Windows host or another container.

## Local setup and operation

This is a learning environment, not a production deployment. The following commands are instructions to run deliberately; documenting them does not execute them.

### Prerequisites

- Docker Desktop running Linux containers.
- Docker Compose v2.14 or newer.
- At least 4 GB of memory available to Docker; 8 GB is preferable.
- Host port 8080 available.
- A private environment file beside Compose, or the configured `ENV_FILE_PATH` alternative.

Review the settings below without publishing their secret values:

| Setting | Purpose |
|---|---|
| `AIRFLOW_UID` | Container user ID; Windows can use `50000` |
| `FERNET_KEY` | Valid Fernet key protecting stored connection credentials; retain it across restarts |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Shared secret for API authentication |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Initial UI account |
| `AIRFLOW_IMAGE_NAME` | Optional image override |
| `AIRFLOW_PROJ_DIR` | Optional base directory override for mounts |

Keep the existing environment file and keys when resuming an established installation. Do not put credentials in DAGs, documentation, or Git. Consult the [version-matched Docker guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html) for platform setup.

### Initialize and start

From the repository root:

```powershell
cd src/data_engineering_zoomcamp/module_02_workflow_orchestration
```

For first-time setup, initialize Airflow's metadata database and UI account:

```powershell
docker compose up airflow-init
```

Initialization should exit with code 0. This changes Airflow metadata; it is not a status check.

Start the services:

```powershell
docker compose up -d
docker compose ps --all
```

Open <http://localhost:8080> and use the account configured during initialization. Long-running services should become healthy; an initialization container that exited successfully is expected.

Use the same Compose project name each time. If the original invocation used `-p`, keep using that name. Changing project names can select different networks and named volumes.

### Inspect and stop

Once the services are running:

```powershell
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail 100 airflow-dag-processor
```

Stop without removing containers or volumes:

```powershell
docker compose stop
```

Resume existing containers:

```powershell
docker compose start
```

Do not use volume deletion or Docker pruning as routine troubleshooting. Preserve Airflow metadata and the existing Module 1 PostgreSQL data.

## Existing DAGs

### TaskFlow: hello_airflow

Source: [hello_airflow.py](dags/hello_airflow.py).

```text
extract -> transform -> load
```

- Uses `@dag` and `@task` from `airflow.sdk`.
- Runs manually with `schedule=None` and catchup disabled.
- Extracts a small list, uppercases its strings, and prints the result.
- Passes small return values through TaskFlow/XCom.
- Does not write to PostgreSQL: the task named `load` only prints.

Practice: enable the DAG if paused, trigger a run, and inspect each task's state and logs. Expected final message:

```text
Processed items: ['AIRFLOW', 'DOCKER', 'WINDOWS']
```

**Acceptance criteria:** all three tasks succeed in dependency order, and you can explain how the list moves between them.

### Operators and templating: tutorial

Source: [tutorial.py](dags/tutorial.py).

```text
print_date -> sleep
           -> templated
```

- Uses `BashOperator` and a one-day timedelta schedule.
- Disables catchup despite a start date in 2021.
- Sets one retry by default and a five-minute retry delay; `sleep` overrides this to three retries.
- Demonstrates Jinja date templates and task documentation.
- Makes both downstream tasks eligible after `print_date` succeeds; there is no ordering dependency between them.

New DAGs are configured to start paused. Enabling this DAG allows scheduled runs. A daily timedelta schedule is not a declaration of a fixed wall-clock time such as midnight.

**Acceptance criteria:** explain the dependency graph, inspect rendered template output, and distinguish additional retry attempts from total attempts.

## Kestra concepts mapped to Airflow

Based on the [Kestra concepts lesson](../../../02-workflow-orchestration/04-kestra-concepts.md). These are conceptual correspondences, not exact API translations.

| Kestra concept | Airflow counterpart | Learning point |
|---|---|---|
| Flow | DAG | Define workflow dependencies in Python |
| Task | TaskFlow task or operator instance | Choose a meaningful unit of work |
| Execution | DAG run and task instances | Distinguish definitions from individual executions |
| Inputs | Params and validated run configuration | Supply dataset and processing-period inputs |
| Outputs | XCom and external artifacts | Pass small values or references, not large DataFrames |
| Schedule trigger | DAG schedule/timetable | Define when runs happen and what period they represent |
| Variables | Python configuration, Params, or Airflow Variables | Choose configuration scope deliberately |
| Credentials | Airflow Connections or a secrets backend | Separate secrets from task logic |
| Plugin defaults | `default_args` and explicit operator arguments | Reuse settings such as retry policy |
| Plugins | Providers, hooks, and operators | Select integrations matching the runtime |
| Concurrency limits | Run/task limits and pools | Protect shared resources and control overlap |

Keep downloads and database operations inside task execution, not at DAG import time. Airflow repeatedly parses DAG definitions. See [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/3.3.2/best-practices.html).

## Learning path adapted from Module 2

### 1. Understand orchestration and execute Python

Course references: [orchestration](../../../02-workflow-orchestration/01-what-is-workflow-orchestration.md) and [orchestrating Python](../../../02-workflow-orchestration/05-orchestrate-python-code.md).

Use the existing DAGs to study dependencies, task states, logs, retries, and small data exchanges. Explain the scheduler and worker responsibilities before extending the environment.

### 2. Load one month of taxi data into PostgreSQL

Course reference: [taxi data to PostgreSQL](../../../02-workflow-orchestration/07-load-taxi-data-to-postgres.md).

Reuse the parsing rules and chunked ingestion ideas from [Module 1](../module_01_docker_terraform/ingest_data.py). Separate callable Python functions from the command-line interface before orchestrating them.

Design target, not yet implemented:

```text
select source period -> extract -> load staging -> validate -> publish
```

Begin with one taxi type and one historical month. Use the course's CSV release dataset and verify the source URL. Decide how extracted artifacts become available to workers; the current host data directory is not mounted.

The Module 1 loader replaces its whole target table before appending chunks. Wrapping it in a DAG does not make it safe for multiple months or retries. Compare a transactional replacement of one validated source period with a merge based on a defensible unique key.

**Acceptance criteria:**

- Each run identifies its source file and processing month.
- Rerunning a month does not multiply its rows or erase other months.
- A failed load does not expose a partially published replacement.
- Validation checks schema, required values, and loaded row counts.
- Logs record period, progress, and failures without credentials.

**Design question:** if a worker fails after publication but before Airflow records success, why must the next attempt still be safe?

### 3. Schedule, retry, and backfill

Course reference: [scheduling and backfills](../../../02-workflow-orchestration/08-scheduling-and-backfills.md).

Distinguish the processing period from the time a task happens to execute. Initially use explicit year/month inputs for manual runs. When scheduling, define and test how the timetable's data interval maps to the source month; do not select historical inputs using today's date.

- **Retry:** another attempt at a failed task instance.
- **Catchup:** scheduler creation of missed eligible intervals when enabled.
- **Backfill:** deliberate creation of runs over a historical range.

Start with a small historical range and limited concurrent runs. Shared staging tables and filenames can cause runs to interfere with each other.

**Acceptance criteria:** load two months, rerun one, and verify both remain correct. Introduce a controlled failure with disposable test data and explain recovery. Use the [Airflow 3.3.2 backfill guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/backfill.html) instead of copying Airflow 2 commands.

### 4. Compare ETL and ELT

Course reference: [ETL versus ELT](../../../02-workflow-orchestration/09-etl-vs-elt.md).

ETL transforms data before loading it into the destination. ELT loads it first and transforms it there. Airflow can orchestrate either approach; the orchestrator does not determine where transformations happen.

**Exercise:** justify which taxi processing steps belong in Python and which belong in SQL, considering memory use, validation, and database workload.

### 5. Extend to GCS and BigQuery later

Course references: [GCP setup](../../../02-workflow-orchestration/10-setup-google-cloud-platform.md), [BigQuery loading](../../../02-workflow-orchestration/11-load-taxi-data-to-bigquery.md), and [historical dataset loading](../../../02-workflow-orchestration/12-schedule-and-backfill-full-dataset.md).

After the local pipeline is reliable, explore the Google provider, Airflow Connections, GCS objects, and BigQuery load/transform tasks. Keep source periods and artifact names deterministic. These cloud workflows are not implemented in this workspace.

**Acceptance criteria:** explain authentication, storage layout, cost, and data-quality checks, and show that reruns do not introduce unintended duplicates.

### 6. Optional AI and deployment topics

The course's AI-assisted development and RAG lessons are optional extensions. Kestra's AI Copilot is product-specific; an equivalent is not required for this Airflow module. Use AI for explanations, hints, and review while retaining responsibility for the implementation.

Cloud deployment can follow once local execution, retries, and backfills are understood. It is not required for the initial exercises.

## Troubleshooting

| Symptom | First checks |
|---|---|
| No containers listed | Docker context, module directory, and original Compose project name |
| UI unavailable | API server health and host port 8080 |
| DAG missing | DAG processor logs, import errors, and the DAG directory mount |
| DAG visible but idle | Paused state, schedule, and whether a manual trigger is needed |
| Tasks remain queued | Worker/Redis health and concurrency limits |
| Package import fails | Container dependencies; the host virtual environment is separate |
| Taxi connection fails | Airflow Connection and container networking; metadata PostgreSQL is a different database |
| Rows disappear or duplicate | Publication boundaries, keys, and shared staging before adding retries |

## References

- [Original Module 2 curriculum](../../../02-workflow-orchestration/README.md)
- [Airflow 3.3.2 Docker setup](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html)
- [TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/3.3.2/tutorial/taskflow.html)
- [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/3.3.2/best-practices.html)
- [Backfills](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/backfill.html)

If the image is overridden or upgraded, use documentation matching the version reported inside the containers.
