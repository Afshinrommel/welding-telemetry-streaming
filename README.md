# 3D Welding Digital Twin - Simulation Data Streaming Pipeline

A Dockerized data streaming pipeline for reading pre-computed 3D welding thermomechanical simulation outputs (temperature, stress, and deformation), streaming records through Apache Kafka, persisting them in MongoDB, and inspecting them via Streamlit, Prometheus, and Grafana.

## System Architecture & Pipeline Flow

The pipeline is orchestrated with Docker Compose using the following components:

1. **Data Ingestion (`producer`)**: Reads pre-computed numerical simulation files from `result_data_welding/` (and `result_data_welding/extension/` when present) and publishes records line-by-line to Apache Kafka topics.

2. **Message Broker (`kafka` & `zookeeper`)**: Buffers and transports simulation telemetry and mesh geometry messages between the producer and consumer.

3. **Data Consumption & Persistence (`consumer` & `mongodb`)**: Subscribes to Kafka topics, decodes JSON payloads, and persists records into MongoDB using upsert idempotency (`$setOnInsert` with a unique compound index) to avoid duplicate records on replay.

4. **Visualization Interface (`dashboard`)**: A containerized Streamlit application on port `8501` that queries stored simulation records from MongoDB to render 3D mesh and thermomechanical plots.

5. **Pipeline Monitoring (`prometheus` & `grafana`)**: Prometheus (`:9090`) scrapes metrics exposed by the producer and consumer, and Grafana (`:3000`) displays dashboards provisioned via `setup_grafana.py`.

## Project Repository Structure

```text
welding-telemetry-streaming/
│
├── docker-compose.yml               # Multi-container service and volume definitions
├── Dockerfile                       # Container image definition for Python services
├── requirements.txt                 # Python dependencies (streamlit, kafka-python, pymongo)
├── .dockerignore                    # Files excluded from Docker build context
│
├── producer.py                      # Simulation dataset reader and Kafka producer
├── consumer.py                      # Kafka consumer and MongoDB writer
├── dashboard.py                     # Streamlit 3D visualization interface for stored simulation data
├── setup_grafana.py                 # Script for generating Grafana provisioning files and dashboards
├── prometheus.yml                   # Prometheus scrape configuration
│
├── run_failure_recovery_test.py     # Interactive Appendix B failure and recovery test controller
├── recovery_test.ps1                # PowerShell script testing Kafka buffering & consumer replay after MongoDB outage
├── run_pipeline.ps1                 # PowerShell script to reset, provision Grafana, start containers, and check health
│
├── grafana/                         # Generated Grafana provisioning files and dashboard JSONs
│   ├── provisioning/                # Datasource and dashboard provisioning definitions
│   └── dashboards/                  # Provisioned Grafana dashboard definitions
│
├── docs/
│   └── screenshots/                 # Appendix B verification log and stage screenshots
│       ├── appendix_b_failure_recovery_report.log
│       ├── stage1/                  # Normal operation baseline dashboards
│       ├── stage2/                  # MongoDB outage & connection drop dashboards
│       ├── stage3/                  # Processing interruption & latency spike dashboards
│       └── stage4/                  # Post-recovery & resumed processing dashboards
│
├── result_data_welding/             # Numerical simulation dataset directory
│   ├── extension/                   # Optional 100-110s simulation extension files
│   └── recovery_test/               # Isolated local test outputs (excluded from source checksums)
│
├── tests/                           # Automated verification scripts
│   ├── test_idempotency.py          # Concurrency & duplicate-handling verification
│   └── test_fault_tolerance.py      # Simulated DB outage & offset replay verification
│
└── welding_simulation/              # FEniCSx simulation notebook/scripts used to generate dataset files
```

## Architectural Configuration Details

1. **Finite Dataset Ingestion (`producer.py`)**
   Configured with `restart: on-failure` in `docker-compose.yml`, the producer streams the pre-computed simulation dataset once and then exits cleanly (`EXITED` state) rather than looping indefinitely over static simulation files.

2. **Idempotent Consumer (`consumer.py`)**
   Enforces ingestion idempotency using a MongoDB unique compound index (`node_id`, `time_step`) combined with atomic `$setOnInsert` operations. Replayed or redelivered Kafka messages after service interruptions do not create duplicate entries, while the consumer stays active to wait for subsequent messages.

3. **Container & Storage Setup (`docker-compose.yml`)**
   Manages 8 services (`mongodb`, `zookeeper`, `kafka`, `telemetry_producer`, `telemetry_consumer`, `telemetry_dashboard`, `prometheus`, and `grafana`) and attaches named volumes so persisted MongoDB data survives container restarts.

4. **Browser-Based Simulation Viewer (`dashboard.py`)**
   Runs Streamlit inside a headless Linux container on port `8501`, reading persisted simulation chunks from MongoDB instead of connecting to physical welding hardware or live sensors.

5. **Automated Monitoring Setup (`setup_grafana.py`)**
   Generates local Grafana provisioning directories before startup so Docker Compose mounts the Prometheus datasource and pre-configured dashboards without manual GUI setup.

## Connecting the Simulation Data

Before starting the pipeline, place the numerical simulation files into `result_data_welding/`:

### Option A: Use the Pre-Computed Simulation Datasets

Download the pre-computed simulation files and place them in the directories below:

1. **Baseline Simulation Dataset (0–100 s):** Extract the contents directly into `result_data_welding/`.
   [Download Baseline Dataset](https://drive.google.com/drive/folders/18R-EeZBpHHTf9Dpv04cxxzvRVQwzE-xS?usp=sharing)

2. **Extension Simulation Dataset (100–110 s):** Extract the contents into `result_data_welding/extension/` to test directory monitoring for additional simulation steps.
   [Download Extension Dataset](https://drive.google.com/drive/folders/1kLc61-AOx-XbLDPz5LNR2HHdLOIofLvM?usp=drive_link)

### Option B: Generate Data via FEniCSx Simulation

If you run the FEniCSx simulation yourself using `WELD.ipynb` inside `welding_simulation/`:

1. Create `result_data_welding/` in the project root directory.
2. Copy the generated JSONL files (such as `thermal_series.jsonl` and `mechanical_results.jsonl`) from your simulation output folder into `result_data_welding/`.

## Running the Pipeline

Follow these sequential steps to prepare, start, and verify the pipeline:

**Prerequisites**

- Docker Desktop installed and running.
- Python installed locally to run the setup and verification scripts.

**Step 1: Provision Grafana Dashboards**
Before starting the containers, generate the Grafana provisioning files by running:

```bash
python setup_grafana.py
```
*(On Windows PowerShell, `.\run_pipeline.ps1` performs a clean reset with `docker compose down -v`, runs `setup_grafana.py`, starts all services with `docker compose up -d --build`, checks HTTP readiness and Prometheus targets, and opens Grafana and Streamlit in the browser.)*

**Step 2: Build and Start Containers**
Build the images and start the containers in detached mode:

```bash
docker compose up -d --build
```

**Step 3: Open the Dashboards**
Once the containers are running and the producer starts streaming simulation records, open the two web interfaces:

- **Simulation Viewer (Streamlit):** `http://localhost:8501`
- **Metrics & Monitoring (Grafana):** `http://localhost:3000` (Login: `admin` / `admin`)
  *(Open **Dashboards** to inspect **Pipeline Performance**, **Data Persistence & Recovery**, and **Service Health & Errors**.)*

**Step 4: Run Verification and Recovery Tests**
Run the test suites against the active containers to verify idempotency, Kafka buffering, and failure recovery:

```bash
# 1. Verify ingestion idempotency and duplicate prevention
python tests/test_idempotency.py

# 2. Verify fault tolerance and message replay under simulated DB outage
python -W ignore tests/test_fault_tolerance.py

# 3. Run the interactive Appendix B failure & recovery test
python run_failure_recovery_test.py
```
*(On Windows PowerShell, `.\recovery_test.ps1` can also be executed to inject 20 isolated test records into Kafka while MongoDB and the consumer are stopped, verifying complete recovery after restart.)*

**Step 5: Stop Containers and Remove Volumes**
To stop the services and clear the Docker volumes before a fresh run, execute:

```bash
docker compose down -v
```

## Appendix B: Failure & Recovery Verification

To evaluate pipeline behavior during a database outage without modifying `producer.py`, `consumer.py`, Docker Compose services, or source simulation files, a controlled outage-and-recovery test (`Test ID: APPB-20261120_192023`, total wall time `812.23s`) was executed using `run_failure_recovery_test.py`.

### Execution Summary (`RESULT: PASS`)

| Verification Criterion | Status | Observed Evidence (`APPB-20261120_192023`) |
| :--- | :--- | :--- |
| **MongoDB Failure Observed** | `PASS` | `docker compose stop mongodb` triggered consumer interruption and `welding_consumer_mongodb_connected = 0` within **`11.69s`**. |
| **Producer Continuity During Outage** | `PASS` | Producer remained `UP (1)` and connected to Kafka (`1`), sending telemetry messages from `269.0` to `501.0` (**`+232.0` messages** buffered in Kafka). |
| **MongoDB Restoration** | `PASS` | `docker compose start mongodb` restored database availability (`db.adminCommand({ping:1}).ok == 1`). |
| **Consumer Recovery & Replay** | `PASS` | Docker restart policy and consumer retry logic re-established MongoDB/Kafka connections and resumed processing within **`16.66s`** (`welding_consumer_mongodb_connected = 1`, `up = 1`). |
| **MongoDB Persistence Comparison** | `PASS` | `welding_telemetry` increased from `277` to `337` documents (**`+60`**), while `mesh_geometry` remained unchanged at `16,226` (total non-system documents: `16,503` $\rightarrow$ `16,563`). |
| **Source Data Integrity** | `PASS` | All 9 source files under `result_data_welding/` (excluding `recovery_test/`) kept their pre-test SHA-256 checksums with zero missing, modified, or unexpected files. |

Full execution trace: [`docs/screenshots/appendix_b_failure_recovery_report.log`](docs/screenshots/appendix_b_failure_recovery_report.log)

### Stage-by-Stage Dashboard Captures

Each stage directory under `docs/screenshots/` contains captures of the three Grafana dashboards (`Data Persistence & Recovery`, `Pipeline Performance`, and `Service Health & Errors`) over a 15-minute window:

1. **Stage 1 — Normal Operation Baseline (`docs/screenshots/stage1/`)**
   - `data_persistence.png`: Baseline MongoDB average write latency (`0.0636s`) and steady Kafka offset progression.
   - `pipeline_performance.png`: Synchronized producer and consumer throughput with minimal backlog (`2` messages).
   - `service_health.png`: Both `welding_producer` and `welding_consumer` scrape targets `UP`, with all Kafka and MongoDB connections `CONNECTED`.

2. **Stage 2 — MongoDB Failure Injection (`docs/screenshots/stage2/`)**
   - `data_persistence.png`: Average write latency increases (`0.278s`), committed Kafka offsets flatten, and time since last successful consumer processing rises.
   - `pipeline_performance.png`: Consumer throughput drops to zero while producer continues advancing simulation progress.
   - `service_health.png`: `Consumer - MongoDB` and `Consumer - Kafka` switch to `DISCONNECTED` and error counters (`mongo_connect`, `persistence_or_broker`) appear, while `Producer - Kafka` remains `CONNECTED`.

3. **Stage 3 — Continued Outage & Processing Interruption (`docs/screenshots/stage3/`)**
   - `data_persistence.png`: Average write latency reaches `5.06s` and time since last successful consumer processing continues increasing linearly while uncommitted offsets remain in Kafka.
   - `pipeline_performance.png`: Producer simulation progress continues uninterrupted while consumer processing stays halted.
   - `service_health.png`: Consumer connection errors continue accumulating while the producer remains unaffected.

4. **Stage 4 — Service Restoration & Recovery (`docs/screenshots/stage4/`)**
   - `data_persistence.png`: MongoDB average write latency returns to `0.0381s` as consumer processing resumes.
   - `pipeline_performance.png`: Consumer simulation progress catches up alongside active process memory usage.
   - `service_health.png`: All connection indicators (`Producer - Kafka`, `Consumer - Kafka`, and `Consumer - MongoDB`) return to `CONNECTED`.

> **Note on `Estimated Unprocessed Messages`:** During Stage 4 (`docs/screenshots/stage4/pipeline_performance.png`), the `Estimated Unprocessed Messages` panel displays `16504`. This panel computes a simple difference between cumulative sent and in-process consumer counters rather than true Kafka consumer group lag; restarting the consumer resets its in-memory Prometheus counter (`counter_reset_detected=True`), whereas committed Kafka offsets, MongoDB document counts (`+60` new `welding_telemetry` records), and `CONNECTED` health gauges confirm successful recovery and replay.