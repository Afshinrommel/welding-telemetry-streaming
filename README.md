# 3D Welding Digital Twin - Real-Time Data Streaming Pipeline

An end-to-end, resilient, and fully Dockerized data engineering pipeline designed to stream, persist, and visualize real-time welding process thermomechanical fields (such as temperature, stress, and deformation), coupled with automated zero-touch infrastructure observability.

## 🏗️ System Architecture & Pipeline Flow

The entire data streaming infrastructure is orchestrated using a decentralized, containerized topology:

1. **Data Ingestion (Static Dataset Streamer)**: The `producer` container reads baseline historical sensor and simulation data from the `result_data_welding/` directory and streams records line-by-line.

2. **Message Broker Integration**: Data is published asynchronously to the Apache Kafka topic.

3. **Data Consumption & Persistence**: The `consumer` container subscribes to the Kafka broker, decodes incoming JSON payload streams, and persists the records into MongoDB using an **Upsert-based Idempotency** mechanism (`$setOnInsert` coupled with a compound unique key) to guarantee zero duplication.

4. **Live Graphical Analytics Display**: A containerized Streamlit dashboard runs on port 8501, fetching real-time data chunks from MongoDB to render an interactive 3D digital twin and thermomechanical variations.

5. **Infrastructure Observability (Prometheus & Grafana)**: A dedicated monitoring layer tracks system health, producer/consumer throughput, Kafka lag, and MongoDB write latency in real-time, deployed automatically via an Infrastructure as Code (IaC) approach.

## 📂 Project Repository Structure

```text
welding-telemetry-streaming/
│
├── docker-compose.yml               # Complete multi-container orchestration architecture
├── Dockerfile                       # Reusable slim image definition for Python services
├── requirements.txt                 # Core pipeline dependencies (streamlit, kafka-python, pymongo)
├── .dockerignore                    # Filters local caches, virtual environments, and logs
│
├── producer.py                      # Robust streaming data producer
├── consumer.py                      # Resilient streaming data consumer with MongoDB sync
├── dashboard.py                     # Streamlit interactive 3D visualization dashboard
├── setup_grafana.py                 # IaC script for automated Grafana provisioning
├── prometheus.yml                   # Prometheus scraping configuration
│
├── run_failure_recovery_test.py     # Appendix B interactive failure and recovery test controller
├── recovery_test.ps1                # PowerShell Kafka buffering & MongoDB outage replay test
├── run_pipeline.ps1                 # PowerShell end-to-end stack bootstrap & health-check script
│
├── grafana/                         # Auto-generated Grafana configuration volumes
│   ├── provisioning/                # Auto-provisioned datasources and dashboard specs
│   └── dashboards/                  # Pre-built JSON dashboards
│
├── docs/
│   └── screenshots/                 # Verified Appendix B test artifacts
│       ├── appendix_b_failure_recovery_report.log
│       ├── stage1/                  # Normal operation baseline dashboards
│       ├── stage2/                  # MongoDB outage & connection drop dashboards
│       ├── stage3/                  # Processing interruption & latency spike dashboards
│       └── stage4/                  # Post-recovery & resumed processing dashboards
│
├── result_data_welding/             # Target dataset directory (Requires user data injection)
│   ├── extension/                   # Subfolder for 100-110s hot-reload dataset
│   └── recovery_test/               # Isolated local test outputs (excluded from source checksums)
│
├── tests/                           # Verification and resilience test suites
│   ├── test_idempotency.py          # Concurrency & race condition validation
│   └── test_fault_tolerance.py      # Simulated DB outage & explicit offset replay audit
│
└── welding_simulation/              # Physics simulation scripts (FEniCSx Colab pipeline)
```

## 🛠️ Architectural Configuration Details

1. **Robust Production Ingestion (`producer.py`)**
   Designed to handle static datasets, the producer is configured with `restart: on-failure` in the Docker Compose file. It streams the complete dataset once and then gracefully stops (`EXITED` state). This intentional design prevents infinite execution loops and protects the host machine's disk space from being overwhelmed by repetitive Kafka logs.

2. **Resilient Sync Consumer (`consumer.py`)**
   Enforces ingestion idempotency via a MongoDB unique compound index (`node_id`, `time_step`) combined with atomic `$setOnInsert` operations. This ensures that duplicate telemetry frames or message redeliveries during network partitions cannot corrupt baseline physical states or create redundant records. It remains running continuously to ensure pipeline stability even if the upstream data stream temporarily pauses.

3. **Isolated Storage Layer (`docker-compose.yml`)**
   Orchestrates 8 cohesive services: `mongodb`, `zookeeper`, `kafka`, `telemetry_producer`, `telemetry_consumer`, `telemetry_dashboard`, `prometheus`, and `grafana`. Binds persistent named volumes to prevent datastore erasure across runtime state recreations.

4. **Decoupled Monitoring Framework (`dashboard.py`)**
   To eliminate X11 display server dependencies and graphical environment errors inside headless Linux containers, the visualization engine is fully containerized using Streamlit and served securely over port 8501.

5. **Zero-Touch Observability (IaC)**
   Grafana and Prometheus are deployed using an Infrastructure as Code strategy. By running `setup_grafana.py`, local provisioning folders are dynamically built. When `docker-compose up` is executed, Grafana mounts these volumes and automatically connects to the Prometheus data source and pre-loads the Grafana monitoring dashboards without manual GUI configuration.

## 🔌 Connecting the Simulation Data

Before running the pipeline, you must provide the dataset for the producer to stream. You can either run the simulation yourself or use the pre-computed full-scale datasets.

### Option A: Use the Provided Full-Scale Datasets (Recommended)

Download the pre-computed 100-second simulation datasets from Google Drive and place them in the correct directories:

1. **Baseline Dataset (0–100 s):** Extract the contents of this link directly into the `result_data_welding/` folder.
   [Download Baseline Dataset](https://drive.google.com/drive/folders/18R-EeZBpHHTf9Dpv04cxxzvRVQwzE-xS?usp=sharing)

2. **Extension Dataset (100–110 s):** Extract the contents of this link into a subfolder named `result_data_welding/extension/` to test directory monitoring and hot-reloading.
   [Download Extension Dataset](https://drive.google.com/drive/folders/1kLc61-AOx-XbLDPz5LNR2HHdLOIofLvM?usp=drive_link)

### Option B: Generate Data via Simulation

If you ran the FEniCSx simulation yourself using the provided `WELD.ipynb` notebook (located in the `welding_simulation/` folder):

1. Create a folder named `result_data_welding/` in the root directory.
2. Move the generated files (e.g., `thermal_series.jsonl` and `mechanical_results.jsonl`) from your Colab `results/` folder into the newly created `result_data_welding/` directory.

## 🚀 Deployment Instructions

Follow these sequential steps to compile, initialize, and execute the entire pipeline framework:

**Prerequisites**

* Docker Desktop installed and running.
* Python installed locally (to run the setup and verification scripts).

**Step 1: Provision Grafana Dashboards**
Before starting the containers, generate the automated Grafana configurations by running the provided setup script in your root directory:

```bash
python setup_grafana.py
```
*(Alternatively, on Windows PowerShell, you can execute `.\run_pipeline.ps1` to perform a clean reset, provision Grafana, build/start all containers, verify HTTP/Prometheus readiness, and launch the browser dashboards automatically.)*

**Step 2: Initialize the Containerized Ecosystem**
Execute the following unified command to build the shared images from scratch and spin up the complete automated ecosystem in detached mode:

```bash
docker compose up -d --build
```

**Step 3: Access the Dynamic Dashboards**
Once the containers are running and the producer starts processing data, open your web browser to access the dual-dashboard interfaces:

* **Engineering Digital Twin (Streamlit):** `http://localhost:8501`
* **System Observability & Monitoring (Grafana):** `http://localhost:3000` (Login: `admin` / `admin`)
  *(Navigate to **Dashboards** to inspect **Pipeline Performance**, **Data Persistence & Recovery**, and **Service Health & Errors**).*

**Step 4: Run Automated Pipeline Audits & Recovery Tests**
Execute the verification suites against the active infrastructure to validate idempotency, Kafka buffering, and failure recovery:

```bash
# 1. Verify ingestion idempotency and race condition handling
python tests/test_idempotency.py

# 2. Verify fault tolerance and message replay under simulated DB outage
python -W ignore tests/test_fault_tolerance.py

# 3. Run the end-to-end interactive Appendix B failure & recovery test
python run_failure_recovery_test.py
```
*(On Windows PowerShell, `.\recovery_test.ps1` can also be used to inject 20 isolated test events into Kafka while MongoDB and the Consumer are stopped, verifying complete message replay once services restart.)*

**Step 5: Graceful Teardown and Resource Cleanup**
To safely terminate all active streams, capture database buffers, and completely clean up the allocated volumes (ensuring a clean slate for the next run), execute:

```bash
docker compose down -v
```

## 🧪 Appendix B: Interactive Failure & Recovery Verification

To validate end-to-end fault tolerance without modifying `producer.py`, `consumer.py`, Docker Compose services, or source simulation datasets, a controlled outage-and-recovery experiment (`Test ID: APPB-20261120_192023`, total wall time `812.23s`) was executed via `run_failure_recovery_test.py`.

### Verified Execution Summary (`RESULT: PASS`)

| Verification Criterion | Status | Observed Evidence (`APPB-20261120_192023`) |
| :--- | :--- | :--- |
| **MongoDB Failure Observed** | `PASS` | `docker compose stop mongodb` triggered observable Consumer interruption and `welding_consumer_mongodb_connected = 0` within **`11.69s`**. |
| **Producer Continuity During Outage** | `PASS` | Producer remained `UP (1)` and connected to Kafka (`1`), monotonically increasing sent telemetry messages from `269.0` to `501.0` (**`+232.0` messages** buffered in Kafka). |
| **MongoDB Restoration** | `PASS` | `docker compose start mongodb` restored database availability (`db.adminCommand({ping:1}).ok == 1`). |
| **Consumer Recovery & Replay** | `PASS` | Docker restart policy and Consumer retry logic re-established MongoDB/Kafka connections and resumed processing within **`16.66s`** (`welding_consumer_mongodb_connected = 1`, `up = 1`). |
| **MongoDB Persistence Comparison** | `PASS` | `welding_telemetry` increased from `277` to `337` documents (**`+60`**), while `mesh_geometry` remained unchanged at `16,226` (total non-system documents: `16,503` $\rightarrow$ `16,563`). |
| **Source Data Integrity** | `PASS` | All 9 source files under `result_data_welding/` (excluding `recovery_test/`) matched their pre-test SHA-256 checksums with zero missing, modified, or unexpected files. |

Full execution trace: [`docs/screenshots/appendix_b_failure_recovery_report.log`](docs/screenshots/appendix_b_failure_recovery_report.log)

### Stage-by-Stage Observability Evidence

Each stage under `docs/screenshots/` captures the three Grafana dashboards (`Data Persistence & Recovery`, `Pipeline Performance`, and `Service Health & Errors`) over a 15-minute window:

1. **Stage 1 — Normal Operation Baseline (`docs/screenshots/stage1/`)**
   * `data_persistence.png`: Low baseline MongoDB average write latency (`0.0636s`) and steady Kafka offset progression.
   * `pipeline_performance.png`: Synchronized Producer and Consumer throughput with minimal unprocessed backlog (`2` messages).
   * `service_health.png`: Both `welding_producer` and `welding_consumer` scrape targets `UP`, with all Kafka and MongoDB connections `CONNECTED`.

2. **Stage 2 — MongoDB Failure Injection (`docs/screenshots/stage2/`)**
   * `data_persistence.png`: Write latency rises (`0.278s`), committed Kafka offsets flatten, and time since last successful Consumer processing climbs.
   * `pipeline_performance.png`: Consumer throughput drops to zero while Producer continues advancing simulation progress.
   * `service_health.png`: `Consumer - MongoDB` and `Consumer - Kafka` transition to `DISCONNECTED` and error counters (`mongo_connect`, `persistence_or_broker`) appear, while `Producer - Kafka` remains `CONNECTED`.

3. **Stage 3 — Continued Outage & Processing Interruption (`docs/screenshots/stage3/`)**
   * `data_persistence.png`: Average write latency peaks at `5.06s` and time since last successful Consumer processing continues increasing linearly while uncommitted offsets remain preserved in Kafka.
   * `pipeline_performance.png`: Producer simulation progress continues uninterrupted while Consumer processing remains halted.
   * `service_health.png`: Consumer connection errors continue accumulating while the Producer remains unaffected.

4. **Stage 4 — Service Restoration & Automated Recovery (`docs/screenshots/stage4/`)**
   * `data_persistence.png`: MongoDB average write latency returns to normal (`0.0381s`) as Consumer processing resumes.
   * `pipeline_performance.png`: Consumer simulation progress catches up alongside active process memory activity.
   * `service_health.png`: All internal health indicators (`Producer - Kafka`, `Consumer - Kafka`, and `Consumer - MongoDB`) return to `CONNECTED`.

> **Interpretation Note on `Estimated Unprocessed Messages`:** During Stage 4 (`docs/screenshots/stage4/pipeline_performance.png`), the `Estimated Unprocessed Messages` panel displays `16504`. As documented in the test report, this panel computes a raw difference between cumulative sent and in-process Consumer counters rather than true Kafka consumer group lag; container restart resets the Consumer's in-memory Prometheus counter (`counter_reset_detected=True`), whereas committed Kafka offsets, MongoDB document counts (`+60` new `welding_telemetry` records), and `CONNECTED` health gauges confirm successful recovery and replay.