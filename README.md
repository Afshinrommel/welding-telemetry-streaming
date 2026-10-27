# 3D Welding Digital Twin - Real-Time Data Streaming Pipeline

An end-to-end, resilient, and fully Dockerized data engineering pipeline designed to stream, persist, and visualize real-time welding process thermomechanical fields (such as temperature, stress, and deformation).

## 🏗️ System Architecture & Pipeline Flow

The entire data streaming infrastructure is orchestrated using a decentralized, containerized topology:

1. **Data Ingestion (Static Dataset Streamer)**: The `producer` container reads baseline historical sensor and simulation data from the `result_data_welding/` directory and streams records line-by-line.

2. **Message Broker Integration**: Data is published asynchronously to the Apache Kafka topic.

3. **Data Consumption & Persistence**: The `consumer` container subscribes to the Kafka broker, decodes the incoming JSON payload streams, and injects the records into a MongoDB instance using an **Upsert (Idempotency)** mechanism to prevent duplicate data storage.

4. **Live Graphical Analytics Display**: A containerized Streamlit dashboard runs on port 8501, fetching real-time data chunks from MongoDB to render an interactive 3D digital twin and thermomechanical variations.

## 📂 Project Repository Structure

```text
my_streaming_project/
│
├── docker-compose.yml       # Complete multi-container orchestration architecture
├── Dockerfile               # Reusable slim image definition for Python services
├── requirements.txt         # Core pipeline dependencies (streamlit, kafka-python, pymongo)
├── .dockerignore            # Filters local caches, virtual environments, and logs
│
├── producer.py              # Robust streaming data producer
├── consumer.py              # Resilient streaming data consumer with MongoDB sync
├── dashboard.py             # Streamlit interactive 3D visualization dashboard
├── result_data_welding/     # Target dataset directory (Requires user data injection)
│   └── extension/           # Subfolder for 100-110s hot-reload dataset
└── welding_simulation/      # Physics simulation scripts (FEniCSx Colab pipeline)
```

## 🛠️ Architectural Configuration Details

1. **Robust Production Ingestion (`producer.py`)**
   Designed to handle static datasets, the producer is configured with `restart: on-failure` in the Docker Compose file. It streams the complete dataset once and then gracefully stops (`EXITED` state). This intentional design prevents infinite execution loops and protects the host machine's disk space from being overwhelmed by repetitive Kafka logs.

2. **Resilient Sync Consumer (`consumer.py`)**
   Features dedicated MongoDB and Kafka connection verification blocks. Converts data directly into structured document payloads before triggering transactional index writing inside the database. It remains running continuously to ensure pipeline stability even if the data stream temporarily stops.

3. **Isolated Storage Layer (`docker-compose.yml`)**
   Orchestrates 6 cohesive services: `mongodb`, `zookeeper`, `kafka`, `telemetry_producer`, `telemetry_consumer`, and `telemetry_dashboard`.
   Binds a persistent named volume to prevent datastore erasure across runtime state recreations. Exposes database port `27017:27017` to the local interface.

4. **Decoupled Monitoring Framework (`dashboard.py`)**
   To eliminate X11 display server dependencies and graphical environment errors inside headless Linux containers, the visualization engine is fully containerized using Streamlit and served securely over port 8501.

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

**Step 1: Initialize the Containerized Streaming Pipeline**
Open a terminal in the root directory of the project and execute the following unified command to build the shared images from scratch and spin up the complete automated ecosystem in detached mode:

```bash
docker compose up -d --build
```

**Step 2: Launch the Dynamic Visualization Dashboard**
Once the containers are running and the producer starts processing data, open your web browser and navigate to the Streamlit dashboard:

```text
http://localhost:8501
```

**Step 3: Graceful Teardown and Resource Cleanup**
To safely terminate all active streams, capture database buffers, and completely clean up the allocated volumes (ensuring a clean slate for the next run), execute:

```bash
docker compose down -v
```