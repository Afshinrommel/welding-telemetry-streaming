import json
import time
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ================= Configuration =================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
GEOMETRY_TOPIC = os.getenv("GEOMETRY_TOPIC", "welding_geometry")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "welding_telemetry")
THERMAL_TOPIC = os.getenv("THERMAL_TOPIC", "welding_thermal")

# Real-time physical time step (50 Hz solver output)
STREAM_INTERVAL_SECONDS = float(os.getenv("STREAM_INTERVAL", "0.02"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# Use Docker standard mount path or current directory
DATA_DIR = os.getenv("DATA_DIR", "result_data_welding")
POSSIBLE_DIRS = [DATA_DIR, "."]

# Extension folder paths for subsequent data chunks
EXTENSION_DIR = os.getenv("EXTENSION_DIR", "result_data_welding/extension")
POSSIBLE_EXT_DIRS = [EXTENSION_DIR, "extension", "./extension"]

# Retry configuration for connection
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 30

# ================= Prometheus Metrics =================
MESSAGES_SENT = Counter(
    "welding_producer_messages_sent_total",
    "Total number of messages successfully published to Kafka",
    ["topic"]
)
SEND_ERRORS = Counter(
    "welding_producer_send_errors_total",
    "Total number of Kafka connection or message publish errors",
    ["error_type"]
)
CURRENT_SIM_TIME = Gauge(
    "welding_producer_current_sim_time_seconds",
    "Current physical simulation time step being streamed (seconds)"
)
SEND_LATENCY = Histogram(
    "welding_producer_send_duration_seconds",
    "Latency of publishing a message to Kafka and receiving broker ack",
    ["topic"]
)
FILES_PROCESSED = Counter(
    "welding_producer_files_processed_total",
    "Total number of JSONL simulation files completely streamed"
)
KAFKA_CONNECTED = Gauge(
    "welding_producer_kafka_connected",
    "Kafka broker connection status (1 = connected, 0 = disconnected)"
)

# ================= Utility =================
def retry_delay(attempt):
    """Exponential backoff with an upper limit."""
    return min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)

def find_file(filename):
    """Searches for the target file in standard container paths."""
    for base_dir in POSSIBLE_DIRS:
        if not base_dir:
            continue
        candidate = os.path.join(base_dir, filename)
        if os.path.exists(candidate):
            return candidate
    return None

def find_extension_dir():
    """Searches for the extension directory."""
    for d in POSSIBLE_EXT_DIRS:
        if d and os.path.exists(d) and os.path.isdir(d):
            return d
    return None

# ================= Kafka Producer =================
def create_kafka_producer():
    """Initializes Kafka producer with infinite retry for resilience."""
    attempt = 1
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                api_version=(2, 5, 0),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                max_request_size=10 * 1024 * 1024,
                request_timeout_ms=20000
            )
            KAFKA_CONNECTED.set(1)
            print(f"[Producer] Connected to Kafka broker at {KAFKA_BROKER}.")
            return producer
        except KafkaError as err:
            KAFKA_CONNECTED.set(0)
            SEND_ERRORS.labels(error_type="connection").inc()
            print(f"[Producer] Connection attempt {attempt} failed: {err}")
            delay = retry_delay(attempt)
            print(f"[Producer] Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1

# ================= Streaming Functions =================
def stream_mesh_geometry(producer):
    """Streams base mesh geometry and guarantees delivery."""
    node_file = find_file("kafka_nodes_stream.jsonl")
    topo_file = find_file("mesh_topology.json")

    if node_file:
        print(f"\n--- Phase 1: Ingesting Node-Centric Geometry from {node_file} ---")
        node_count = 0
        with open(node_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                with SEND_LATENCY.labels(topic=GEOMETRY_TOPIC).time():
                    future = producer.send(GEOMETRY_TOPIC, value=json.loads(line))
                    future.get(timeout=10)
                
                MESSAGES_SENT.labels(topic=GEOMETRY_TOPIC).inc()
                node_count += 1
                if node_count % 500 == 0:
                    print(f"Dispatched {node_count} nodes to [{GEOMETRY_TOPIC}]...")
        
        producer.flush()
        FILES_PROCESSED.inc()
        print(f"Base mesh ingestion complete: {node_count} nodes published.\n")
        return

    if topo_file:
        print(f"\n--- Phase 1: Ingesting Mesh Topology from {topo_file} ---")
        with open(topo_file, "r", encoding="utf-8") as f:
            topo_data = json.load(f)
        
        nodes = topo_data.get("nodes", [])
        for node_id, coords in enumerate(nodes):
            payload = {
                "node_id": node_id,
                "coordinates": {"x": coords[0], "y": coords[1], "z": coords[2]}
            }
            with SEND_LATENCY.labels(topic=GEOMETRY_TOPIC).time():
                future = producer.send(GEOMETRY_TOPIC, value=payload)
                future.get(timeout=10)
            MESSAGES_SENT.labels(topic=GEOMETRY_TOPIC).inc()
            
        producer.flush()
        FILES_PROCESSED.inc()
        print(f"Base mesh ingestion complete: {len(nodes)} nodes published from topology.\n")
        return

    raise FileNotFoundError("Critical Error: No base geometry file found.")

def stream_file_content(producer, file_path, target_topic=TELEMETRY_TOPIC, start_step_count=0):
    """Streams a given telemetry/extension file frame-by-frame to the specified topic."""
    print(f"\n--- Streaming from {file_path} to topic [{target_topic}] ---")
    step_count = start_step_count
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            frame_data = json.loads(line)
            sim_time = frame_data.get("time", round(step_count * 0.02, 3))

            try:
                with SEND_LATENCY.labels(topic=target_topic).time():
                    future = producer.send(target_topic, value=frame_data)
                    record_metadata = future.get(timeout=10)
                
                MESSAGES_SENT.labels(topic=target_topic).inc()
                CURRENT_SIM_TIME.set(float(sim_time))
                
                if (step_count + 1) % 10 == 0 or step_count < 5:
                    print(
                        f"Streamed Frame #{step_count+1} (t={sim_time}s) -> "
                        f"Topic: {target_topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}"
                    )
            except Exception as send_err:
                SEND_ERRORS.labels(error_type="publish").inc()
                raise RuntimeError(f"Failed to stream frame at t={sim_time}s: {send_err}")

            step_count += 1
            time.sleep(STREAM_INTERVAL_SECONDS)

    producer.flush()
    FILES_PROCESSED.inc()
    return step_count

def stream_mechanical_telemetry(producer):
    """Streams initial main telemetry first, then monitors extension folder every 15 seconds."""
    processed_files = set()
    
    # 1. Process initial main file
    results_file = find_file("mechanical_results.jsonl")
    if not results_file:
        raise FileNotFoundError("Critical Error: 'mechanical_results.jsonl' not found.")

    step_count = stream_file_content(producer, results_file, target_topic=TELEMETRY_TOPIC, start_step_count=0)
    processed_files.add(os.path.abspath(results_file))
    print(f"Initial main telemetry streaming completed. Total frames: {step_count}\n")

    # 2. Continuous folder monitoring for extension files (triggered ONLY after main file finishes)
    print("--- Entering Monitoring Mode: Watching extension folder every 15s ---")
    while True:
        try:
            ext_dir = find_extension_dir()
            if ext_dir:
                for fname in os.listdir(ext_dir):
                    if fname.endswith(".jsonl"):
                        fpath = os.path.abspath(os.path.join(ext_dir, fname))
                        if fpath not in processed_files:
                            print(f"\n[Watcher] New extension file detected: {fname}")
                            
                            # Generic routing based on filename pattern
                            topic = TELEMETRY_TOPIC
                            if "thermal" in fname.lower():
                                topic = THERMAL_TOPIC
                            
                            step_count = stream_file_content(producer, fpath, target_topic=topic, start_step_count=step_count)
                            processed_files.add(fpath)
                            print(f"[Watcher] Finished streaming {fname}. Total frames processed so far: {step_count}")
            
            # Wait 15 seconds before checking again
            time.sleep(15)
        except Exception as watch_err:
            SEND_ERRORS.labels(error_type="watcher").inc()
            print(f"[Watcher] Error during folder monitoring: {watch_err}")
            time.sleep(15)

# ================= Entry Point =================
def run_pipeline():
    producer = None
    try:
        # Start Prometheus metrics HTTP server
        start_http_server(METRICS_PORT)
        print(f"[Producer] Prometheus metrics server started on port {METRICS_PORT}.")

        producer = create_kafka_producer()
        
        # 1. Ingest physical mesh
        stream_mesh_geometry(producer)
        
        # 2. Brief pause for consumer collection setup
        time.sleep(1.0)
        
        # 3. Stream main results and then start monitoring extension folder
        stream_mechanical_telemetry(producer)

    except KeyboardInterrupt:
        print("\n[Producer] Streaming interrupted by user.")
        sys.exit(0)
    except Exception as exc:
        SEND_ERRORS.labels(error_type="critical").inc()
        print(f"\n[Producer] Critical runtime error: {exc}")
        sys.exit(1)
    finally:
        KAFKA_CONNECTED.set(0)
        if producer is not None:
            producer.flush()
            producer.close()
            print("[Producer] Kafka producer disconnected.")

if __name__ == "__main__":
    run_pipeline()