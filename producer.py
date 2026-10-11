import json
import time
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ================= Configuration =================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
GEOMETRY_TOPIC = os.getenv("GEOMETRY_TOPIC", "welding_geometry")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "welding_telemetry")

# Real-time physical time step (50 Hz solver output)
STREAM_INTERVAL_SECONDS = float(os.getenv("STREAM_INTERVAL", "0.02"))

# Use Docker standard mount path or current directory
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
POSSIBLE_DIRS = [DATA_DIR, "."]

# Retry configuration for connection
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 30

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
                retries=5, # Internal Kafka client retries
                max_request_size=10 * 1024 * 1024,  # 10 MB limit
                request_timeout_ms=20000
            )
            print(f"[Producer] Connected to Kafka broker at {KAFKA_BROKER}.")
            return producer
        except KafkaError as err:
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

    # 1. Preferred: jsonl format
    if node_file:
        print(f"\n--- Phase 1: Ingesting Node-Centric Geometry from {node_file} ---")
        node_count = 0
        with open(node_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Send and ensure delivery
                future = producer.send(GEOMETRY_TOPIC, value=json.loads(line))
                future.get(timeout=10)
                
                node_count += 1
                if node_count % 500 == 0:
                    print(f"Dispatched {node_count} nodes to [{GEOMETRY_TOPIC}]...")
        
        producer.flush()
        print(f"Base mesh ingestion complete: {node_count} nodes published.\n")
        return

    # 2. Fallback: standard json format
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
            future = producer.send(GEOMETRY_TOPIC, value=payload)
            future.get(timeout=10) # Ensure delivery
            
        producer.flush()
        print(f"Base mesh ingestion complete: {len(nodes)} nodes published from topology.\n")
        return

    # Fatal Error if no geometry is found
    raise FileNotFoundError(
        "Critical Error: Neither 'kafka_nodes_stream.jsonl' nor 'mesh_topology.json' found. "
        "The digital twin cannot function without base geometry."
    )

def stream_mechanical_telemetry(producer):
    """Streams dynamic mechanical results frame-by-frame with dt = 0.02s."""
    results_file = find_file("mechanical_results.jsonl")
    if not results_file:
        raise FileNotFoundError("Critical Error: 'mechanical_results.jsonl' not found.")

    print(f"--- Phase 2: Streaming Thermomechanical Telemetry from {results_file} ---")
    print(f"Sampling frequency: 50 Hz (interval = {STREAM_INTERVAL_SECONDS}s)\n")
    
    step_count = 0
    with open(results_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            frame_data = json.loads(line)
            sim_time = frame_data.get("time", round(step_count * 0.02, 3))

            # Send data to Kafka
            try:
                future = producer.send(TELEMETRY_TOPIC, value=frame_data)
                record_metadata = future.get(timeout=10)
                
                if (step_count + 1) % 10 == 0 or step_count < 5:
                    print(
                        f"Streamed Frame #{step_count+1} (t={sim_time}s) -> "
                        f"Partition: {record_metadata.partition} | Offset: {record_metadata.offset}"
                    )
            except Exception as send_err:
                raise RuntimeError(f"Failed to stream frame at t={sim_time}s: {send_err}")

            step_count += 1
            time.sleep(STREAM_INTERVAL_SECONDS)

    producer.flush()
    print(f"\nTelemetry streaming completed successfully. Total frames dispatched: {step_count}")

# ================= Entry Point =================
def run_pipeline():
    producer = None
    try:
        producer = create_kafka_producer()
        
        # 1. Ingest physical mesh
        stream_mesh_geometry(producer)
        
        # 2. Brief pause for consumer collection setup
        time.sleep(1.0)
        
        # 3. Stream time-dependent results
        stream_mechanical_telemetry(producer)

    except KeyboardInterrupt:
        print("\n[Producer] Streaming interrupted by user.")
        sys.exit(0)
    except Exception as exc:
        print(f"\n[Producer] Critical runtime error: {exc}")
        sys.exit(1)  # Ensure Docker detects the failure and applies restart policies
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            print("[Producer] Kafka producer disconnected.")

if __name__ == "__main__":
    run_pipeline()