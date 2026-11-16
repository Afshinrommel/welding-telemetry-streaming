import json
import time
import os
import hashlib

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from prometheus_client import start_http_server, Counter, Gauge, Histogram


# ============================================================
# Configuration
# ============================================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
GEOMETRY_TOPIC = os.getenv("GEOMETRY_TOPIC", "welding_geometry")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "welding_telemetry")
THERMAL_TOPIC = os.getenv("THERMAL_TOPIC", "welding_thermal")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = os.getenv("MONGO_DB", "welding_digital_twin")

GEOMETRY_COLLECTION = os.getenv("GEOMETRY_COLLECTION", "mesh_geometry")
TELEMETRY_COLLECTION = os.getenv("TELEMETRY_COLLECTION", "welding_telemetry")
THERMAL_COLLECTION = os.getenv("THERMAL_COLLECTION", "welding_thermal")

KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "welding_digital_twin_group")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 30

MAX_PARTITION_FETCH_BYTES = 10 * 1024 * 1024
FETCH_MAX_BYTES = 50 * 1024 * 1024


# ============================================================
# Prometheus Metrics
# ============================================================

MESSAGES_PROCESSED = Counter(
    "welding_consumer_messages_processed_total",
    "Total number of Kafka messages validated and persisted to MongoDB",
    ["topic"]
)
PROCESSING_ERRORS = Counter(
    "welding_consumer_processing_errors_total",
    "Total number of errors encountered during consumption or persistence",
    ["error_type"]
)
DB_WRITE_LATENCY = Histogram(
    "welding_consumer_db_write_duration_seconds",
    "Latency of MongoDB upsert operations",
    ["collection"]
)
LAST_CONSUMED_SIM_TIME = Gauge(
    "welding_consumer_last_sim_time_seconds",
    "Latest simulation time step persisted in MongoDB (seconds)",
    ["topic"]
)
LAST_COMMITTED_OFFSET = Gauge(
    "welding_consumer_last_committed_offset",
    "Last committed Kafka offset per topic",
    ["topic"]
)
MONGO_CONNECTED = Gauge(
    "welding_consumer_mongodb_connected",
    "MongoDB connection status (1 = connected, 0 = disconnected)"
)
KAFKA_CONNECTED = Gauge(
    "welding_consumer_kafka_connected",
    "Kafka consumer connection status (1 = connected, 0 = disconnected)"
)


# ============================================================
# Utility functions
# ============================================================

def retry_delay(attempt):
    return min(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY)

def generate_stable_id(record):
    record_copy = dict(record)
    record_copy.pop("event_id", None)
    record_str = json.dumps(record_copy, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(record_str.encode("utf-8")).hexdigest()


# ============================================================
# MongoDB connection
# ============================================================

def connect_to_mongodb():
    for attempt in range(1, MAX_RETRIES + 1):
        mongo_client = None
        try:
            mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
            mongo_client.admin.command("ping")
            db = mongo_client[DB_NAME]

            geo_collection = db[GEOMETRY_COLLECTION]
            telem_collection = db[TELEMETRY_COLLECTION]
            thermal_collection = db[THERMAL_COLLECTION]

            geo_collection.create_index([("node_id", ASCENDING)], unique=True)
            telem_collection.create_index([("time", ASCENDING), ("event_id", ASCENDING)], unique=True)
            thermal_collection.create_index([("time", ASCENDING), ("event_id", ASCENDING)], unique=True)

            MONGO_CONNECTED.set(1)
            print(f"[Consumer] Connected to MongoDB '{DB_NAME}' successfully.")
            return (mongo_client, geo_collection, telem_collection, thermal_collection)

        except Exception as exc:
            MONGO_CONNECTED.set(0)
            PROCESSING_ERRORS.labels(error_type="mongo_connect").inc()
            print(f"[Consumer] MongoDB connection attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if mongo_client is not None:
                mongo_client.close()
            if attempt < MAX_RETRIES:
                delay = retry_delay(attempt)
                print(f"[Consumer] Retrying MongoDB connection in {delay} seconds...")
                time.sleep(delay)

    raise RuntimeError("MongoDB could not be reached after multiple attempts.")


# ============================================================
# Kafka connection
# ============================================================

def create_kafka_consumer():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BROKER],
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_partition_fetch_bytes=MAX_PARTITION_FETCH_BYTES,
                fetch_max_bytes=FETCH_MAX_BYTES,
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )

            consumer.subscribe([GEOMETRY_TOPIC, TELEMETRY_TOPIC, THERMAL_TOPIC])

            KAFKA_CONNECTED.set(1)
            print(f"[Consumer] Connected to Kafka broker '{KAFKA_BROKER}'.")
            print(f"[Consumer] Subscribed to: {GEOMETRY_TOPIC}, {TELEMETRY_TOPIC}, {THERMAL_TOPIC}")
            return consumer

        except KafkaError as exc:
            KAFKA_CONNECTED.set(0)
            PROCESSING_ERRORS.labels(error_type="kafka_connect").inc()
            print(f"[Consumer] Kafka connection attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                delay = retry_delay(attempt)
                print(f"[Consumer] Retrying Kafka connection in {delay} seconds...")
                time.sleep(delay)

    raise RuntimeError("Kafka broker could not be reached after multiple attempts.")


# ============================================================
# Validation
# ============================================================

def validate_geometry_record(record):
    if not isinstance(record, dict): raise ValueError("Geometry message must be a JSON object.")
    if "node_id" not in record: raise ValueError("Geometry record is missing 'node_id'.")
    if record["node_id"] is None: raise ValueError("Geometry record contains null 'node_id'.")
    try: node_id = int(record["node_id"])
    except (TypeError, ValueError): raise ValueError(f"Invalid node_id value: {record['node_id']}")
    record["node_id"] = node_id
    return node_id

def validate_telemetry_record(record):
    if not isinstance(record, dict): raise ValueError("Telemetry message must be a JSON object.")
    if "time" not in record: raise ValueError("Telemetry record is missing 'time'.")
    if record["time"] is None: raise ValueError("Telemetry record contains null 'time'.")
    if not record.get("event_id"):
        record["event_id"] = generate_stable_id(record)
    return (record["time"], record["event_id"])


# ============================================================
# MongoDB persistence
# ============================================================

def store_geometry(record, geo_collection):
    node_id = validate_geometry_record(record)
    with DB_WRITE_LATENCY.labels(collection=GEOMETRY_COLLECTION).time():
        geo_collection.update_one({"node_id": node_id}, {"$set": record}, upsert=True)
    if node_id % 1000 == 0:
        print(f"[Consumer] Saved geometry node #{node_id} -> '{GEOMETRY_COLLECTION}'")

def store_telemetry(record, target_collection, collection_name):
    sim_time, event_id = validate_telemetry_record(record)
    with DB_WRITE_LATENCY.labels(collection=collection_name).time():
        target_collection.update_one(
            {"time": sim_time, "event_id": event_id},
            {"$set": record},
            upsert=True
        )
    return sim_time


# ============================================================
# Main streaming consumer
# ============================================================

def start_streaming_consumer():
    mongo_client = None
    geo_collection = None
    telem_collection = None
    thermal_collection = None
    consumer = None

    try:
        start_http_server(METRICS_PORT)
        print(f"[Consumer] Prometheus metrics server started on port {METRICS_PORT}.")

        (mongo_client, geo_collection, telem_collection, thermal_collection) = connect_to_mongodb()
        consumer = create_kafka_consumer()
        print("[Consumer] Actively listening for 3D welding Digital Twin events...")

        for message in consumer:
            record = message.value
            current_topic = message.topic
            topic_partition = TopicPartition(message.topic, message.partition)
            offset_and_metadata = OffsetAndMetadata(message.offset + 1, "")
            processed = False

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    if current_topic == GEOMETRY_TOPIC:
                        store_geometry(record, geo_collection)
                        print(f"[Consumer] Geometry event persisted (offset={message.offset})")

                    elif current_topic == TELEMETRY_TOPIC:
                        sim_time = store_telemetry(record, telem_collection, TELEMETRY_COLLECTION)
                        LAST_CONSUMED_SIM_TIME.labels(topic=TELEMETRY_TOPIC).set(float(sim_time))
                        print(f"[Consumer] Stored telemetry frame at t={sim_time}s (offset={message.offset})")

                    elif current_topic == THERMAL_TOPIC:
                        sim_time = store_telemetry(record, thermal_collection, THERMAL_COLLECTION)
                        LAST_CONSUMED_SIM_TIME.labels(topic=THERMAL_TOPIC).set(float(sim_time))
                        print(f"[Consumer] Stored THERMAL frame at t={sim_time}s (offset={message.offset})")

                    else:
                        raise ValueError(f"Unexpected Kafka topic: {current_topic}")

                    consumer.commit(offsets={topic_partition: offset_and_metadata})
                    MESSAGES_PROCESSED.labels(topic=current_topic).inc()
                    LAST_COMMITTED_OFFSET.labels(topic=current_topic).set(message.offset + 1)
                    processed = True
                    break

                except (PyMongoError, KafkaError) as exc:
                    PROCESSING_ERRORS.labels(error_type="persistence_or_broker").inc()
                    print(f"[Consumer] Processing attempt {attempt}/{MAX_RETRIES} failed: {exc}")
                    if attempt < MAX_RETRIES:
                        delay = retry_delay(attempt)
                        time.sleep(delay)

                except (ValueError, TypeError) as exc:
                    PROCESSING_ERRORS.labels(error_type="validation").inc()
                    print(f"[Consumer] Invalid message (skipping): {exc}")
                    try:
                        consumer.commit(offsets={topic_partition: offset_and_metadata})
                        processed = True
                    except KafkaError as commit_error:
                        PROCESSING_ERRORS.labels(error_type="offset_commit").inc()
                        print(f"[Consumer] Failed to commit invalid message offset: {commit_error}")
                    break

                except Exception as exc:
                    PROCESSING_ERRORS.labels(error_type="unexpected").inc()
                    print(f"[Consumer] Unexpected processing error on attempt {attempt}/{MAX_RETRIES}: {exc}")
                    if attempt < MAX_RETRIES:
                        delay = retry_delay(attempt)
                        time.sleep(delay)

            if not processed:
                PROCESSING_ERRORS.labels(error_type="fatal_uncommitted").inc()
                raise RuntimeError(f"Failed to process Kafka message after {MAX_RETRIES} attempts. Offset {message.offset} remains uncommitted for replay.")

    except KeyboardInterrupt:
        print("\n[Consumer] Process stopped manually.")
    except Exception as exc:
        print(f"[Consumer] Critical consumer error: {exc}")
        raise
    finally:
        MONGO_CONNECTED.set(0)
        KAFKA_CONNECTED.set(0)
        if consumer is not None:
            try:
                consumer.close()
                print("[Consumer] Kafka consumer closed.")
            except Exception as exc:
                print(f"[Consumer] Kafka shutdown error: {exc}")
        if mongo_client is not None:
            try:
                mongo_client.close()
                print("[Consumer] MongoDB connection closed.")
            except Exception as exc:
                print(f"[Consumer] MongoDB shutdown error: {exc}")

if __name__ == "__main__":
    start_streaming_consumer()