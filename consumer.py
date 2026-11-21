import json
import time
import os
import sys
import uuid
import hashlib
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone

from kafka import (
    KafkaConsumer,
    KafkaAdminClient,
    TopicPartition,
    OffsetAndMetadata
)
from kafka.errors import KafkaError

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

from prometheus_client import (
    start_http_server,
    Counter,
    Gauge,
    Histogram
)


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

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "welding_digital_twin_group"
)

METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 30

MAX_PARTITION_FETCH_BYTES = 10 * 1024 * 1024
FETCH_MAX_BYTES = 50 * 1024 * 1024

# Background health-monitoring interval
HEALTH_CHECK_INTERVAL = 5

# Consumer poll timeout for periodic idle reporting (ms)
CONSUMER_POLL_TIMEOUT_MS = 5000

# Operational execution monitoring configuration
RUN_ID = os.getenv("RUN_ID", "").strip()
if not RUN_ID:
    raise RuntimeError(
        "RUN_ID environment variable is required and must be injected once at pipeline startup."
    )

PROCESS_INSTANCE_ID = (
    f"CONS-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
)
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ============================================================
# Structured Logging Setup
# ============================================================

class ExecutionContextFilter(logging.Filter):
    """Injects pipeline RUN_ID and per-process PROCESS_INSTANCE_ID into every log record."""

    def filter(self, record):
        record.run_id = RUN_ID
        record.process_instance_id = PROCESS_INSTANCE_ID
        return True


def setup_logger():
    """Configures dual logging to stdout (Docker logs) and consumer.log."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file_path = os.path.join(LOG_DIR, "consumer.log")

    logger = logging.getLogger("welding.consumer")
    numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
    logger.setLevel(numeric_level)
    logger.propagate = False

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        fmt=(
            "%(asctime)s | %(levelname)s | %(name)s | "
            "run_id=%(run_id)s | process_instance_id=%(process_instance_id)s | "
            "%(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    context_filter = ExecutionContextFilter()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(numeric_level)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(context_filter)

    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(context_filter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()


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

# Timestamp of the last successfully persisted and committed message.
# Separate value is maintained for each Kafka topic.
LAST_SUCCESSFUL_PROCESSING_TIMESTAMP = Gauge(
    "welding_consumer_last_successful_processing_timestamp_seconds",
    "Unix timestamp of the last successfully persisted and committed Kafka message",
    ["topic"]
)

# Initial connection states
MONGO_CONNECTED.set(0)
KAFKA_CONNECTED.set(0)


# ============================================================
# Utility functions
# ============================================================

def utc_now_iso():
    """Returns current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def retry_delay(attempt):
    return min(
        INITIAL_RETRY_DELAY * (2 ** (attempt - 1)),
        MAX_RETRY_DELAY
    )


def generate_stable_id(record):
    record_copy = dict(record)
    record_copy.pop("event_id", None)

    record_str = json.dumps(
        record_copy,
        sort_keys=True,
        separators=(",", ":")
    )

    return hashlib.md5(
        record_str.encode("utf-8")
    ).hexdigest()


# ============================================================
# Background Service Health Monitoring
# ============================================================

def monitor_service_connections(stop_event, mongo_client, mark_runtime_failure):
    """
    Continuously checks MongoDB and Kafka connectivity in a
    background thread.

    MongoDB:
        Executes a ping command.

    Kafka:
        Uses KafkaAdminClient to request the available topics.

    Both checks are repeated every HEALTH_CHECK_INTERVAL seconds.
    """

    logger.info("HEALTH_MONITOR_STARTED | interval_s=%s", HEALTH_CHECK_INTERVAL)

    while not stop_event.is_set():

        # ----------------------------------------------------
        # MongoDB health check
        # ----------------------------------------------------

        try:
            mongo_client.admin.command("ping")

            MONGO_CONNECTED.set(1)

        except Exception as exc:
            MONGO_CONNECTED.set(0)
            mark_runtime_failure()

            logger.warning(
                f"HEALTH_CHECK_FAILED | component=mongodb | error={exc}"
            )

        # ----------------------------------------------------
        # Kafka health check
        # ----------------------------------------------------

        admin_client = None

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=[KAFKA_BROKER],
                request_timeout_ms=5000
            )

            # Request topic metadata from the Kafka broker.
            admin_client.list_topics()

            KAFKA_CONNECTED.set(1)

        except Exception as exc:
            KAFKA_CONNECTED.set(0)
            mark_runtime_failure()

            logger.warning(
                f"HEALTH_CHECK_FAILED | component=kafka | error={exc}"
            )

        finally:
            if admin_client is not None:
                try:
                    admin_client.close()

                except Exception as exc:
                    logger.warning(
                        f"HEALTH_MONITOR_CLEANUP_WARNING | "
                        f"component=kafka_admin | error={exc}"
                    )

        # Wait for the next check.
        # Using Event.wait() allows the thread to stop immediately
        # when a shutdown signal is received.
        stop_event.wait(HEALTH_CHECK_INTERVAL)

    logger.info("HEALTH_MONITOR_STOPPED")


# ============================================================
# MongoDB connection
# ============================================================

def connect_to_mongodb():

    for attempt in range(1, MAX_RETRIES + 1):

        mongo_client = None

        try:
            mongo_client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )

            mongo_client.admin.command("ping")

            db = mongo_client[DB_NAME]

            geo_collection = db[GEOMETRY_COLLECTION]
            telem_collection = db[TELEMETRY_COLLECTION]
            thermal_collection = db[THERMAL_COLLECTION]

            # Unique index for geometry records
            geo_collection.create_index(
                [("node_id", ASCENDING)],
                unique=True
            )

            # Unique indexes for telemetry and thermal records
            telem_collection.create_index(
                [
                    ("time", ASCENDING),
                    ("event_id", ASCENDING)
                ],
                unique=True
            )

            thermal_collection.create_index(
                [
                    ("time", ASCENDING),
                    ("event_id", ASCENDING)
                ],
                unique=True
            )

            MONGO_CONNECTED.set(1)

            logger.info(
                f"MONGODB_CONNECTED | database={DB_NAME} | "
                f"collections=[{GEOMETRY_COLLECTION}, {TELEMETRY_COLLECTION}, {THERMAL_COLLECTION}]"
            )

            return (
                mongo_client,
                geo_collection,
                telem_collection,
                thermal_collection
            )

        except Exception as exc:

            MONGO_CONNECTED.set(0)

            PROCESSING_ERRORS.labels(
                error_type="mongo_connect"
            ).inc()

            logger.warning(
                f"MONGODB_CONNECTION_RETRY | attempt={attempt}/{MAX_RETRIES} | "
                f"error={exc}"
            )

            if mongo_client is not None:
                mongo_client.close()

            if attempt < MAX_RETRIES:

                delay = retry_delay(attempt)

                logger.warning(
                    f"MONGODB_RETRY_BACKOFF | delay_s={delay}"
                )

                time.sleep(delay)

    raise RuntimeError(
        "MongoDB could not be reached after multiple attempts."
    )


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
                consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                max_partition_fetch_bytes=MAX_PARTITION_FETCH_BYTES,
                fetch_max_bytes=FETCH_MAX_BYTES,
                value_deserializer=lambda x: json.loads(
                    x.decode("utf-8")
                )
            )

            consumer.subscribe([
                GEOMETRY_TOPIC,
                TELEMETRY_TOPIC,
                THERMAL_TOPIC
            ])

            KAFKA_CONNECTED.set(1)

            logger.info(
                f"KAFKA_CONNECTED | broker={KAFKA_BROKER} | "
                f"group_id={KAFKA_GROUP_ID} | "
                f"topics=[{GEOMETRY_TOPIC}, {TELEMETRY_TOPIC}, {THERMAL_TOPIC}]"
            )

            return consumer

        except KafkaError as exc:

            KAFKA_CONNECTED.set(0)

            PROCESSING_ERRORS.labels(
                error_type="kafka_connect"
            ).inc()

            logger.warning(
                f"KAFKA_CONNECTION_RETRY | attempt={attempt}/{MAX_RETRIES} | "
                f"error={exc}"
            )

            if attempt < MAX_RETRIES:

                delay = retry_delay(attempt)

                logger.warning(
                    f"KAFKA_RETRY_BACKOFF | delay_s={delay}"
                )

                time.sleep(delay)

    raise RuntimeError(
        "Kafka broker could not be reached after multiple attempts."
    )


# ============================================================
# Validation
# ============================================================

def validate_geometry_record(record):

    if not isinstance(record, dict):
        raise ValueError(
            "Geometry message must be a JSON object."
        )

    if "node_id" not in record:
        raise ValueError(
            "Geometry record is missing 'node_id'."
        )

    if record["node_id"] is None:
        raise ValueError(
            "Geometry record contains null 'node_id'."
        )

    try:
        node_id = int(record["node_id"])

    except (TypeError, ValueError):
        raise ValueError(
            f"Invalid node_id value: {record['node_id']}"
        )

    record["node_id"] = node_id

    return node_id


def validate_telemetry_record(record):

    if not isinstance(record, dict):
        raise ValueError(
            "Telemetry message must be a JSON object."
        )

    if "time" not in record:
        raise ValueError(
            "Telemetry record is missing 'time'."
        )

    if record["time"] is None:
        raise ValueError(
            "Telemetry record contains null 'time'."
        )

    if not record.get("event_id"):
        record["event_id"] = generate_stable_id(record)

    return (record["time"], record["event_id"])


# ============================================================
# MongoDB persistence
# ============================================================

def store_geometry(record, geo_collection):

    node_id = validate_geometry_record(record)

    with DB_WRITE_LATENCY.labels(
        collection=GEOMETRY_COLLECTION
    ).time():

        result = geo_collection.update_one(
            {"node_id": node_id},
            {"$set": record},
            upsert=True
        )

    is_inserted = result.upserted_id is not None

    logger.debug(
        f"GEOMETRY_NODE_UPSERTED | node_id={node_id} | "
        f"collection={GEOMETRY_COLLECTION} | "
        f"operation={'inserted' if is_inserted else 'existing_updated'}"
    )

    return is_inserted


def store_telemetry(
    record,
    target_collection,
    collection_name
):

    sim_time, event_id = validate_telemetry_record(record)

    with DB_WRITE_LATENCY.labels(
        collection=collection_name
    ).time():

        result = target_collection.update_one(
            {
                "time": sim_time,
                "event_id": event_id
            },
            {"$set": record},
            upsert=True
        )

    is_inserted = result.upserted_id is not None
    return sim_time, is_inserted


# ============================================================
# Main streaming consumer
# ============================================================

def start_streaming_consumer():

    mongo_client = None
    geo_collection = None
    telem_collection = None
    thermal_collection = None
    consumer = None

    # Background health-monitoring resources
    health_stop_event = threading.Event()
    health_thread = None

    # Thread-safe failure state shared between health monitor and main loop
    failure_lock = threading.Lock()
    failure_state = {
        "had_runtime_failure": False
    }

    def mark_runtime_failure():
        with failure_lock:
            failure_state["had_runtime_failure"] = True

    def consume_runtime_failure():
        with failure_lock:
            if failure_state["had_runtime_failure"]:
                failure_state["had_runtime_failure"] = False
                return True
            return False

    # Track partitions where an existing committed offset (> 0) was observed on first encounter
    checked_partitions = set()
    resumed_existing_logged = set()

    # Per-topic and total operational counters (scoped to this PROCESS_INSTANCE_ID)
    topic_stats = {
        GEOMETRY_TOPIC: {
            "received": 0,
            "inserted_count": 0,
            "existing_documents_updated": 0,
            "messages_skipped_invalid": 0,
            "last_committed_offset": None
        },
        TELEMETRY_TOPIC: {
            "received": 0,
            "inserted_count": 0,
            "existing_documents_updated": 0,
            "messages_skipped_invalid": 0,
            "last_committed_offset": None
        },
        THERMAL_TOPIC: {
            "received": 0,
            "inserted_count": 0,
            "existing_documents_updated": 0,
            "messages_skipped_invalid": 0,
            "last_committed_offset": None
        }
    }

    last_activity_iso = None
    idle_reported = False

    def total_stats():
        return {
            "received": sum(v["received"] for v in topic_stats.values()),
            "inserted_count": sum(v["inserted_count"] for v in topic_stats.values()),
            "existing_documents_updated": sum(
                v["existing_documents_updated"] for v in topic_stats.values()
            ),
            "messages_skipped_invalid": sum(
                v["messages_skipped_invalid"] for v in topic_stats.values()
            )
        }

    try:

        logger.info(
            f"CONSUMER_STARTED | started_at={utc_now_iso()} | "
            f"counters_scope=process_instance_reset_to_zero | "
            f"broker={KAFKA_BROKER} | group_id={KAFKA_GROUP_ID} | "
            f"database={DB_NAME}"
        )

        # ----------------------------------------------------
        # Start Prometheus metrics server
        # ----------------------------------------------------

        start_http_server(METRICS_PORT)

        logger.info(
            f"PROMETHEUS_EXPORTER_STARTED | port={METRICS_PORT}"
        )

        # ----------------------------------------------------
        # Connect to MongoDB
        # ----------------------------------------------------

        (
            mongo_client,
            geo_collection,
            telem_collection,
            thermal_collection
        ) = connect_to_mongodb()

        # ----------------------------------------------------
        # Connect to Kafka
        # ----------------------------------------------------

        consumer = create_kafka_consumer()

        # ----------------------------------------------------
        # Start background health-monitoring thread
        # ----------------------------------------------------

        health_thread = threading.Thread(
            target=monitor_service_connections,
            args=(health_stop_event, mongo_client, mark_runtime_failure),
            name="service-health-monitor",
            daemon=True
        )

        health_thread.start()

        logger.info(
            "CONSUMER_LISTENING | Actively listening for 3D welding Digital Twin events"
        )

        # ----------------------------------------------------
        # Main Kafka consumption loop (continuous across idle polls)
        # ----------------------------------------------------

        while True:
            received_in_poll = False

            for message in consumer:
                received_in_poll = True
                idle_reported = False

                record = message.value
                current_topic = message.topic

                topic_partition = TopicPartition(
                    message.topic,
                    message.partition
                )

                # Inspect prior committed offset once per partition encounter in this process instance
                if topic_partition not in checked_partitions:
                    checked_partitions.add(topic_partition)
                    try:
                        prior_committed = consumer.committed(topic_partition)
                        if (
                            prior_committed is not None
                            and prior_committed > 0
                            and topic_partition not in resumed_existing_logged
                        ):
                            logger.info(
                                f"RESUMED_FROM_EXISTING_COMMITTED_OFFSET | "
                                f"topic={current_topic} | partition={message.partition} | "
                                f"prior_committed_offset={prior_committed} | "
                                f"first_polled_offset={message.offset} | "
                                f"observed_at={utc_now_iso()}"
                            )
                            resumed_existing_logged.add(topic_partition)
                    except KafkaError as committed_err:
                        logger.warning(
                            f"COMMITTED_OFFSET_CHECK_WARNING | topic={current_topic} | "
                            f"partition={message.partition} | error={committed_err}"
                        )

                offset_and_metadata = OffsetAndMetadata(
                    message.offset + 1,
                    ""
                )

                processed = False
                is_inserted = False

                for attempt in range(1, MAX_RETRIES + 1):

                    try:

                        # ----------------------------------------
                        # Geometry topic
                        # ----------------------------------------

                        if current_topic == GEOMETRY_TOPIC:

                            is_inserted = store_geometry(
                                record,
                                geo_collection
                            )

                            logger.debug(
                                f"MESSAGE_PERSISTED | topic={GEOMETRY_TOPIC} | "
                                f"offset={message.offset} | "
                                f"result={'inserted' if is_inserted else 'existing_updated'}"
                            )

                        # ----------------------------------------
                        # Telemetry topic
                        # ----------------------------------------

                        elif current_topic == TELEMETRY_TOPIC:

                            sim_time, is_inserted = store_telemetry(
                                record,
                                telem_collection,
                                TELEMETRY_COLLECTION
                            )

                            LAST_CONSUMED_SIM_TIME.labels(
                                topic=TELEMETRY_TOPIC
                            ).set(float(sim_time))

                            logger.debug(
                                f"MESSAGE_PERSISTED | topic={TELEMETRY_TOPIC} | "
                                f"t={sim_time}s | offset={message.offset} | "
                                f"result={'inserted' if is_inserted else 'existing_updated'}"
                            )

                        # ----------------------------------------
                        # Thermal topic
                        # ----------------------------------------

                        elif current_topic == THERMAL_TOPIC:

                            sim_time, is_inserted = store_telemetry(
                                record,
                                thermal_collection,
                                THERMAL_COLLECTION
                            )

                            LAST_CONSUMED_SIM_TIME.labels(
                                topic=THERMAL_TOPIC
                            ).set(float(sim_time))

                            logger.debug(
                                f"MESSAGE_PERSISTED | topic={THERMAL_TOPIC} | "
                                f"t={sim_time}s | offset={message.offset} | "
                                f"result={'inserted' if is_inserted else 'existing_updated'}"
                            )

                        else:

                            raise ValueError(
                                f"Unexpected Kafka topic: {current_topic}"
                            )

                        # ----------------------------------------
                        # Commit Kafka offset after persistence
                        # ----------------------------------------

                        consumer.commit(
                            offsets={
                                topic_partition: offset_and_metadata
                            }
                        )

                        # ----------------------------------------
                        # Update processing metrics and counters
                        # ----------------------------------------

                        MESSAGES_PROCESSED.labels(
                            topic=current_topic
                        ).inc()

                        LAST_COMMITTED_OFFSET.labels(
                            topic=current_topic
                        ).set(message.offset + 1)

                        now_ts = time.time()
                        last_activity_iso = utc_now_iso()

                        LAST_SUCCESSFUL_PROCESSING_TIMESTAMP.labels(
                            topic=current_topic
                        ).set(now_ts)

                        if current_topic in topic_stats:
                            stats = topic_stats[current_topic]
                            stats["received"] += 1
                            if is_inserted:
                                stats["inserted_count"] += 1
                            else:
                                stats["existing_documents_updated"] += 1
                            stats["last_committed_offset"] = message.offset + 1

                            if (
                                (current_topic == GEOMETRY_TOPIC and stats["received"] % 2500 == 0)
                                or (current_topic != GEOMETRY_TOPIC and stats["received"] % 100 == 0)
                            ):
                                totals = total_stats()
                                logger.info(
                                    f"BATCH_PROGRESS_SUMMARY | topic={current_topic} | "
                                    f"topic_received={stats['received']} | "
                                    f"topic_inserted={stats['inserted_count']} | "
                                    f"topic_existing_updated={stats['existing_documents_updated']} | "
                                    f"topic_skipped_invalid={stats['messages_skipped_invalid']} | "
                                    f"last_committed_offset={stats['last_committed_offset']} | "
                                    f"total_received={totals['received']} | "
                                    f"total_inserted={totals['inserted_count']} | "
                                    f"total_existing_updated={totals['existing_documents_updated']} | "
                                    f"total_skipped_invalid={totals['messages_skipped_invalid']} | "
                                    f"last_activity={last_activity_iso}"
                                )

                        # Log RUNTIME_CONNECTIVITY_RECOVERED only after an observed in-process failure
                        if consume_runtime_failure():
                            logger.info(
                                f"RUNTIME_CONNECTIVITY_RECOVERED | "
                                f"recovery_source=observed_runtime_failure | "
                                f"topic={current_topic} | partition={message.partition} | "
                                f"resumed_offset={message.offset} | "
                                f"resumed_at={last_activity_iso}"
                            )

                        processed = True

                        break

                    # --------------------------------------------
                    # Persistence or Kafka errors
                    # --------------------------------------------

                    except (PyMongoError, KafkaError) as exc:

                        mark_runtime_failure()

                        PROCESSING_ERRORS.labels(
                            error_type="persistence_or_broker"
                        ).inc()

                        if isinstance(exc, PyMongoError):
                            MONGO_CONNECTED.set(0)

                        if isinstance(exc, KafkaError):
                            KAFKA_CONNECTED.set(0)

                        logger.warning(
                            f"PROCESSING_RETRY | topic={current_topic} | "
                            f"offset={message.offset} | "
                            f"attempt={attempt}/{MAX_RETRIES} | error={exc}"
                        )

                        if attempt < MAX_RETRIES:

                            delay = retry_delay(attempt)

                            time.sleep(delay)

                    # --------------------------------------------
                    # Invalid messages
                    # --------------------------------------------

                    except (ValueError, TypeError) as exc:

                        PROCESSING_ERRORS.labels(
                            error_type="validation"
                        ).inc()

                        try:

                            consumer.commit(
                                offsets={
                                    topic_partition: offset_and_metadata
                                }
                            )

                            LAST_COMMITTED_OFFSET.labels(
                                topic=current_topic
                            ).set(message.offset + 1)

                            last_activity_iso = utc_now_iso()

                            if current_topic in topic_stats:
                                stats = topic_stats[current_topic]
                                stats["received"] += 1
                                stats["messages_skipped_invalid"] += 1
                                stats["last_committed_offset"] = message.offset + 1

                            logger.error(
                                f"PROCESSING_ERROR | stage=validation | "
                                f"topic={current_topic} | offset={message.offset} | "
                                f"committed_offset={message.offset + 1} | "
                                f"commit_status=COMMITTED | "
                                f"action=skipped_invalid_and_counted | error={exc}"
                            )

                            processed = True

                        except KafkaError as commit_error:

                            mark_runtime_failure()

                            PROCESSING_ERRORS.labels(
                                error_type="offset_commit"
                            ).inc()

                            KAFKA_CONNECTED.set(0)

                            logger.error(
                                f"PROCESSING_ERROR | stage=offset_commit | "
                                f"topic={current_topic} | offset={message.offset} | "
                                f"commit_status=FAILED | error={commit_error}"
                            )

                        break

                    # --------------------------------------------
                    # Unexpected errors
                    # --------------------------------------------

                    except Exception as exc:

                        mark_runtime_failure()

                        PROCESSING_ERRORS.labels(
                            error_type="unexpected"
                        ).inc()

                        logger.warning(
                            f"PROCESSING_RETRY | stage=unexpected | "
                            f"topic={current_topic} | offset={message.offset} | "
                            f"attempt={attempt}/{MAX_RETRIES} | error={exc}"
                        )

                        if attempt < MAX_RETRIES:

                            delay = retry_delay(attempt)

                            time.sleep(delay)

                # ------------------------------------------------
                # Failed message: leave offset uncommitted
                # ------------------------------------------------

                if not processed:

                    PROCESSING_ERRORS.labels(
                        error_type="fatal_uncommitted"
                    ).inc()

                    logger.error(
                        f"PROCESSING_ERROR | stage=fatal_uncommitted | "
                        f"topic={current_topic} | offset={message.offset} | "
                        f"attempts={MAX_RETRIES} | "
                        f"message=Offset remains uncommitted for replay"
                    )

                    raise RuntimeError(
                        f"Failed to process Kafka message after "
                        f"{MAX_RETRIES} attempts. "
                        f"Offset {message.offset} remains uncommitted "
                        f"for replay."
                    )

            # Log CONSUMER_IDLE once per idle window when no new messages arrive
            if not received_in_poll and not idle_reported:
                totals = total_stats()
                logger.info(
                    f"CONSUMER_IDLE | status=WAITING_FOR_MESSAGES | "
                    f"idle_window_ms={CONSUMER_POLL_TIMEOUT_MS} | "
                    f"total_received={totals['received']} | "
                    f"total_inserted={totals['inserted_count']} | "
                    f"total_existing_updated={totals['existing_documents_updated']} | "
                    f"total_skipped_invalid={totals['messages_skipped_invalid']} | "
                    f"by_topic={json.dumps(topic_stats, sort_keys=True)} | "
                    f"last_activity={last_activity_iso} | "
                    f"observed_at={utc_now_iso()}"
                )
                idle_reported = True

    # ========================================================
    # Shutdown handling
    # ========================================================

    except KeyboardInterrupt:

        logger.info("CONSUMER_STOPPED | reason=KeyboardInterrupt")

    except Exception as exc:

        logger.error(
            f"PROCESSING_ERROR | stage=critical_runtime | error={exc}",
            exc_info=True
        )

        raise

    finally:

        # ----------------------------------------------------
        # Stop background health-monitoring thread first
        # ----------------------------------------------------

        health_stop_event.set()

        if health_thread is not None:

            health_thread.join(
                timeout=HEALTH_CHECK_INTERVAL + 7
            )

            if health_thread.is_alive():

                logger.warning(
                    "HEALTH_MONITOR_SHUTDOWN_WARNING | "
                    "Thread did not stop within expected timeout"
                )

            else:

                logger.info(
                    "HEALTH_MONITOR_SHUTDOWN_OK"
                )

        # ----------------------------------------------------
        # Set final connection states
        # ----------------------------------------------------

        MONGO_CONNECTED.set(0)
        KAFKA_CONNECTED.set(0)

        # ----------------------------------------------------
        # Close Kafka consumer
        # ----------------------------------------------------

        if consumer is not None:

            try:

                consumer.close()

                logger.info("CONSUMER_KAFKA_CLOSED")

            except Exception as exc:

                logger.warning(
                    f"CONSUMER_KAFKA_CLOSE_WARNING | error={exc}"
                )

        # ----------------------------------------------------
        # Close MongoDB connection
        # ----------------------------------------------------

        if mongo_client is not None:

            try:

                mongo_client.close()

                logger.info("CONSUMER_MONGODB_CLOSED")

            except Exception as exc:

                logger.warning(
                    f"CONSUMER_MONGODB_CLOSE_WARNING | error={exc}"
                )


# ============================================================
# Application entry point
# ============================================================

if __name__ == "__main__":

    start_streaming_consumer()