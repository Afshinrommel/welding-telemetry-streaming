import json
import time
import os
import sys
import uuid
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
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

# Heuristic file readiness check delay before ingesting newly detected extension files
FILE_STABILITY_CHECK_SECONDS = 1.0

# Operational execution monitoring configuration
RUN_ID = os.getenv("RUN_ID", "").strip()
if not RUN_ID:
    raise RuntimeError(
        "RUN_ID environment variable is required and must be injected once at pipeline startup."
    )

PROCESS_INSTANCE_ID = (
    f"PROD-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
)
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ================= Structured Logging Setup =================
class ExecutionContextFilter(logging.Filter):
    """Injects pipeline RUN_ID and per-process PROCESS_INSTANCE_ID into every log record."""

    def filter(self, record):
        record.run_id = RUN_ID
        record.process_instance_id = PROCESS_INSTANCE_ID
        return True


def setup_logger():
    """Configures dual logging to stdout (Docker logs) and producer.log."""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file_path = os.path.join(LOG_DIR, "producer.log")

    logger = logging.getLogger("welding.producer")
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

LAST_SUCCESSFUL_SEND_TIMESTAMP = Gauge(
    "welding_producer_last_successful_send_timestamp_seconds",
    "Unix timestamp of the last successfully acknowledged Kafka message"
)


# ================= Utility =================
def utc_now_iso():
    """Returns current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def is_file_stable(file_path, wait_seconds=FILE_STABILITY_CHECK_SECONDS):
    """
    Heuristic readiness check for newly detected extension files.
    Verifies that the file is non-empty, readable, and keeps an unchanged size
    and modification time across a short observation interval.
    Note: This reduces the chance of reading an actively written file, though
    atomic write-to-temp-then-rename (.tmp -> .jsonl) by the writer remains the
    strict guarantee when writer coordination is available.
    """
    try:
        stat_before = os.stat(file_path)
        if stat_before.st_size <= 0:
            return False

        time.sleep(wait_seconds)

        stat_after = os.stat(file_path)
        if (
            stat_before.st_size != stat_after.st_size
            or stat_before.st_mtime != stat_after.st_mtime
        ):
            return False

        with open(file_path, "rb") as probe:
            probe.read(1)

        return True
    except OSError:
        return False


def record_successful_send(topic):
    """Updates metrics after Kafka acknowledges a message."""
    MESSAGES_SENT.labels(topic=topic).inc()
    LAST_SUCCESSFUL_SEND_TIMESTAMP.set(time.time())
    KAFKA_CONNECTED.set(1)


def record_send_error(error_type="publish"):
    """Updates metrics when a Kafka publish operation fails."""
    SEND_ERRORS.labels(error_type=error_type).inc()
    KAFKA_CONNECTED.set(0)


def publish_message(producer, topic, payload):
    """
    Publishes one message and waits for broker acknowledgement.
    Returns the Kafka record metadata after successful delivery.
    """
    try:
        with SEND_LATENCY.labels(topic=topic).time():
            future = producer.send(topic, value=payload)
            record_metadata = future.get(timeout=10)

        record_successful_send(topic)
        return record_metadata

    except Exception as send_err:
        record_send_error("publish")
        raise RuntimeError(
            f"Failed to publish message to topic [{topic}]: {send_err}"
        ) from send_err


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

            # This indicates successful producer initialization.
            # A successful message acknowledgement confirms connectivity.
            KAFKA_CONNECTED.set(1)

            logger.info(
                f"KAFKA_CONNECTED | broker={KAFKA_BROKER} | acks=all"
            )
            return producer

        except KafkaError as err:
            KAFKA_CONNECTED.set(0)
            SEND_ERRORS.labels(error_type="connection").inc()

            delay = retry_delay(attempt)
            logger.warning(
                f"KAFKA_CONNECTION_RETRY | attempt={attempt} | "
                f"retry_in={delay}s | error={err}"
            )

            time.sleep(delay)
            attempt += 1


# ================= Streaming Functions =================
def stream_mesh_geometry(producer):
    """
    Streams base mesh geometry and guarantees delivery.
    Returns (processed_filename, records_read, kafka_acked).
    """
    node_file = find_file("kafka_nodes_stream.jsonl")
    topo_file = find_file("mesh_topology.json")

    if node_file:
        fname = os.path.basename(node_file)
        start_ts = time.time()
        logger.info(
            f"FILE_PROCESSING_STARTED | phase=initial_geometry | "
            f"file={fname} | topic={GEOMETRY_TOPIC} | "
            f"started_at={utc_now_iso()}"
        )

        node_count = 0
        acked_count = 0

        with open(node_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                payload = json.loads(line)
                node_count += 1

                record_metadata = publish_message(
                    producer,
                    GEOMETRY_TOPIC,
                    payload
                )
                acked_count += 1

                logger.debug(
                    f"GEOMETRY_NODE_ACKED | node_index={node_count} | "
                    f"partition={record_metadata.partition} | "
                    f"offset={record_metadata.offset}"
                )

                if node_count % 2500 == 0:
                    logger.info(
                        f"GEOMETRY_PROGRESS | file={fname} | "
                        f"dispatched={node_count} | topic={GEOMETRY_TOPIC}"
                    )

        producer.flush()
        FILES_PROCESSED.inc()
        duration = round(time.time() - start_ts, 3)

        logger.info(
            f"FILE_PROCESSING_COMPLETED | file={fname} | "
            f"records_read={node_count} | duration_s={duration}"
        )
        logger.info(
            f"KAFKA_PUBLISH_COMPLETED | file={fname} | "
            f"topic={GEOMETRY_TOPIC} | kafka_acked={acked_count} | "
            f"finished_at={utc_now_iso()}"
        )
        return fname, node_count, acked_count

    if topo_file:
        fname = os.path.basename(topo_file)
        start_ts = time.time()
        logger.info(
            f"FILE_PROCESSING_STARTED | phase=initial_geometry | "
            f"file={fname} | topic={GEOMETRY_TOPIC} | "
            f"started_at={utc_now_iso()}"
        )

        with open(topo_file, "r", encoding="utf-8") as f:
            topo_data = json.load(f)

        nodes = topo_data.get("nodes", [])
        node_count = len(nodes)
        acked_count = 0

        for node_id, coords in enumerate(nodes):
            payload = {
                "node_id": node_id,
                "coordinates": {
                    "x": coords[0],
                    "y": coords[1],
                    "z": coords[2]
                }
            }

            record_metadata = publish_message(
                producer,
                GEOMETRY_TOPIC,
                payload
            )
            acked_count += 1

            logger.debug(
                f"GEOMETRY_NODE_ACKED | node_id={node_id} | "
                f"partition={record_metadata.partition} | "
                f"offset={record_metadata.offset}"
            )

            if (node_id + 1) % 2500 == 0:
                logger.info(
                    f"GEOMETRY_PROGRESS | file={fname} | "
                    f"dispatched={node_id + 1}/{node_count} | "
                    f"topic={GEOMETRY_TOPIC}"
                )

        producer.flush()
        FILES_PROCESSED.inc()
        duration = round(time.time() - start_ts, 3)

        logger.info(
            f"FILE_PROCESSING_COMPLETED | file={fname} | "
            f"records_read={node_count} | duration_s={duration}"
        )
        logger.info(
            f"KAFKA_PUBLISH_COMPLETED | file={fname} | "
            f"topic={GEOMETRY_TOPIC} | kafka_acked={acked_count} | "
            f"finished_at={utc_now_iso()}"
        )
        return fname, node_count, acked_count

    raise FileNotFoundError(
        "Critical Error: No base geometry file found."
    )


def stream_file_content(
    producer,
    file_path,
    target_topic=TELEMETRY_TOPIC,
    start_step_count=0,
    phase="telemetry"
):
    """
    Streams a given telemetry/extension file frame-by-frame.
    Returns (updated_step_count, records_read_in_file, kafka_acked_in_file).
    """
    fname = os.path.basename(file_path)
    start_ts = time.time()

    logger.info(
        f"FILE_PROCESSING_STARTED | phase={phase} | file={fname} | "
        f"topic={target_topic} | started_at={utc_now_iso()}"
    )

    step_count = start_step_count
    records_read = 0
    kafka_acked = 0
    last_offset = None

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            frame_data = json.loads(line)
            records_read += 1

            sim_time = frame_data.get(
                "time",
                round(step_count * 0.02, 3)
            )

            try:
                record_metadata = publish_message(
                    producer,
                    target_topic,
                    frame_data
                )
                kafka_acked += 1
                last_offset = record_metadata.offset

                CURRENT_SIM_TIME.set(float(sim_time))

                logger.debug(
                    f"FRAME_ACKED | file={fname} | frame={step_count + 1} | "
                    f"t={sim_time}s | topic={target_topic} | "
                    f"partition={record_metadata.partition} | "
                    f"offset={record_metadata.offset}"
                )

                if records_read % 100 == 0:
                    logger.info(
                        f"STREAM_PROGRESS | file={fname} | "
                        f"records_acked={kafka_acked} | t={sim_time}s | "
                        f"topic={target_topic} | last_offset={last_offset}"
                    )

            except Exception as send_err:
                raise RuntimeError(
                    f"Failed to stream frame at "
                    f"t={sim_time}s: {send_err}"
                ) from send_err

            step_count += 1
            time.sleep(STREAM_INTERVAL_SECONDS)

    producer.flush()
    FILES_PROCESSED.inc()
    duration = round(time.time() - start_ts, 3)

    logger.info(
        f"FILE_PROCESSING_COMPLETED | file={fname} | "
        f"records_read={records_read} | duration_s={duration}"
    )
    logger.info(
        f"KAFKA_PUBLISH_COMPLETED | file={fname} | topic={target_topic} | "
        f"kafka_acked={kafka_acked} | last_offset={last_offset} | "
        f"finished_at={utc_now_iso()}"
    )

    return step_count, records_read, kafka_acked


def stream_mechanical_telemetry(
    producer,
    detected_initial_files,
    processed_geo_filename,
    initial_geo_read,
    initial_geo_acked
):
    """
    Streams initial main telemetry first, then monitors
    extension folder every 15 seconds.
    """
    processed_files = set()
    actually_processed_initial = [processed_geo_filename]

    # 1. Process initial main file
    results_file = find_file("mechanical_results.jsonl")

    if not results_file:
        raise FileNotFoundError(
            "Critical Error: 'mechanical_results.jsonl' not found."
        )

    step_count, telem_read, telem_acked = stream_file_content(
        producer,
        results_file,
        target_topic=TELEMETRY_TOPIC,
        start_step_count=0,
        phase="initial_telemetry"
    )

    processed_files.add(os.path.abspath(results_file))
    actually_processed_initial.append(os.path.basename(results_file))

    total_initial_read = initial_geo_read + telem_read
    total_initial_acked = initial_geo_acked + telem_acked

    logger.info(
        f"INITIAL_BATCH_COMPLETED | "
        f"initial_files_detected={len(detected_initial_files)} | "
        f"detected_files={detected_initial_files} | "
        f"initial_files_processed={len(actually_processed_initial)} | "
        f"processed_files={actually_processed_initial} | "
        f"records_read={total_initial_read} | "
        f"kafka_acked={total_initial_acked} | "
        f"geometry_acked={initial_geo_acked} | "
        f"telemetry_acked={telem_acked} | "
        f"completed_at={utc_now_iso()}"
    )

    # 2. Continuous folder monitoring
    logger.info(
        f"WATCHING_EXTENSION | extension_dir={EXTENSION_DIR} | "
        f"poll_interval_s=15 | status=ACTIVE"
    )

    while True:
        try:
            ext_dir = find_extension_dir()
            logger.debug(
                f"EXTENSION_SCAN | dir={ext_dir} | scanned_at={utc_now_iso()}"
            )

            if ext_dir:
                candidate_files = []
                for fname in sorted(os.listdir(ext_dir)):
                    if fname.endswith(".jsonl"):
                        fpath = os.path.abspath(
                            os.path.join(ext_dir, fname)
                        )
                        if fpath not in processed_files:
                            candidate_files.append((fname, fpath))

                if candidate_files:
                    logger.info(
                        f"NEW_FILES_DETECTED | count={len(candidate_files)} | "
                        f"files={[f[0] for f in candidate_files]} | "
                        f"detected_at={utc_now_iso()}"
                    )

                    for fname, fpath in candidate_files:
                        if not is_file_stable(fpath):
                            logger.warning(
                                f"EXTENSION_FILE_NOT_READY | file={fname} | "
                                f"check=heuristic_size_mtime | "
                                f"action=defer_to_next_scan"
                            )
                            continue

                        # Generic routing based on filename pattern
                        topic = TELEMETRY_TOPIC
                        if "thermal" in fname.lower():
                            topic = THERMAL_TOPIC

                        # Note: processed_files is updated only after full file completion.
                        # If an error interrupts streaming mid-file, the file remains unmarked
                        # and will be reprocessed from the beginning on a subsequent scan.
                        step_count, ext_read, ext_acked = stream_file_content(
                            producer,
                            fpath,
                            target_topic=topic,
                            start_step_count=step_count,
                            phase="extension"
                        )

                        processed_files.add(fpath)

                        logger.info(
                            f"EXTENSION_FILE_COMPLETED | file={fname} | "
                            f"topic={topic} | records_read={ext_read} | "
                            f"kafka_acked={ext_acked} | "
                            f"cumulative_frames={step_count} | "
                            f"completed_at={utc_now_iso()}"
                        )

            # Wait 15 seconds before checking again
            time.sleep(15)

        except Exception as watch_err:
            SEND_ERRORS.labels(error_type="watcher").inc()

            logger.error(
                f"PRODUCER_ERROR | stage=extension_watcher | "
                f"retry_behavior=full_file_reprocess_on_next_scan | "
                f"error={watch_err}"
            )

            time.sleep(15)


# ================= Entry Point =================
def run_pipeline():
    producer = None

    try:
        logger.info(
            f"PRODUCER_STARTED | started_at={utc_now_iso()} | "
            f"broker={KAFKA_BROKER} | data_dir={DATA_DIR} | "
            f"extension_dir={EXTENSION_DIR} | interval_s={STREAM_INTERVAL_SECONDS}"
        )

        # Start Prometheus metrics HTTP server
        start_http_server(METRICS_PORT)
        logger.info(
            f"PROMETHEUS_EXPORTER_STARTED | port={METRICS_PORT}"
        )

        # Initial scan summary of primary directory
        detected_initial_files = [
            fname for fname in (
                "kafka_nodes_stream.jsonl",
                "mesh_topology.json",
                "mechanical_results.jsonl"
            )
            if find_file(fname) is not None
        ]
        logger.info(
            f"INITIAL_SCAN | data_dir={DATA_DIR} | "
            f"initial_files_detected={len(detected_initial_files)} | "
            f"files={detected_initial_files}"
        )

        producer = create_kafka_producer()

        # 1. Ingest physical mesh
        geo_filename, geo_read, geo_acked = stream_mesh_geometry(producer)

        # 2. Brief pause for consumer collection setup
        time.sleep(1.0)

        # 3. Stream main results and monitor extension folder
        stream_mechanical_telemetry(
            producer=producer,
            detected_initial_files=detected_initial_files,
            processed_geo_filename=geo_filename,
            initial_geo_read=geo_read,
            initial_geo_acked=geo_acked
        )

    except KeyboardInterrupt:
        logger.info("PRODUCER_STOPPED | reason=KeyboardInterrupt")
        sys.exit(0)

    except Exception as exc:
        SEND_ERRORS.labels(error_type="critical").inc()

        logger.error(
            f"PRODUCER_ERROR | stage=runtime | error={exc}",
            exc_info=True
        )

        sys.exit(1)

    finally:
        KAFKA_CONNECTED.set(0)

        if producer is not None:
            producer.flush()
            producer.close()

            logger.info("PRODUCER_DISCONNECTED | broker_closed=True")


if __name__ == "__main__":
    run_pipeline()