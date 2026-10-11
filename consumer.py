import json
import time
import os
import hashlib

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError


# ============================================================
# Configuration
# ============================================================

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka:29092"
)

GEOMETRY_TOPIC = os.getenv(
    "GEOMETRY_TOPIC",
    "welding_geometry"
)

TELEMETRY_TOPIC = os.getenv(
    "TELEMETRY_TOPIC",
    "welding_telemetry"
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongodb:27017/"
)

DB_NAME = os.getenv(
    "MONGO_DB",
    "welding_digital_twin"
)

GEOMETRY_COLLECTION = os.getenv(
    "GEOMETRY_COLLECTION",
    "mesh_geometry"
)

TELEMETRY_COLLECTION = os.getenv(
    "TELEMETRY_COLLECTION",
    "welding_telemetry"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "welding_digital_twin_group"
)

# Retry configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 30

# Kafka message-size configuration
MAX_PARTITION_FETCH_BYTES = 10 * 1024 * 1024
FETCH_MAX_BYTES = 50 * 1024 * 1024


# ============================================================
# Utility functions
# ============================================================

def retry_delay(attempt):
    """
    Exponential backoff with an upper limit.

    2 -> 4 -> 8 -> 16 -> 30 seconds
    """
    return min(
        INITIAL_RETRY_DELAY * (2 ** (attempt - 1)),
        MAX_RETRY_DELAY
    )


def generate_stable_id(record):
    """
    Generate a deterministic ID only when the producer has
    not already supplied an event_id.

    The same record content produces the same ID, which
    supports idempotent processing during Kafka replays.
    """

    record_copy = dict(record)

    # Do not include event_id itself in the generated hash.
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
# MongoDB connection
# ============================================================

def connect_to_mongodb():
    """
    Connect to MongoDB with bounded retries.

    If MongoDB is still unavailable after the retries,
    the application exits and Docker can restart the container.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        mongo_client = None

        try:

            mongo_client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )

            # Verify that MongoDB is actually reachable.
            mongo_client.admin.command("ping")

            db = mongo_client[DB_NAME]

            geo_collection = db[GEOMETRY_COLLECTION]
            telem_collection = db[TELEMETRY_COLLECTION]

            # ------------------------------------------------
            # Geometry idempotency
            # ------------------------------------------------

            geo_collection.create_index(
                [("node_id", ASCENDING)],
                unique=True
            )

            # ------------------------------------------------
            # Telemetry idempotency
            #
            # Allows multiple events at the same simulation
            # time as long as their event_id is different.
            # ------------------------------------------------

            telem_collection.create_index(
                [
                    ("time", ASCENDING),
                    ("event_id", ASCENDING)
                ],
                unique=True
            )

            print(
                f"[Consumer] Connected to MongoDB "
                f"'{DB_NAME}' successfully."
            )

            return (
                mongo_client,
                geo_collection,
                telem_collection
            )

        except Exception as exc:

            print(
                f"[Consumer] MongoDB connection attempt "
                f"{attempt}/{MAX_RETRIES} failed: {exc}"
            )

            if mongo_client is not None:
                mongo_client.close()

            if attempt < MAX_RETRIES:

                delay = retry_delay(attempt)

                print(
                    f"[Consumer] Retrying MongoDB connection "
                    f"in {delay} seconds..."
                )

                time.sleep(delay)

    raise RuntimeError(
        "MongoDB could not be reached after multiple attempts."
    )


# ============================================================
# Kafka connection
# ============================================================

def create_kafka_consumer():
    """
    Create and configure the Kafka consumer.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BROKER],

                group_id=KAFKA_GROUP_ID,

                # Replay messages if no committed offset exists.
                auto_offset_reset="earliest",

                # Commit only after successful MongoDB persistence.
                enable_auto_commit=False,

                # Support large welding telemetry messages.
                max_partition_fetch_bytes=MAX_PARTITION_FETCH_BYTES,
                fetch_max_bytes=FETCH_MAX_BYTES,

                # Decode JSON messages.
                value_deserializer=lambda x: json.loads(
                    x.decode("utf-8")
                )
            )

            consumer.subscribe(
                [
                    GEOMETRY_TOPIC,
                    TELEMETRY_TOPIC
                ]
            )

            print(
                f"[Consumer] Connected to Kafka broker "
                f"'{KAFKA_BROKER}'."
            )

            print(
                f"[Consumer] Subscribed to: "
                f"{GEOMETRY_TOPIC}, {TELEMETRY_TOPIC}"
            )

            return consumer

        except KafkaError as exc:

            print(
                f"[Consumer] Kafka connection attempt "
                f"{attempt}/{MAX_RETRIES} failed: {exc}"
            )

            if attempt < MAX_RETRIES:

                delay = retry_delay(attempt)

                print(
                    f"[Consumer] Retrying Kafka connection "
                    f"in {delay} seconds..."
                )

                time.sleep(delay)

    raise RuntimeError(
        "Kafka broker could not be reached after "
        "multiple attempts."
    )


# ============================================================
# Validation
# ============================================================

def validate_geometry_record(record):
    """
    Validate and normalize a geometry record.
    """

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
            f"Invalid node_id value: "
            f"{record['node_id']}"
        )

    record["node_id"] = node_id

    return node_id


def validate_telemetry_record(record):
    """
    Validate telemetry and ensure that an event_id exists.
    """

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

    # Prefer the event_id generated by the producer.
    # Generate one only if it is missing.
    if not record.get("event_id"):

        record["event_id"] = generate_stable_id(
            record
        )

    return (
        record["time"],
        record["event_id"]
    )


# ============================================================
# MongoDB persistence
# ============================================================

def store_geometry(
    record,
    geo_collection
):
    """
    Store geometry with an idempotent upsert.
    """

    node_id = validate_geometry_record(
        record
    )

    geo_collection.update_one(
        {"node_id": node_id},
        {"$set": record},
        upsert=True
    )

    # Reduce logging for large meshes.
    if node_id % 1000 == 0:

        print(
            f"[Consumer] Saved geometry node "
            f"#{node_id} -> "
            f"'{GEOMETRY_COLLECTION}'"
        )


def store_telemetry(
    record,
    telem_collection
):
    """
    Store telemetry using (time, event_id) as the
    idempotency key.
    """

    sim_time, event_id = validate_telemetry_record(
        record
    )

    telem_collection.update_one(
        {
            "time": sim_time,
            "event_id": event_id
        },
        {
            "$set": record
        },
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
    consumer = None

    try:

        # ----------------------------------------------------
        # 1. MongoDB
        # ----------------------------------------------------

        (
            mongo_client,
            geo_collection,
            telem_collection
        ) = connect_to_mongodb()

        # ----------------------------------------------------
        # 2. Kafka
        # ----------------------------------------------------

        consumer = create_kafka_consumer()

        print(
            "[Consumer] Actively listening for "
            "3D welding Digital Twin events..."
        )

        # ----------------------------------------------------
        # 3. Streaming loop
        # ----------------------------------------------------

        for message in consumer:

            record = message.value
            current_topic = message.topic

            topic_partition = TopicPartition(
                message.topic,
                message.partition
            )

            offset_and_metadata = OffsetAndMetadata(
                message.offset + 1,
                ""
            )

            processed = False

            # ------------------------------------------------
            # Retry processing of the current message.
            # ------------------------------------------------

            for attempt in range(
                1,
                MAX_RETRIES + 1
            ):

                try:

                    # ========================================
                    # Geometry
                    # ========================================

                    if current_topic == GEOMETRY_TOPIC:

                        store_geometry(
                            record,
                            geo_collection
                        )

                        print(
                            f"[Consumer] Geometry event persisted "
                            f"(partition={message.partition}, "
                            f"offset={message.offset})"
                        )

                    # ========================================
                    # Telemetry
                    # ========================================

                    elif current_topic == TELEMETRY_TOPIC:

                        sim_time = store_telemetry(
                            record,
                            telem_collection
                        )

                        print(
                            f"[Consumer] Stored telemetry frame "
                            f"at t={sim_time}s "
                            f"(partition={message.partition}, "
                            f"offset={message.offset})"
                        )

                    else:

                        raise ValueError(
                            f"Unexpected Kafka topic: "
                            f"{current_topic}"
                        )

                    # ------------------------------------------------
                    # IMPORTANT:
                    # Commit only after MongoDB write succeeds.
                    # ------------------------------------------------

                    consumer.commit(
                        offsets={
                            topic_partition:
                                offset_and_metadata
                        }
                    )

                    processed = True
                    break

                # ------------------------------------------------
                # Database or Kafka temporary failure
                # ------------------------------------------------

                except (
                    PyMongoError,
                    KafkaError
                ) as exc:

                    print(
                        f"[Consumer] Processing attempt "
                        f"{attempt}/{MAX_RETRIES} failed: "
                        f"{exc}"
                    )

                    if attempt < MAX_RETRIES:

                        delay = retry_delay(
                            attempt
                        )

                        print(
                            f"[Consumer] Retrying in "
                            f"{delay} seconds..."
                        )

                        time.sleep(delay)

                # ------------------------------------------------
                # Invalid data
                # ------------------------------------------------

                except (
                    ValueError,
                    TypeError
                ) as exc:

                    print(
                        f"[Consumer] Invalid message "
                        f"(skipping): {exc}"
                    )

                    # A malformed message should not block
                    # the entire Kafka partition forever.
                    try:

                        consumer.commit(
                            offsets={
                                topic_partition:
                                    offset_and_metadata
                            }
                        )

                        processed = True

                    except KafkaError as commit_error:

                        print(
                            f"[Consumer] Failed to commit "
                            f"invalid message offset: "
                            f"{commit_error}"
                        )

                    break

                # ------------------------------------------------
                # Unexpected temporary/runtime failure
                # ------------------------------------------------

                except Exception as exc:

                    print(
                        f"[Consumer] Unexpected processing "
                        f"error on attempt "
                        f"{attempt}/{MAX_RETRIES}: {exc}"
                    )

                    if attempt < MAX_RETRIES:

                        delay = retry_delay(
                            attempt
                        )

                        print(
                            f"[Consumer] Retrying in "
                            f"{delay} seconds..."
                        )

                        time.sleep(delay)

            # ------------------------------------------------
            # If processing completely failed:
            #
            # Do NOT commit the offset.
            # Docker can restart the container and Kafka can
            # replay the uncommitted message.
            # ------------------------------------------------

            if not processed:

                raise RuntimeError(
                    f"Failed to process Kafka message after "
                    f"{MAX_RETRIES} attempts. "
                    f"Offset {message.offset} remains "
                    f"uncommitted for replay."
                )

    except KeyboardInterrupt:

        print(
            "\n[Consumer] Process stopped manually."
        )

    except Exception as exc:

        print(
            f"[Consumer] Critical consumer error: {exc}"
        )

        # Important for Docker restart policy.
        raise

    finally:

        # ----------------------------------------------------
        # Clean Kafka shutdown
        # ----------------------------------------------------

        if consumer is not None:

            try:

                consumer.close()

                print(
                    "[Consumer] Kafka consumer closed."
                )

            except Exception as exc:

                print(
                    f"[Consumer] Kafka shutdown error: "
                    f"{exc}"
                )

        # ----------------------------------------------------
        # Clean MongoDB shutdown
        # ----------------------------------------------------

        if mongo_client is not None:

            try:

                mongo_client.close()

                print(
                    "[Consumer] MongoDB connection closed."
                )

            except Exception as exc:

                print(
                    f"[Consumer] MongoDB shutdown error: "
                    f"{exc}"
                )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    start_streaming_consumer()