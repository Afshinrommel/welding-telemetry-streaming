import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from pymongo import MongoClient, errors

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "telemetry_fault_tolerance_audit"
MONGO_URI = "mongodb://localhost:27017"
UNREACHABLE_MONGO_URI = "mongodb://127.0.0.1:27019"
TEST_DB_NAME = "welding_test_sandbox"
TEST_COLLECTION_NAME = "telemetry_fault_tolerance_records"

def run_fault_tolerance_test():
    client = None
    producer = None
    consumer = None
    recovery_consumer = None
    test_run_id = str(uuid.uuid4())

    try:
        # 1. Environment and database safety verification
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")

        if client.address is None or client.address[0] not in ("localhost", "127.0.0.1", "::1"):
            raise RuntimeError("Safety check failed: MongoDB must be local.")

        target_db = client[TEST_DB_NAME]
        if target_db.name != "welding_test_sandbox":
            raise RuntimeError(f"Database safety validation failed: unexpected target '{target_db.name}'")

        collection = target_db[TEST_COLLECTION_NAME]
        if collection.name != "telemetry_fault_tolerance_records":
            raise RuntimeError(f"Collection safety validation failed: unexpected target '{collection.name}'")

        collection.drop()

        # Enforce unique compound index scoped by run_id and sequence_id for idempotent writes
        collection.create_index(
            [("run_id", 1), ("sequence_id", 1)],
            unique=True,
            name="idx_unique_run_sequence"
        )
        print(f"[1/6] Sandbox collection reset. Unique index enforced. Run ID: {test_run_id}")

        # 2. Setup Kafka Producer with durable acknowledgments
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3
        )

        total_baseline = 5
        total_outage = 10
        total_expected = total_baseline + total_outage

        # Isolated Consumer Group ID for deterministic offset tracking
        consumer_group_id = f"group_ft_{test_run_id[:8]}"

        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=consumer_group_id
        )

        # 3. Stream baseline telemetry and consume with deadline-based loop
        print(f"[2/6] Streaming initial {total_baseline} baseline frames to operational pipeline...")
        for i in range(total_baseline):
            payload = {
                "run_id": test_run_id,
                "sequence_id": i,
                "node_id": 9038 + i,
                "time_step": round(10.0 + (i * 0.02), 2),
                "temperature": 1200.0 + (i * 10),
                "phase": "online_operational"
            }
            producer.send(TOPIC_NAME, payload)
        producer.flush()

        baseline_persisted = 0
        baseline_deadline = time.time() + 15.0

        while baseline_persisted < total_baseline:
            if time.time() > baseline_deadline:
                raise RuntimeError(
                    f"Baseline consumption timed out. Expected {total_baseline} messages, persisted {baseline_persisted}."
                )

            for msg in consumer:
                tp = TopicPartition(msg.topic, msg.partition)
                next_offset = OffsetAndMetadata(msg.offset + 1, "")
                val = msg.value

                # Acknowledge and skip messages from earlier runs
                if val.get("run_id") != test_run_id:
                    consumer.commit({tp: next_offset})
                    continue

                collection.update_one(
                    {"run_id": val["run_id"], "sequence_id": val["sequence_id"]},
                    {"$setOnInsert": val},
                    upsert=True
                )
                consumer.commit({tp: next_offset})
                baseline_persisted += 1

                if baseline_persisted == total_baseline:
                    break

        print(f"[3/6] Baseline phase verified: {baseline_persisted} frames persisted with explicit per-message offset commits.")

        # 4. Stream buffered outage messages and simulate MongoDB write failure
        print(f"[4/6] Publishing {total_outage} frames to Kafka buffer during simulated outage...")
        for i in range(total_baseline, total_expected):
            payload = {
                "run_id": test_run_id,
                "sequence_id": i,
                "node_id": 9038 + i,
                "time_step": round(10.0 + (i * 0.02), 2),
                "temperature": 1350.0 + (i * 10),
                "phase": "buffered_during_outage"
            }
            producer.send(TOPIC_NAME, payload)
        producer.flush()

        # Controlled write failure simulation: targeting unreachable endpoint (port 27019)
        unreachable_client = MongoClient(UNREACHABLE_MONGO_URI, serverSelectionTimeoutMS=1000)
        unreachable_coll = unreachable_client[TEST_DB_NAME][TEST_COLLECTION_NAME]

        write_failure_intercepted = False
        outage_deadline = time.time() + 15.0

        try:
            while not write_failure_intercepted:
                if time.time() > outage_deadline:
                    raise RuntimeError("Timed out waiting to intercept simulated write failure.")

                for msg in consumer:
                    tp = TopicPartition(msg.topic, msg.partition)
                    next_offset = OffsetAndMetadata(msg.offset + 1, "")
                    val = msg.value

                    if val.get("run_id") != test_run_id:
                        consumer.commit({tp: next_offset})
                        continue

                    try:
                        unreachable_coll.update_one(
                            {"run_id": val["run_id"], "sequence_id": val["sequence_id"]},
                            {"$setOnInsert": val},
                            upsert=True
                        )
                        consumer.commit({tp: next_offset})
                    except errors.PyMongoError:
                        # Controlled write failure: intercept error and do NOT commit offset
                        write_failure_intercepted = True
                        break
        finally:
            unreachable_client.close()
            consumer.close()
            consumer = None

        if not write_failure_intercepted:
            raise RuntimeError("Test setup failed: Controlled write failure was not triggered.")

        print("      Controlled write failure intercepted. Consumer closed with uncommitted offsets retained in Kafka.")

        # 5. Recovery Consumer: Reuse same Consumer Group ID with deadline validation
        print("[5/6] Initializing recovery consumer to replay uncommitted messages...")
        recovery_consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=consumer_group_id
        )

        recovery_drained = 0
        recovery_deadline = time.time() + 20.0

        try:
            while recovery_drained < total_outage:
                if time.time() > recovery_deadline:
                    raise RuntimeError(
                        f"Recovery consumption timed out. Expected {total_outage} outage messages, processed {recovery_drained}."
                    )

                for msg in recovery_consumer:
                    tp = TopicPartition(msg.topic, msg.partition)
                    next_offset = OffsetAndMetadata(msg.offset + 1, "")
                    val = msg.value

                    if val.get("run_id") != test_run_id:
                        recovery_consumer.commit({tp: next_offset})
                        continue

                    collection.update_one(
                        {"run_id": val["run_id"], "sequence_id": val["sequence_id"]},
                        {"$setOnInsert": val},
                        upsert=True
                    )
                    recovery_consumer.commit({tp: next_offset})
                    recovery_drained += 1

                    if recovery_drained == total_outage:
                        break
        finally:
            recovery_consumer.close()
            recovery_consumer = None

        print(f"      Replayed and explicitly committed {recovery_drained}/{total_outage} outage messages during recovery phase.")

        # 6. Scope-Specific Verification and Data Integrity Audit
        persisted_documents = list(
            collection.find({"run_id": test_run_id}, {"_id": 0, "sequence_id": 1, "phase": 1}).sort("sequence_id", 1)
        )
        persisted_count = len(persisted_documents)
        persisted_seq_ids = [doc["sequence_id"] for doc in persisted_documents]
        expected_seq_ids = list(range(total_expected))

        baseline_docs_count = collection.count_documents({"run_id": test_run_id, "phase": "online_operational"})
        outage_docs_count = collection.count_documents({"run_id": test_run_id, "phase": "buffered_during_outage"})

        print("[6/6] Storage and Integrity Audit (Run ID Scope):")
        print(f"      - Expected total records: {total_expected}")
        print(f"      - Actual persisted records: {persisted_count}")
        print(f"      - Baseline records: {baseline_docs_count}/{total_baseline}")
        print(f"      - Outage buffer records: {outage_docs_count}/{total_outage}")
        print(f"      - Sequence IDs continuity: {persisted_seq_ids == expected_seq_ids}")

        if persisted_count != total_expected:
            raise AssertionError(f"Record count mismatch: Expected {total_expected}, found {persisted_count}")

        if persisted_seq_ids != expected_seq_ids:
            raise AssertionError(f"Sequence continuity failure or duplicates found: {persisted_seq_ids}")

        if baseline_docs_count != total_baseline:
            raise AssertionError(f"Baseline record mismatch: Expected {total_baseline}, found {baseline_docs_count}")

        if outage_docs_count != total_outage:
            raise AssertionError(f"Outage record retention mismatch: Expected {total_outage}, found {outage_docs_count}")

        print("SUCCESS: Recovery from simulated MongoDB write failure and reliable message replay verified.")

    except Exception as e:
        print(f"FAILED: Fault tolerance test encountered an exception: {e}")
        raise
    finally:
        if producer:
            producer.close()
        if consumer:
            consumer.close()
        if recovery_consumer:
            recovery_consumer.close()
        if client:
            client.close()

if __name__ == "__main__":
    run_fault_tolerance_test()