import concurrent.futures
import time
from pymongo import MongoClient, errors

MONGO_URI = "mongodb://localhost:27017"
TEST_DB_NAME = "welding_test_sandbox"
TEST_COLLECTION_NAME = "telemetry_idempotency_audit"
EXPECTED_KEY_PATTERN = {"node_id": 1, "time_step": 1}

def run_idempotency_stress_test():
    client = None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        
        # 1. Deterministic safety verification for local execution environment
        if client.address is None or client.address[0] not in ("localhost", "127.0.0.1", "::1"):
            raise RuntimeError("Safety check failed: MongoDB must be local.")

        target_db = client[TEST_DB_NAME]
        if target_db.name != "welding_test_sandbox":
            raise RuntimeError(f"Database safety validation failed: unexpected target '{target_db.name}'")
            
        collection = target_db[TEST_COLLECTION_NAME]
        if collection.name != "telemetry_idempotency_audit":
            raise RuntimeError(f"Collection safety validation failed: unexpected target '{collection.name}'")
        
        collection.drop()

        # 2. Build unique compound index on composite business key
        collection.create_index(
            [("node_id", 1), ("time_step", 1)],
            unique=True,
            name="idx_unique_node_timestep"
        )
        print("[1/6] Unique compound index created on (node_id, time_step).")

        # Ingestion worker with strict key pattern and value validation
        def ingest_telemetry_worker(payload):
            query_filter = {
                "node_id": payload["node_id"],
                "time_step": payload["time_step"]
            }
            update_op = {
                "$setOnInsert": {
                    "node_id": payload["node_id"],
                    "time_step": payload["time_step"],
                    "temperature": payload["temperature"],
                    "stress_mises": payload["stress_mises"]
                },
                "$set": {
                    "last_seen_timestamp": time.time()
                }
            }
            
            max_retries = 3
            backoff_base = 0.01

            for attempt in range(max_retries):
                try:
                    res = collection.update_one(query_filter, update_op, upsert=True)
                    if res.upserted_id is not None:
                        return "inserted"
                    return "deduplicated"
                except errors.DuplicateKeyError as err:
                    details = getattr(err, "details", None)
                    if not isinstance(details, dict):
                        raise

                    # Validate exact structured keyPattern dictionary match
                    key_pattern = details.get("keyPattern")
                    if key_pattern != EXPECTED_KEY_PATTERN:
                        raise

                    # Validate structured keyValue dictionary match
                    key_value = details.get("keyValue")
                    if not isinstance(key_value, dict):
                        raise
                        
                    if (
                        key_value.get("node_id") != payload["node_id"] or 
                        key_value.get("time_step") != payload["time_step"]
                    ):
                        raise

                    # Inspect document existence persisted by the winning thread
                    existing_doc = collection.find_one(query_filter, {"_id": 1})
                    if existing_doc is not None:
                        return "deduplicated"

                    if attempt == max_retries - 1:
                        raise RuntimeError(
                            f"Persistent DuplicateKeyError on {query_filter} without visible document."
                        )
                    time.sleep(backoff_base * (2 ** attempt))

        # -------------------------------------------------------------
        # STAGE 1: Deterministic Baseline & Immutability Verification
        # -------------------------------------------------------------
        target_node_id = 9038
        target_time_step = 11.20
        baseline_temp = 1305.4
        baseline_stress = 265.1

        initial_filter = {"node_id": target_node_id, "time_step": target_time_step}
        initial_payload = {
            "node_id": target_node_id,
            "time_step": target_time_step,
            "temperature": baseline_temp,
            "stress_mises": baseline_stress
        }
        
        init_res = collection.update_one(
            initial_filter,
            {
                "$setOnInsert": initial_payload,
                "$set": {"last_seen_timestamp": time.time()}
            },
            upsert=True
        )
        assert init_res.upserted_id is not None, "Failed to initialize deterministic baseline record."
        print(f"[2/6] Baseline record seeded (Temp: {baseline_temp} C, Stress: {baseline_stress} MPa).")

        competing_count = 99
        competing_payloads = [
            {
                "node_id": target_node_id,
                "time_step": target_time_step,
                "temperature": 1400.0 + (i % 20),
                "stress_mises": 320.0 + (i % 10)
            }
            for i in range(competing_count)
        ]

        print(f"[3/6] Dispatching {competing_count} concurrent overwrite attempts...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            stage1_futures = [executor.submit(ingest_telemetry_worker, p) for p in competing_payloads]
            stage1_results = [f.result() for f in concurrent.futures.as_completed(stage1_futures)]

        s1_inserted = stage1_results.count("inserted")
        s1_dedup = stage1_results.count("deduplicated")
        print(f"      Stage 1 Results: {s1_inserted} inserts, {s1_dedup} deduplicated.")

        assert s1_inserted == 0, f"Integrity Failure: Expected 0 inserts, observed {s1_inserted}"
        assert s1_dedup == competing_count, f"Tracking Failure: Expected {competing_count} deduplicated, observed {s1_dedup}"

        baseline_doc = collection.find_one(initial_filter)
        assert baseline_doc["temperature"] == baseline_temp, "Data Corruption: Baseline temperature altered"
        assert baseline_doc["stress_mises"] == baseline_stress, "Data Corruption: Baseline stress altered"
        print("[4/6] Immutability verified: Existing values retained under concurrent updates.")

        # -------------------------------------------------------------
        # STAGE 2: Cold Race Condition Ingestion (100 Requests, Up to 20 Concurrent Workers)
        # -------------------------------------------------------------
        cold_node_id = 10492
        cold_time_step = 22.50
        cold_temp = 1450.0
        cold_stress = 310.5
        total_cold_requests = 100

        print(f"[5/6] Simulating cold race condition ({total_cold_requests} requests, up to 20 concurrent workers)...")
        cold_payloads = [
            {
                "node_id": cold_node_id,
                "time_step": cold_time_step,
                "temperature": cold_temp,
                "stress_mises": cold_stress
            }
            for _ in range(total_cold_requests)
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            stage2_futures = [executor.submit(ingest_telemetry_worker, p) for p in cold_payloads]
            stage2_results = [f.result() for f in concurrent.futures.as_completed(stage2_futures)]

        s2_inserted = stage2_results.count("inserted")
        s2_dedup = stage2_results.count("deduplicated")
        print(f"      Stage 2 Results: {s2_inserted} insert, {s2_dedup} deduplicated.")

        assert s2_inserted == 1, f"Concurrency Failure: Expected exactly 1 insert, observed {s2_inserted}"
        assert s2_dedup == (total_cold_requests - 1), (
            f"Deduplication Mismatch: Expected {total_cold_requests - 1}, observed {s2_dedup}"
        )

        cold_doc = collection.find_one({"node_id": cold_node_id, "time_step": cold_time_step})
        assert cold_doc is not None, "Storage Error: Target race document not found in collection"
        assert cold_doc["temperature"] == cold_temp, f"Data Corruption: Expected {cold_temp}, found {cold_doc['temperature']}"
        assert cold_doc["stress_mises"] == cold_stress, f"Data Corruption: Expected {cold_stress}, found {cold_doc['stress_mises']}"

        cold_count_in_db = collection.count_documents({"node_id": cold_node_id, "time_step": cold_time_step})
        assert cold_count_in_db == 1, f"Storage Error: Expected 1 document, found {cold_count_in_db}"

        print("[6/6] SUCCESS: Idempotency verified for the tested concurrency scenarios.")

    except Exception as e:
        print(f"FAILED: Test execution failed: {e}")
        raise
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    run_idempotency_stress_test()