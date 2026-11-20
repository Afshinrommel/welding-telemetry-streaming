#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Appendix B - Interactive failure and recovery test controller
Run this file from the project root (the folder containing docker-compose.yml).
Uses only the Python standard library plus Docker Compose CLI.
"""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "result_data_welding"
TEST_ROOT = DATA_DIR / "recovery_test" / "appendix_b"
PROMETHEUS_URL = "http://localhost:9090"
PRODUCER_METRICS_URL = "http://localhost:8000/metrics"
CONSUMER_METRICS_URL = "http://localhost:8001/metrics"

# Compose service names from the project's docker-compose configuration.
MONGO_SERVICE = "mongodb"
PRODUCER_SERVICE = "producer"
CONSUMER_SERVICE = "consumer"

POLL_SECONDS = 5
STARTUP_TIMEOUT_SECONDS = 240
BASELINE_TIMEOUT_SECONDS = 180
FAILURE_DETECTION_TIMEOUT_SECONDS = 90
RECOVERY_TIMEOUT_SECONDS = 240
OUTAGE_MINIMUM_SECONDS = 25
MONGO_RESTORE_TIMEOUT_SECONDS = 120

# All pre-existing files under result_data_welding are fingerprinted, except
# recovery_test, where this controller writes its own report and screenshots.
SCREENSHOTS = [
    (
        "Figure_B1_Normal_Operation.png",
        "Pipeline Performance",
        "Open the Pipeline Performance view. Ideally, all four panels—especially "
        "Producer vs Consumer Throughput and Simulation Processing Progress—should be visible.",
    ),
    (
        "Figure_B2_MongoDB_Failure.png",
        "Service Health & Errors",
        "Open the Service Health & Errors view. Capture the status of Prometheus Scrape "
        "Availability, Internal Connection Health, and Consumer errors.",
    ),
    (
        "Figure_B3_Processing_Interruption.png",
        "Data Persistence & Recovery",
        "Open the Data Persistence & Recovery view. Capture the increasing time since "
        "the last successful processing and the stagnant offset.",
    ),
    (
        "Figure_B4_Recovery.png",
        "Data Persistence & Recovery + Service Health & Errors",
        "After recovery, capture the Data Persistence & Recovery and Service Health & Errors "
        "views; the resumption of successful processing and MongoDB connection should be visible.",
    ),
]


class TestLogger:
    def __init__(self, path: Path):
        self.path = path
        self.logger = logging.getLogger("appendix_b")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setFormatter(formatter)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(sh)

    def info(self, message: str):
        self.logger.info(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def error(self, message: str):
        self.logger.error(message)

    def section(self, title: str):
        self.info("")
        self.info("=" * 76)
        self.info(title)
        self.info("=" * 76)


LOG: TestLogger | None = None


def run_cmd(
    args: list[str], *, timeout: int = 120, check: bool = True
) -> subprocess.CompletedProcess:
    """Run a command in the project root and return captured output."""
    try:
        result = subprocess.run(
            args,
            cwd=PROJECT_ROOT,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        output = ""
        if exc.stdout:
            output = (
                exc.stdout.decode("utf-8", errors="replace")
                if isinstance(exc.stdout, bytes)
                else str(exc.stdout)
            )
        if LOG is not None:
            LOG.warning(f"Command timed out after {timeout}s: {' '.join(args)}")
        if check:
            raise RuntimeError(
                f"Command timed out after {timeout}s: {' '.join(args)}\n{output}"
            ) from exc
        return subprocess.CompletedProcess(args, returncode=124, stdout=output, stderr="")

    if result.stdout.strip() and LOG is not None:
        LOG.info("$ " + " ".join(args))
        for line in result.stdout.rstrip().splitlines()[-80:]:
            LOG.info("  " + line)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(args)}\n{result.stdout}"
        )
    return result


def compose(*args: str, timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
    return run_cmd(["docker", "compose", *args], timeout=timeout, check=check)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def source_inventory() -> dict[str, dict[str, Any]]:
    inventory: dict[str, dict[str, Any]] = {}
    for path in sorted(DATA_DIR.rglob("*")):
        if not path.is_file():
            continue
        # The test writes logs/screenshots under recovery_test; do not fingerprint
        # its own generated output as original simulation input.
        try:
            relative_to_data = path.relative_to(DATA_DIR)
        except ValueError:
            continue
        if relative_to_data.parts and relative_to_data.parts[0].lower() == "recovery_test":
            continue
        rel = str(path.relative_to(PROJECT_ROOT))
        inventory[rel] = {
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        assert LOG is not None
        LOG.info(
            f"Source file: {rel} | size={path.stat().st_size} bytes | "
            f"SHA256={inventory[rel]['sha256']}"
        )
    return inventory


def http_json(url: str, timeout: int = 4) -> Any:
    req = urllib.request.Request(
        url, headers={"User-Agent": "AppendixB-TestController/1.0"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def prometheus_query(expression: str) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({"query": expression})
    try:
        data = http_json(f"{PROMETHEUS_URL}/api/v1/query?{query}", timeout=5)
        if data.get("status") != "success":
            return []
        return data.get("data", {}).get("result", [])
    except Exception:
        return []


def metric_value(expression: str) -> float | None:
    rows = prometheus_query(expression)
    if not rows:
        return None
    try:
        return float(rows[0]["value"][1])
    except (KeyError, TypeError, ValueError, IndexError):
        return None


def metric_summary() -> dict[str, float | None]:
    expressions = {
        "producer_sent_telemetry": 'welding_producer_messages_sent_total{topic="welding_telemetry"}',
        "consumer_processed_telemetry": 'welding_consumer_messages_processed_total{topic="welding_telemetry"}',
        "producer_kafka_connected": "welding_producer_kafka_connected",
        "consumer_kafka_connected": "welding_consumer_kafka_connected",
        "consumer_mongodb_connected": "welding_consumer_mongodb_connected",
        "producer_last_success_epoch": "welding_producer_last_successful_send_timestamp_seconds",
        "consumer_last_success_epoch": "welding_consumer_last_successful_processing_timestamp_seconds",
        "consumer_processing_errors": "sum(welding_consumer_processing_errors_total)",
        "producer_send_errors": "sum(welding_producer_send_errors_total)",
        "producer_up": 'up{job="welding_producer"}',
        "consumer_up": 'up{job="welding_consumer"}',
    }
    return {name: metric_value(expr) for name, expr in expressions.items()}


def log_metrics(label: str) -> dict[str, float | None]:
    assert LOG is not None
    values = metric_summary()
    LOG.info(f"METRICS SNAPSHOT: {label}")
    for name, value in values.items():
        LOG.info(
            f"  {name} = {'N/A (no series / scrape unavailable)' if value is None else value}"
        )
    return values


def mongo_ping() -> bool:
    try:
        result = compose(
            "exec",
            "-T",
            MONGO_SERVICE,
            "mongosh",
            "--quiet",
            "--eval",
            "db.adminCommand({ping:1}).ok",
            timeout=15,
            check=False,
        )
        return result.returncode == 0 and result.stdout.strip() in {"1", "1.0"}
    except Exception as exc:
        if LOG is not None:
            LOG.warning(f"MongoDB ping check raised an exception: {exc}")
        return False


def wait_until(
    description: str, predicate, timeout: int, interval: int = POLL_SECONDS
) -> bool:
    assert LOG is not None
    LOG.info(f"Waiting for: {description} (timeout={timeout}s)")
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            if predicate():
                LOG.info(f"Condition satisfied: {description}")
                return True
        except Exception as exc:
            LOG.warning(f"Check temporarily unavailable: {exc}")
        time.sleep(interval)
    LOG.warning(f"Timed out waiting for: {description}")
    return False


def ensure_mongo_restored(context: str) -> bool:
    """Attempt to restart MongoDB and confirm ping succeeds. Never raises."""
    assert LOG is not None
    LOG.section(f"CLEANUP — GUARANTEED MONGODB RESTORATION ({context})")
    try:
        LOG.info(f"Attempting automatic restart of {MONGO_SERVICE}...")
        start_res = compose("start", MONGO_SERVICE, timeout=60, check=False)
        if start_res.returncode != 0:
            LOG.error(
                f"Automatic 'docker compose start {MONGO_SERVICE}' exited with "
                f"code {start_res.returncode}."
            )
        mongo_ready = wait_until(
            "MongoDB ping succeeds during guaranteed cleanup",
            mongo_ping,
            MONGO_RESTORE_TIMEOUT_SECONDS,
        )
        if mongo_ready:
            LOG.info("Guaranteed cleanup succeeded: MongoDB is running and responding to ping.")
            return True
    except Exception as exc:
        LOG.error(f"Exception during automatic MongoDB restoration: {exc}")

    LOG.error("Automatic MongoDB restoration FAILED. Manual intervention is required.")
    LOG.error(f"Manual recovery command: docker compose start {MONGO_SERVICE}")
    print("\n" + "!" * 76)
    print("ERROR: Automatic MongoDB restoration FAILED.")
    print("Run the following manual recovery command from the project root:")
    print(f"  docker compose start {MONGO_SERVICE}")
    print("!" * 76)
    return False


def wait_for_services() -> bool:
    def ready() -> bool:
        mongo_ok = mongo_ping()
        producer_up = metric_value('up{job="welding_producer"}')
        consumer_up = metric_value('up{job="welding_consumer"}')
        return mongo_ok and producer_up == 1 and consumer_up == 1

    return wait_until(
        "MongoDB ping and Producer/Consumer Prometheus targets UP",
        ready,
        STARTUP_TIMEOUT_SECONDS,
    )


def wait_for_baseline() -> tuple[bool, dict[str, float | None]]:
    assert LOG is not None
    LOG.info("Waiting for telemetry messages to be sent and processed...")
    start = time.monotonic()
    while time.monotonic() - start < BASELINE_TIMEOUT_SECONDS:
        values = metric_summary()
        sent = values["producer_sent_telemetry"]
        processed = values["consumer_processed_telemetry"]
        if sent is not None and processed is not None and sent >= 5 and processed >= 5:
            LOG.info(f"Baseline traffic observed: sent={sent}, processed={processed}")
            return True, values
        time.sleep(POLL_SECONDS)
    return False, metric_summary()


def prompt_screenshot(
    filename: str, dashboard: str, instructions: str, screenshot_dir: Path
):
    assert LOG is not None
    LOG.section(f"SCREENSHOT REQUEST: {filename}")
    LOG.info(f"Grafana dashboard: {dashboard}")
    LOG.info(instructions)
    LOG.info(f"Save the screenshot here:\n{screenshot_dir / filename}")
    input("\nAfter saving the screenshot to the path above, press Enter to continue...")
    saved = screenshot_dir / filename
    if saved.is_file():
        LOG.info(f"Screenshot file confirmed: {saved}")
    else:
        LOG.warning(f"Screenshot file was not found at the expected path: {saved}")
        answer = (
            input("File not saved. Do you want to continue without confirming the file? (y/N): ")
            .strip()
            .lower()
        )
        if answer != "y":
            input("Save the screenshot and then press Enter...")
            if saved.is_file():
                LOG.info(f"Screenshot file confirmed: {saved}")
            else:
                LOG.warning(
                    "Screenshot still not found; continuing and recording this limitation."
                )


def get_mongo_collection_counts() -> tuple[dict[str, dict[str, int]] | None, str]:
    """Return parsed MongoDB collection counts and a formatted representation."""
    # Discover database and collection names dynamically; no schema names are assumed.
    js = (
        "const out={}; "
        "db.adminCommand({listDatabases:1}).databases.forEach(d=>{"
        "if(d.name!=='admin' && d.name!=='local' && d.name!=='config'){"
        "const x=db.getSiblingDB(d.name); out[d.name]={}; "
        "x.getCollectionNames().forEach(c=>{out[d.name][c]=x.getCollection(c).countDocuments({});});"
        "}}); print(JSON.stringify(out));"
    )
    result = compose(
        "exec", "-T", MONGO_SERVICE, "mongosh", "--quiet", "--eval", js,
        timeout=30, check=False,
    )
    if result.returncode != 0:
        return None, "UNAVAILABLE: " + result.stdout[-1000:]
    for line in reversed(result.stdout.splitlines()):
        try:
            parsed = json.loads(line.strip())
            if isinstance(parsed, dict):
                return parsed, json.dumps(parsed, ensure_ascii=False, indent=2)
        except Exception:
            continue
    return None, "No database/collection count JSON returned. Raw output:\n" + result.stdout[-1000:]


def log_mongo_count_comparison(
    before: dict[str, dict[str, int]] | None,
    after: dict[str, dict[str, int]] | None,
) -> dict[str, Any]:
    """Log before-and-after MongoDB collection counts and return a comparison summary."""
    assert LOG is not None
    summary: dict[str, Any] = {
        "before_available": before is not None,
        "after_available": after is not None,
        "total_before": None,
        "total_after": None,
        "total_delta": None,
        "collections": {},
    }
    if before is None or after is None:
        LOG.warning(
            "MongoDB before/after persistence comparison is incomplete "
            f"(before_available={before is not None}, after_available={after is not None})."
        )
        return summary

    all_dbs = sorted(set(before.keys()) | set(after.keys()))
    total_before = 0
    total_after = 0
    LOG.info("MongoDB collection document count comparison (before failure vs. after recovery):")
    if not all_dbs:
        LOG.info("  No non-system MongoDB databases found in either snapshot.")
    for db_name in all_dbs:
        before_cols = before.get(db_name, {})
        after_cols = after.get(db_name, {})
        all_cols = sorted(set(before_cols.keys()) | set(after_cols.keys()))
        for col_name in all_cols:
            b_val = int(before_cols.get(col_name, 0))
            a_val = int(after_cols.get(col_name, 0))
            delta = a_val - b_val
            total_before += b_val
            total_after += a_val
            key = f"{db_name}.{col_name}"
            summary["collections"][key] = {
                "before": b_val,
                "after": a_val,
                "delta": delta,
            }
            sign = "+" if delta > 0 else ""
            LOG.info(f"  {key}: before={b_val}, after={a_val}, diff={sign}{delta}")

    total_delta = total_after - total_before
    sign = "+" if total_delta > 0 else ""
    summary["total_before"] = total_before
    summary["total_after"] = total_after
    summary["total_delta"] = total_delta
    LOG.info(
        f"  TOTAL across all non-system collections: "
        f"before={total_before}, after={total_after}, diff={sign}{total_delta}"
    )
    LOG.info(
        "Note: Because the Consumer uses idempotent upserts, replayed or duplicate "
        "records may update existing documents without increasing every collection's count."
    )
    return summary


def container_snapshot(label: str):
    assert LOG is not None
    LOG.info(f"DOCKER SERVICE SNAPSHOT: {label}")
    compose("ps", timeout=30, check=False)


def capture_consumer_logs(label: str):
    assert LOG is not None
    LOG.info(f"CONSUMER LOG EXCERPT: {label}")
    result = compose(
        "logs", "--no-color", "--tail", "100", CONSUMER_SERVICE,
        timeout=30, check=False,
    )
    if result.stdout:
        for line in result.stdout.splitlines()[-100:]:
            LOG.info("  " + line)


def wait_for_consumer_failure() -> bool:
    def failed() -> bool:
        up = metric_value('up{job="welding_consumer"}')
        mongo_connected = metric_value("welding_consumer_mongodb_connected")
        # Do not infer failure merely because a counter has not changed between
        # two close samples. Require direct evidence from scrape availability or
        # the Consumer's MongoDB connection gauge.
        return up == 0 or mongo_connected == 0

    return wait_until(
        "observable Consumer interruption / MongoDB connection failure",
        failed,
        FAILURE_DETECTION_TIMEOUT_SECONDS,
    )


def evaluate_producer_continuity(
    sent_before: float | None,
    outage_snapshots: list[dict[str, float | None]],
) -> tuple[bool, str, str]:
    """
    Verify whether the Producer remained healthy, stayed connected to Kafka,
    and continued sending messages without restarting during the MongoDB outage.
    """
    if sent_before is None or not outage_snapshots:
        return (
            False,
            "INCONCLUSIVE",
            "Producer pre-outage sent counter or outage metric snapshots were unavailable.",
        )

    sent_samples = [snap.get("producer_sent_telemetry") for snap in outage_snapshots]
    up_samples = [snap.get("producer_up") for snap in outage_snapshots]
    kafka_samples = [snap.get("producer_kafka_connected") for snap in outage_snapshots]

    if (
        any(v is None for v in sent_samples)
        or any(v is None for v in up_samples)
        or any(v is None for v in kafka_samples)
    ):
        return (
            False,
            "INCONCLUSIVE",
            "One or more required Producer continuity metrics (sent counter, up, or "
            f"Kafka connection) were unavailable during the outage "
            f"(sent={sent_samples}, up={up_samples}, kafka_connected={kafka_samples}).",
        )

    if any(v != 1 for v in up_samples):
        return (
            False,
            "FAIL",
            f"Producer Prometheus target did not remain UP throughout the outage "
            f"(up={up_samples}).",
        )

    if any(v != 1 for v in kafka_samples):
        return (
            False,
            "FAIL",
            f"Producer Kafka connection did not remain healthy throughout the outage "
            f"(kafka_connected={kafka_samples}).",
        )

    sequence = [sent_before, *[float(v) for v in sent_samples if v is not None]]
    if any(sequence[i] < sequence[i - 1] for i in range(1, len(sequence))):
        return (
            False,
            "FAIL",
            f"Producer sent-message counter decreased during the outage, indicating "
            f"a possible Producer restart (sequence={sequence}).",
        )

    latest_during_outage = sequence[-1]
    delta = latest_during_outage - sent_before
    if latest_during_outage > sent_before:
        return (
            True,
            "VERIFIED",
            f"Producer remained UP (1), stayed connected to Kafka (1), and "
            f"monotonically increased sent messages during the MongoDB outage "
            f"(before={sent_before}, samples={sent_samples}, delta=+{delta}).",
        )

    return (
        False,
        "FAIL",
        f"Producer remained UP and connected to Kafka, but sent-message counter "
        f"did not increase during the MongoDB outage "
        f"(before={sent_before}, latest_during_outage={latest_during_outage}, delta={delta}).",
    )


def wait_for_recovery(
    processed_reference: float | None,
) -> tuple[bool, dict[str, float | None], dict[str, Any]]:
    """
    Verify Consumer recovery using all four required signals:
    1. MongoDB responds to ping.
    2. Prometheus Consumer target is UP (up == 1).
    3. Consumer MongoDB connection gauge reports healthy (1).
    4. Consumer processed-message counter is available and advances after recovery.
    """
    assert LOG is not None
    LOG.info(
        "Waiting for MongoDB ping, Consumer scrape availability, healthy MongoDB "
        "connection metric, and renewed Consumer message processing..."
    )
    start = time.monotonic()
    first_post_recovery_processed: float | None = None
    last_details: dict[str, Any] = {
        "mongo_ping_ok": False,
        "consumer_up_ok": False,
        "consumer_mongo_connected_ok": False,
        "processed_metric_available": False,
        "processed_advanced_ok": False,
        "counter_reset_detected": False,
        "processed_reference": processed_reference,
        "first_post_recovery_processed": None,
        "latest_processed": None,
    }

    while time.monotonic() - start < RECOVERY_TIMEOUT_SECONDS:
        mongo_ok = mongo_ping()
        values = metric_summary()
        up = values["consumer_up"]
        processed = values["consumer_processed_telemetry"]
        mongo_connected = values["consumer_mongodb_connected"]

        up_ok = up == 1
        mongo_conn_ok = mongo_connected == 1
        metric_available = processed is not None
        advanced = False
        reset_detected = False

        if mongo_ok and up_ok and mongo_conn_ok and processed is not None:
            if first_post_recovery_processed is None:
                first_post_recovery_processed = processed
                LOG.info(
                    f"Initial post-recovery processed counter observed: {processed} "
                    f"(pre-recovery reference={processed_reference})"
                )

            if processed_reference is not None and processed < processed_reference:
                # Consumer container restart resets the in-process Prometheus counter.
                reset_detected = True
                advanced = processed > first_post_recovery_processed
            elif processed_reference is not None:
                advanced = (
                    processed > processed_reference
                    or processed > first_post_recovery_processed
                )
            else:
                # Pre-recovery reference was unavailable; require observed increase across polls.
                advanced = processed > first_post_recovery_processed

        last_details = {
            "mongo_ping_ok": mongo_ok,
            "consumer_up_ok": up_ok,
            "consumer_mongo_connected_ok": mongo_conn_ok,
            "processed_metric_available": metric_available,
            "processed_advanced_ok": advanced,
            "counter_reset_detected": reset_detected,
            "processed_reference": processed_reference,
            "first_post_recovery_processed": first_post_recovery_processed,
            "latest_processed": processed,
        }

        if mongo_ok and up_ok and mongo_conn_ok and metric_available and advanced:
            LOG.info(
                "Recovery verified: MongoDB ping OK, Consumer target UP (1), "
                "Consumer MongoDB connection gauge=1, and processed counter advanced "
                f"(reference={processed_reference}, first_post_recovery="
                f"{first_post_recovery_processed}, latest={processed}, "
                f"counter_reset_detected={reset_detected})."
            )
            return True, values, last_details

        time.sleep(POLL_SECONDS)

    LOG.warning(f"Recovery verification timed out. Last state: {last_details}")
    return False, metric_summary(), last_details


def main() -> int:
    global LOG

    compose_file_candidates = [
        PROJECT_ROOT / "docker-compose.yml",
        PROJECT_ROOT / "docker-compose.yaml",
        PROJECT_ROOT / "compose.yml",
        PROJECT_ROOT / "compose.yaml",
    ]
    compose_file = next((p for p in compose_file_candidates if p.is_file()), None)
    if compose_file is None:
        print("ERROR: No Docker Compose file was found beside this script.")
        print("Place run_failure_recovery_test.py in the project root.")
        return 2

    if not DATA_DIR.is_dir():
        print(f"ERROR: Data directory not found: {DATA_DIR}")
        return 2

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = TEST_ROOT / f"test_{timestamp}"
    screenshot_dir = run_dir / "screenshots"
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "appendix_b_failure_recovery_report.log"
    LOG = TestLogger(log_path)

    test_start = time.time()
    LOG.section("APPENDIX B — INTERACTIVE FAILURE AND RECOVERY TEST")
    LOG.info(f"Test ID: APPB-{timestamp}")
    LOG.info(f"Project root: {PROJECT_ROOT}")
    LOG.info(f"Compose file: {compose_file.name}")
    LOG.info(f"Test output directory: {run_dir}")
    LOG.info(
        "Test objective: verify Consumer interruption and recovery after a MongoDB outage, "
        "while Producer continues sending to Kafka."
    )
    LOG.info(
        "Important: this controller does not modify producer.py, consumer.py, "
        "Docker Compose configuration, Grafana dashboards, or source simulation files."
    )

    LOG.section("PHASE 0 — PRE-RESET SOURCE DATA INTEGRITY")
    before_hashes = source_inventory()
    LOG.info(
        "The host-side result_data_welding directory is bind-mounted into the Producer. "
        "Docker volume cleanup does not delete this host directory."
    )

    print("\n" + "!" * 76)
    print("WARNING: The next step will remove all Compose containers and their volumes.")
    print("Current MongoDB data and other data stored in Docker volumes will be lost.")
    print("Simulation files inside result_data_welding on Windows will not be deleted.")
    print("!" * 76)
    confirm = input("To start completely from scratch, type RESET exactly: ").strip()
    if confirm != "RESET":
        LOG.warning("User did not type RESET. Test cancelled before Docker cleanup.")
        print(f"\nTest cancellation log: {log_path}")
        return 1

    mongo_stopped_by_test = False
    exit_code = 2

    try:
        LOG.section("PHASE 1 — COMPLETE DOCKER RESET")
        LOG.info("Executing: docker compose down -v --remove-orphans")
        compose("down", "-v", "--remove-orphans", timeout=180)
        LOG.info("Docker Compose containers and project volumes removed.")

        LOG.section("PHASE 2 — REBUILD AND START ALL SERVICES FROM SCRATCH")
        compose("up", "-d", "--build", timeout=900)
        container_snapshot("fresh startup")
        ready = wait_for_services()
        if not ready:
            LOG.error("Initial startup did not reach the required readiness state.")
            capture_consumer_logs("startup timeout")
            raise RuntimeError("Services did not become ready within the startup timeout.")

        LOG.section("PHASE 3 — NORMAL OPERATION BASELINE")
        baseline_ok, baseline = wait_for_baseline()
        log_metrics("normal operation baseline")
        if not baseline_ok:
            LOG.warning(
                "Could not confirm at least five sent and processed telemetry messages. "
                "The failure test may be inconclusive."
            )
        prompt_screenshot(
            SCREENSHOTS[0][0], SCREENSHOTS[0][1], SCREENSHOTS[0][2], screenshot_dir
        )

        # Allow the dashboard to refresh and record a stable pre-outage baseline.
        time.sleep(10)
        baseline = log_metrics("immediately before MongoDB outage")
        sent_before = baseline.get("producer_sent_telemetry")
        processed_before = baseline.get("consumer_processed_telemetry")
        producer_last_before = baseline.get("producer_last_success_epoch")
        consumer_last_before = baseline.get("consumer_last_success_epoch")

        mongo_counts_before, mongo_counts_before_raw = get_mongo_collection_counts()
        LOG.info("MongoDB collection counts immediately before MongoDB outage:")
        LOG.info("\n" + mongo_counts_before_raw)

        LOG.section("PHASE 4 — MONGODB FAILURE INJECTION")
        failure_command_time = time.time()
        LOG.info(f"MongoDB stop command issued at epoch={failure_command_time:.3f}")
        mongo_stopped_by_test = True
        compose("stop", MONGO_SERVICE, timeout=60)
        container_snapshot("MongoDB stopped")

        failure_observed = wait_for_consumer_failure()
        failure_observed_time = time.time()
        LOG.info(f"Failure observation timestamp epoch={failure_observed_time:.3f}")
        LOG.info(
            f"Time from stop command to observable interruption: "
            f"{failure_observed_time - failure_command_time:.2f}s"
        )
        capture_consumer_logs("during MongoDB outage")

        outage_snapshots: list[dict[str, float | None]] = []
        outage_snapshot_initial = log_metrics("MongoDB outage / Consumer interruption")
        outage_snapshots.append(outage_snapshot_initial)
        consumer_processed_during_outage = outage_snapshot_initial.get(
            "consumer_processed_telemetry"
        )

        prompt_screenshot(
            SCREENSHOTS[1][0], SCREENSHOTS[1][1], SCREENSHOTS[1][2], screenshot_dir
        )

        # Keep MongoDB down long enough for multiple Prometheus scrape cycles
        # and to observe Producer continuity while MongoDB is unavailable.
        elapsed = time.time() - failure_command_time
        if elapsed < OUTAGE_MINIMUM_SECONDS:
            wait_more = OUTAGE_MINIMUM_SECONDS - elapsed
            LOG.info(
                f"Maintaining MongoDB outage for another {wait_more:.1f}s "
                "to capture the interruption and Producer continuity clearly."
            )
            time.sleep(wait_more)

        outage_snapshot_continued = log_metrics("continued MongoDB outage")
        outage_snapshots.append(outage_snapshot_continued)
        if outage_snapshot_continued.get("consumer_processed_telemetry") is not None:
            consumer_processed_during_outage = outage_snapshot_continued.get(
                "consumer_processed_telemetry"
            )

        prompt_screenshot(
            SCREENSHOTS[2][0], SCREENSHOTS[2][1], SCREENSHOTS[2][2], screenshot_dir
        )

        outage_snapshot_final = log_metrics("immediately before MongoDB restoration")
        outage_snapshots.append(outage_snapshot_final)
        if outage_snapshot_final.get("consumer_processed_telemetry") is not None:
            consumer_processed_during_outage = outage_snapshot_final.get(
                "consumer_processed_telemetry"
            )

        producer_continuity_ok, producer_continuity_status, producer_continuity_detail = (
            evaluate_producer_continuity(sent_before, outage_snapshots)
        )
        LOG.info(
            f"PRODUCER CONTINUITY DURING OUTAGE [{producer_continuity_status}]: "
            f"{producer_continuity_detail}"
        )

        LOG.section("PHASE 5 — RESTORE MONGODB")
        restore_command_time = time.time()
        LOG.info(f"MongoDB start command issued at epoch={restore_command_time:.3f}")
        compose("start", MONGO_SERVICE, timeout=60)
        mongo_ready = wait_until(
            "MongoDB ping succeeds after restoration",
            mongo_ping,
            MONGO_RESTORE_TIMEOUT_SECONDS,
        )
        LOG.info(f"MongoDB ping ready: {mongo_ready}")
        if mongo_ready:
            mongo_stopped_by_test = False
        container_snapshot("MongoDB restored; awaiting Consumer recovery")
        capture_consumer_logs("after MongoDB restoration")

        processed_reference = (
            consumer_processed_during_outage
            if consumer_processed_during_outage is not None
            else processed_before
        )
        recovery_ok, recovery_values, recovery_details = wait_for_recovery(
            processed_reference
        )
        recovery_observed_time = time.time()
        LOG.info(f"Recovery observation timestamp epoch={recovery_observed_time:.3f}")
        if recovery_ok:
            LOG.info(
                f"Observed recovery duration from MongoDB start command: "
                f"{recovery_observed_time - restore_command_time:.2f}s"
            )
        else:
            LOG.error("Consumer recovery was not fully verified before timeout.")
            LOG.error(f"Consumer recovery diagnostic details: {recovery_details}")
        log_metrics("after recovery wait")
        container_snapshot("post-recovery")

        LOG.section("PHASE 6 — POST-RECOVERY PERSISTENCE CHECK")
        mongo_counts_after, mongo_counts_after_raw = get_mongo_collection_counts()
        LOG.info("MongoDB collection counts after recovery:")
        LOG.info("\n" + mongo_counts_after_raw)
        persistence_comparison = log_mongo_count_comparison(
            mongo_counts_before, mongo_counts_after
        )
        persistence_available = (
            persistence_comparison["before_available"]
            and persistence_comparison["after_available"]
        )
        capture_consumer_logs("final")
        prompt_screenshot(
            SCREENSHOTS[3][0], SCREENSHOTS[3][1], SCREENSHOTS[3][2], screenshot_dir
        )

        LOG.section("PHASE 7 — SOURCE DATA INTEGRITY VERIFICATION")
        after_hashes = source_inventory()
        before_paths = set(before_hashes.keys())
        after_paths = set(after_hashes.keys())

        missing_files = sorted(before_paths - after_paths)
        new_files = sorted(after_paths - before_paths)
        modified_files = sorted(
            rel_path
            for rel_path in (before_paths & after_paths)
            if before_hashes[rel_path]["sha256"] != after_hashes[rel_path]["sha256"]
        )

        for rel_path in sorted(before_paths):
            if rel_path in missing_files:
                LOG.error(f"{rel_path}: MISSING")
            elif rel_path in modified_files:
                LOG.error(
                    f"{rel_path}: MODIFIED "
                    f"(before={before_hashes[rel_path]['sha256']}, "
                    f"after={after_hashes[rel_path]['sha256']})"
                )
            else:
                LOG.info(f"{rel_path}: UNCHANGED")

        for rel_path in new_files:
            LOG.error(f"{rel_path}: UNEXPECTED NEW FILE (outside recovery_test)")

        LOG.info("Source data integrity summary (excluding recovery_test):")
        LOG.info(f"  Missing files ({len(missing_files)}): {missing_files if missing_files else 'None'}")
        LOG.info(f"  Modified files ({len(modified_files)}): {modified_files if modified_files else 'None'}")
        LOG.info(f"  Unexpected new files ({len(new_files)}): {new_files if new_files else 'None'}")

        if not before_hashes:
            unchanged = False
            LOG.warning("No source files were available for checksum comparison.")
        else:
            unchanged = not missing_files and not modified_files and not new_files

        final_values = log_metrics("final test state")
        sent_after = final_values.get("producer_sent_telemetry")
        processed_after = final_values.get("consumer_processed_telemetry")
        if sent_before is not None and sent_after is not None:
            LOG.info(
                f"Producer telemetry counter change over test: {sent_after - sent_before}"
            )
        if processed_before is not None and processed_after is not None:
            LOG.info(
                f"Consumer telemetry counter change over test: "
                f"{processed_after - processed_before}"
            )
        if producer_last_before is not None:
            LOG.info(
                f"Producer last-success timestamp before outage: {producer_last_before}"
            )
        if consumer_last_before is not None:
            LOG.info(
                f"Consumer last-success timestamp before outage: {consumer_last_before}"
            )

        LOG.section("FINAL RESULT AND INTERPRETATION")
        if not before_hashes:
            source_integrity_msg = "No original source files were available for checksum comparison."
        elif unchanged:
            source_integrity_msg = (
                "All original source simulation files exist with matching SHA-256 checksums, "
                "and no unexpected new files were created outside recovery_test."
            )
        else:
            source_integrity_msg = (
                f"Source data integrity check failed — "
                f"missing={len(missing_files)} ({missing_files}), "
                f"modified={len(modified_files)} ({modified_files}), "
                f"unexpected_new={len(new_files)} ({new_files})."
            )

        conditions = {
            "mongodb_failure_observed": (
                failure_observed,
                "Observable Consumer interruption or MongoDB connection drop detected."
                if failure_observed
                else "Consumer interruption / MongoDB connection failure was not observed.",
            ),
            "producer_continuity_during_outage": (
                producer_continuity_ok,
                f"[{producer_continuity_status}] {producer_continuity_detail}",
            ),
            "mongodb_restored": (
                mongo_ready,
                "MongoDB restarted and responded to ping."
                if mongo_ready
                else "MongoDB did not respond to ping after restart.",
            ),
            "consumer_recovered_and_resumed": (
                recovery_ok,
                "MongoDB ping OK, Consumer target UP (1), MongoDB connection gauge=1, "
                "and Consumer processed-message counter advanced."
                if recovery_ok
                else f"One or more Consumer recovery criteria failed: {recovery_details}",
            ),
            "mongodb_persistence_comparison": (
                persistence_available,
                f"Before and after MongoDB collection counts were captured "
                f"(total_before={persistence_comparison['total_before']}, "
                f"total_after={persistence_comparison['total_after']}, "
                f"total_diff={persistence_comparison['total_delta']})."
                if persistence_available
                else "Before and/or after MongoDB collection counts could not be captured.",
            ),
            "source_files_unchanged": (
                unchanged,
                source_integrity_msg,
            ),
        }

        LOG.info("Verification criteria summary:")
        failed_conditions: list[str] = []
        for cond_name, (cond_ok, cond_msg) in conditions.items():
            status_label = "PASS" if cond_ok else "NOT VERIFIED / FAIL"
            LOG.info(f"  - {cond_name}: {status_label} — {cond_msg}")
            if not cond_ok:
                failed_conditions.append(cond_name)

        LOG.info(
            "Persistence evaluation summary: "
            f"before_available={persistence_comparison['before_available']}, "
            f"after_available={persistence_comparison['after_available']}, "
            f"total_before={persistence_comparison['total_before']}, "
            f"total_after={persistence_comparison['total_after']}, "
            f"total_diff={persistence_comparison['total_delta']}, "
            f"consumer_processed_advanced={recovery_details['processed_advanced_ok']}"
        )

        if all(ok for ok, _ in conditions.values()):
            result = "PASS"
            LOG.info(
                "PASS: All essential criteria were verified — MongoDB failure was observed, "
                "Producer continuity during the outage was verified, MongoDB was restored, "
                "Consumer recovered and resumed processing, before/after MongoDB persistence "
                "counts were captured, and source simulation files remained unchanged."
            )
            exit_code = 0
        elif mongo_ready and recovery_ok and unchanged:
            result = "PARTIAL"
            LOG.warning(
                "PARTIAL: MongoDB restoration, Consumer recovery, and source-file integrity "
                "were verified, but the following essential condition(s) were not verified: "
                + ", ".join(failed_conditions)
            )
            exit_code = 2
        else:
            result = "FAIL"
            LOG.error(
                "FAIL: One or more core recovery or integrity conditions failed: "
                + ", ".join(failed_conditions)
            )
            exit_code = 2

        LOG.info("Interpretation limits:")
        LOG.info("- Grafana visualizes Prometheus metrics; it does not restart or recover services.")
        LOG.info("- Docker restart policy is responsible for restarting the Consumer container.")
        LOG.info("- Prometheus scrapes metrics; this controller performs orchestration and checks.")
        LOG.info(
            "- Producer sent minus Consumer processed is only an estimated difference, "
            "not a measured Kafka consumer lag."
        )
        LOG.info("- A MongoDB ping alone is not treated as proof of application recovery.")
        LOG.info(
            "- Recovery is accepted only when MongoDB ping succeeds, the Consumer target "
            "is UP, its MongoDB connection gauge is 1, and the processed counter advances."
        )
        LOG.info(
            "- MongoDB collection counts are evaluated together with the Consumer processed "
            "counter because idempotent upserts may update existing documents without "
            "increasing every collection's document count."
        )
        LOG.info(f"Total test wall time: {time.time() - test_start:.2f}s")
        LOG.info(f"RESULT: {result}")
        LOG.info(f"Log file: {log_path}")
        LOG.info(f"Screenshot directory: {screenshot_dir}")
        print("\n" + "=" * 76)
        print(f"Test finished: {result}")
        if failed_conditions:
            print(f"Unverified/failed conditions: {', '.join(failed_conditions)}")
        print(f"Full report: {log_path}")
        print(f"Screenshots directory: {screenshot_dir}")
        print("=" * 76)

    except KeyboardInterrupt:
        LOG.warning(
            "Test interrupted by user (Ctrl+C). Partial results are retained in this log."
        )
        print(f"\nTest interrupted. Partial report remains at:\n{log_path}")
        exit_code = 130
    except Exception as exc:
        LOG.error(f"TEST ABORTED: {exc}")
        LOG.error("Partial log and any screenshots already captured have been retained.")
        print(f"\nAn error occurred. Detailed report:\n{log_path}")
        exit_code = 2
    finally:
        if mongo_stopped_by_test:
            restored = ensure_mongo_restored("abnormal exit or incomplete restoration")
            if not restored and exit_code == 0:
                exit_code = 2

    return exit_code


if __name__ == "__main__":
    sys.exit(main())