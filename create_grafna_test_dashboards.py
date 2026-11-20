import json
from pathlib import Path

# ============================================================
# 1. Project paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_DIR = BASE_DIR / "grafana" / "dashboards"

DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

DATASOURCE = {
    "type": "prometheus",
    "uid": "prometheus"
}


# ============================================================
# 2. Helper functions
# ============================================================

def make_target(expr, ref_id, legend=None):
    target = {
        "datasource": DATASOURCE,
        "editorMode": "code",
        "expr": expr,
        "instant": False,
        "refId": ref_id
    }

    if legend:
        target["legendFormat"] = legend

    return target


def make_panel(
    panel_id,
    title,
    description,
    panel_type,
    position,
    targets,
    field_config=None
):
    panel = {
        "id": panel_id,
        "title": title,
        "description": description,
        "type": panel_type,
        "gridPos": position,
        "datasource": DATASOURCE,
        "targets": targets
    }

    if field_config:
        panel["fieldConfig"] = field_config

    if panel_type == "timeseries":
        panel["options"] = {
            "legend": {
                "displayMode": "list",
                "placement": "bottom"
            },
            "tooltip": {
                "mode": "multi"
            }
        }

    elif panel_type == "stat":
        panel["options"] = {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto"
        }

    elif panel_type == "gauge":
        panel["options"] = {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False
            },
            "showThresholdLabels": False,
            "showThresholdMarkers": True
        }

    return panel


# ============================================================
# 3. Shared field configurations
# ============================================================

connection_status_config = {
    "defaults": {
        "mappings": [
            {
                "type": "value",
                "options": {
                    "0": {"text": "DISCONNECTED"},
                    "1": {"text": "CONNECTED"}
                }
            }
        ],
        "thresholds": {
            "mode": "absolute",
            "steps": [
                {"color": "red", "value": None},
                {"color": "green", "value": 1}
            ]
        },
        "color": {"mode": "thresholds"}
    },
    "overrides": []
}

availability_config = {
    "defaults": {
        "mappings": [
            {
                "type": "value",
                "options": {
                    "0": {"text": "DOWN"},
                    "1": {"text": "UP"}
                }
            }
        ],
        "thresholds": {
            "mode": "absolute",
            "steps": [
                {"color": "red", "value": None},
                {"color": "green", "value": 1}
            ]
        },
        "color": {"mode": "thresholds"}
    },
    "overrides": []
}


# ============================================================
# 4. Dashboard 1: Pipeline Performance
# ============================================================

pipeline_panels = [

    make_panel(
        1,
        "Producer vs Consumer Throughput (Messages/sec)",
        "Compares the message sending rate with the successfully processed message rate.",
        "timeseries",
        {"h": 9, "w": 12, "x": 0, "y": 0},
        [
            make_target(
                "rate(welding_producer_messages_sent_total[1m])",
                "A",
                "Producer Sent - {{topic}}"
            ),
            make_target(
                "rate(welding_consumer_messages_processed_total[1m])",
                "B",
                "Consumer Processed - {{topic}}"
            )
        ]
    ),

    make_panel(
        2,
        "Simulation Processing Progress",
        "Compares the latest simulation time produced with the latest simulation time persisted by the Consumer.",
        "timeseries",
        {"h": 9, "w": 12, "x": 12, "y": 0},
        [
            make_target(
                "welding_producer_current_sim_time_seconds",
                "A",
                "Producer Sim Time"
            ),
            make_target(
                "welding_consumer_last_sim_time_seconds",
                "B",
                "Consumer - {{topic}}"
            )
        ]
    ),

    make_panel(
        3,
        "Estimated Unprocessed Messages",
        "Estimated difference between total messages sent and successfully processed. This is not actual Kafka consumer lag.",
        "stat",
        {"h": 9, "w": 12, "x": 0, "y": 9},
        [
            make_target(
                "sum(welding_producer_messages_sent_total) - sum(welding_consumer_messages_processed_total)",
                "A",
                "Estimated Difference"
            )
        ],
        {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "orange", "value": 100},
                        {"color": "red", "value": 500}
                    ]
                }
            },
            "overrides": []
        }
    ),

    make_panel(
        4,
        "Python Process Memory Usage (Bytes)",
        "Resident memory used by the instrumented Python processes. This is not total container memory.",
        "timeseries",
        {"h": 9, "w": 24, "x": 0, "y": 18},
        [
            make_target(
                "process_resident_memory_bytes",
                "A",
                "{{job}} Memory"
            )
        ]
    )
]


# ============================================================
# 5. Dashboard 2: Service Health & Errors
# ============================================================

service_panels = [

    make_panel(
        1,
        "Prometheus Scrape Availability",
        "Shows whether Prometheus can scrape the Producer and Consumer metrics endpoints. It does not represent complete internal service health.",
        "stat",
        {"h": 9, "w": 12, "x": 0, "y": 0},
        [
            make_target(
                'up{job=~"welding_producer|welding_consumer"}',
                "A",
                "{{job}}"
            )
        ],
        availability_config
    ),

    make_panel(
        2,
        "Producer and Consumer Errors",
        "Displays the error rate grouped by service and error type.",
        "timeseries",
        {"h": 9, "w": 12, "x": 12, "y": 0},
        [
            make_target(
                'sum by (job, error_type) (rate({__name__=~"welding_(consumer_processing_errors|producer_send_errors)_total"}[5m]))',
                "A",
                "{{job}} - {{error_type}}"
            )
        ]
    ),

    make_panel(
        3,
        "Internal Connection Health",
        "Shows internal Kafka and MongoDB connection metrics. 1 = connected, 0 = disconnected. Metrics may disappear when a process exits.",
        "stat",
        {"h": 10, "w": 24, "x": 0, "y": 9},
        [
            make_target(
                "welding_producer_kafka_connected",
                "A",
                "Producer - Kafka"
            ),
            make_target(
                "welding_consumer_kafka_connected",
                "B",
                "Consumer - Kafka"
            ),
            make_target(
                "welding_consumer_mongodb_connected",
                "C",
                "Consumer - MongoDB"
            )
        ],
        connection_status_config
    )
]


# ============================================================
# 6. Dashboard 3: Data Persistence & Recovery
# ============================================================

recovery_panels = [

    make_panel(
        1,
        "MongoDB Average Write Latency (Seconds)",
        "Weighted average duration of MongoDB write operations over the last minute.",
        "gauge",
        {"h": 9, "w": 12, "x": 0, "y": 0},
        [
            make_target(
                "sum(rate(welding_consumer_db_write_duration_seconds_sum[1m])) / sum(rate(welding_consumer_db_write_duration_seconds_count[1m]))",
                "A",
                "Average Write Latency"
            )
        ]
    ),

    make_panel(
        2,
        "Time Since Last Successful Consumer Processing (Seconds)",
        "Elapsed wall-clock time since a message was successfully persisted in MongoDB and committed in Kafka.",
        "timeseries",
        {"h": 9, "w": 12, "x": 12, "y": 0},
        [
            make_target(
                "time() - welding_consumer_last_successful_processing_timestamp_seconds",
                "A",
                "{{topic}}"
            )
        ]
    ),

    make_panel(
        3,
        "Last Committed Kafka Offset",
        "Displays the latest committed Kafka offset reported by the Consumer for each topic.",
        "timeseries",
        {"h": 9, "w": 12, "x": 0, "y": 9},
        [
            make_target(
                "welding_consumer_last_committed_offset",
                "A",
                "{{topic}}"
            )
        ]
    ),

    make_panel(
        4,
        "Time Since Last Successful Producer Send (Seconds)",
        "Elapsed wall-clock time since the Producer last received a successful Kafka broker acknowledgement.",
        "timeseries",
        {"h": 9, "w": 12, "x": 12, "y": 9},
        [
            make_target(
                "time() - welding_producer_last_successful_send_timestamp_seconds",
                "A",
                "Producer"
            )
        ]
    )
]


# ============================================================
# 7. Dashboard definitions
# ============================================================

dashboards = [
    {
        "filename": "pipeline_performance.json",
        "uid": "welding-pipeline-performance",
        "title": "Welding Digital Twin - Pipeline Performance",
        "panels": pipeline_panels
    },
    {
        "filename": "service_health_errors.json",
        "uid": "welding-service-health",
        "title": "Welding Digital Twin - Service Health & Errors",
        "panels": service_panels
    },
    {
        "filename": "data_persistence_recovery.json",
        "uid": "welding-data-recovery",
        "title": "Welding Digital Twin - Data Persistence & Recovery",
        "panels": recovery_panels
    }
]


# ============================================================
# 8. Write only the three new dashboard JSON files
# ============================================================

print("\nCreating Grafana test dashboards...\n")

for dashboard in dashboards:

    output = {
        "id": None,
        "uid": dashboard["uid"],
        "title": dashboard["title"],
        "tags": ["Welding Digital Twin", "Operational Test"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "5s",
        "panels": dashboard["panels"]
    }

    output_path = DASHBOARD_DIR / dashboard["filename"]

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(output, file, indent=2)

    print(f"[CREATED] {output_path.name}")
    print(f"          Panels: {len(dashboard['panels'])}")


# ============================================================
# 9. Completion summary
# ============================================================

print("\n" + "=" * 55)
print("Grafana test dashboards created successfully.")
print("=" * 55)

print("\nNew dashboards:")
print("1. Pipeline Performance              - 4 panels")
print("2. Service Health & Errors           - 3 panels")
print("3. Data Persistence & Recovery       - 4 panels")

print("\nTotal new panels: 11")
print("\nExisting dashboard and alert provisioning files")
print("were not modified by this script.")

print("\nNext step: Allow Grafana to detect the new JSON files.")
print("Open http://localhost:3000 and check Dashboards.")