import os
import json

# ============================================================
# 1. Create Grafana provisioning directories
# ============================================================

os.makedirs("grafana/provisioning/datasources", exist_ok=True)
os.makedirs("grafana/provisioning/dashboards", exist_ok=True)
os.makedirs("grafana/provisioning/alerting", exist_ok=True)
os.makedirs("grafana/dashboards", exist_ok=True)


# ============================================================
# 2. Configure Prometheus datasource
# ============================================================

datasource_config = """apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    url: http://prometheus:9090
    access: proxy
    isDefault: true
"""

with open(
    "grafana/provisioning/datasources/prometheus.yml",
    "w",
    encoding="utf-8"
) as f:
    f.write(datasource_config)


# ============================================================
# 3. Configure dashboard provisioning
# ============================================================

dashboard_provider = """apiVersion: 1
providers:
  - name: 'DefaultDashboards'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /var/lib/grafana/dashboards
"""

with open(
    "grafana/provisioning/dashboards/default.yml",
    "w",
    encoding="utf-8"
) as f:
    f.write(dashboard_provider)


# ============================================================
# 4. Dashboard configuration
# ============================================================

dash_data = {
    "title": "Welding Digital Twin - System Health Monitoring",
    "timezone": "browser",
    "refresh": "5s",
    "schemaVersion": 39,
    "panels": [

        # ----------------------------------------------------
        # PANEL 1: Producer vs Consumer Throughput
        # ----------------------------------------------------

        {
            "type": "timeseries",
            "title": "1. Producer vs Consumer Throughput (Messages/sec)",
            "description": (
                "Compares the message sending rate of the Producer "
                "with the successfully processed message rate of the Consumer."
            ),
            "gridPos": {
                "h": 8, "w": 12, "x": 0, "y": 0
            },
            "targets": [
                {
                    "expr": "rate(welding_producer_messages_sent_total[1m])",
                    "legendFormat": "Producer Sent - {{topic}}",
                    "refId": "A"
                },
                {
                    "expr": "rate(welding_consumer_messages_processed_total[1m])",
                    "legendFormat": "Consumer Processed - {{topic}}",
                    "refId": "B"
                }
            ]
        },

        # ----------------------------------------------------
        # PANEL 2: Simulation Processing Progress
        # ----------------------------------------------------

        {
            "type": "timeseries",
            "title": "2. Simulation Processing Progress",
            "description": (
                "Compares the latest simulation time produced "
                "with the latest simulation time persisted by the Consumer."
            ),
            "gridPos": {
                "h": 8, "w": 12, "x": 12, "y": 0
            },
            "targets": [
                {
                    "expr": "welding_producer_current_sim_time_seconds",
                    "legendFormat": "Producer Sim Time",
                    "refId": "A"
                },
                {
                    "expr": "welding_consumer_last_sim_time_seconds",
                    "legendFormat": "Consumer - {{topic}}",
                    "refId": "B"
                }
            ]
        },

        # ----------------------------------------------------
        # PANEL 3: Estimated Unprocessed Messages
        # ----------------------------------------------------

        {
            "type": "stat",
            "title": "3. Estimated Unprocessed Messages",
            "description": (
                "Estimated difference between total messages sent "
                "and total messages successfully processed. "
                "This is not the actual Kafka consumer lag."
            ),
            "gridPos": {
                "h": 8, "w": 8, "x": 0, "y": 8
            },
            "targets": [
                {
                    "expr": (
                        "sum(welding_producer_messages_sent_total) "
                        "- sum(welding_consumer_messages_processed_total)"
                    ),
                    "legendFormat": "Estimated Difference",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "thresholds"
                    },
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "orange", "value": 100},
                            {"color": "red", "value": 500}
                        ]
                    }
                }
            }
        },

        # ----------------------------------------------------
        # PANEL 4: MongoDB Write Latency
        # ----------------------------------------------------

        {
            "type": "gauge",
            "title": "4. MongoDB Average Write Latency (Seconds)",
            "description": (
                "Weighted average duration of MongoDB write operations "
                "over the last minute."
            ),
            "gridPos": {
                "h": 8, "w": 8, "x": 8, "y": 8
            },
            "targets": [
                {
                    "expr": (
                        "sum(rate(welding_consumer_db_write_duration_seconds_sum[1m])) "
                        "/ "
                        "sum(rate(welding_consumer_db_write_duration_seconds_count[1m]))"
                    ),
                    "legendFormat": "Average Write Latency",
                    "refId": "A"
                }
            ]
        },

        # ----------------------------------------------------
        # PANEL 5: Prometheus Scrape Availability
        # ----------------------------------------------------

        {
            "type": "stat",
            "title": "5. Prometheus Scrape Availability",
            "description": (
                "Shows whether Prometheus can successfully scrape "
                "the Producer and Consumer metrics endpoints. "
                "This does not represent complete internal service health."
            ),
            "gridPos": {
                "h": 8, "w": 8, "x": 16, "y": 8
            },
            "targets": [
                {
                    "expr": 'up{job=~"welding_producer|welding_consumer"}',
                    "legendFormat": "{{job}}",
                    "refId": "A"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "thresholds"
                    },
                    "mappings": [
                        {
                            "options": {
                                "0": {"text": "DOWN"},
                                "1": {"text": "UP"}
                            },
                            "type": "value"
                        }
                    ],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "green", "value": 1}
                        ]
                    }
                }
            }
        },

        # ----------------------------------------------------
        # PANEL 6: Python Process Memory
        # ----------------------------------------------------

        {
            "type": "timeseries",
            "title": "6. Python Process Memory Usage (Bytes)",
            "description": (
                "Resident memory used by the instrumented Python processes. "
                "This represents Python process memory, not total container memory."
            ),
            "gridPos": {
                "h": 8, "w": 24, "x": 0, "y": 16
            },
            "targets": [
                {
                    "expr": "process_resident_memory_bytes",
                    "legendFormat": "{{job}} Memory",
                    "refId": "A"
                }
            ]
        },

        # ----------------------------------------------------
        # PANEL 7: Producer and Consumer Processing Errors
        # ----------------------------------------------------

        {
            "type": "timeseries",
            "title": "7. Producer and Consumer Errors",
            "description": (
                "Displays the error rate for Producer and Consumer, "
                "grouped by service and error type."
            ),
            "gridPos": {
                "h": 8, "w": 12, "x": 0, "y": 24
            },
            "targets": [
                {
                    "expr": (
                        'sum by (job, error_type) '
                        '(rate({__name__=~"welding_(consumer_processing_errors|producer_send_errors)_total"}[5m]))'
                    ),
                    "legendFormat": "{{job}} - {{error_type}}",
                    "refId": "A"
                }
            ]
        },

        # ----------------------------------------------------
        # PANEL 8: Internal Connection Health
        # ----------------------------------------------------

        {
            "type": "stat",
            "title": "8. Internal Connection Health",
            "description": (
                "Shows internal connectivity metrics for Producer and Consumer. "
                "1 = connected, 0 = disconnected. "
                "Producer status is updated by Kafka publish operations; "
                "Consumer status is updated by its connection checks."
            ),
            "gridPos": {
                "h": 8, "w": 12, "x": 12, "y": 24
            },
            "targets": [
                {
                    "expr": "welding_producer_kafka_connected",
                    "legendFormat": "Producer - Kafka",
                    "refId": "A"
                },
                {
                    "expr": "welding_consumer_kafka_connected",
                    "legendFormat": "Consumer - Kafka",
                    "refId": "B"
                },
                {
                    "expr": "welding_consumer_mongodb_connected",
                    "legendFormat": "Consumer - MongoDB",
                    "refId": "C"
                }
            ],
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "thresholds"
                    },
                    "mappings": [
                        {
                            "options": {
                                "0": {"text": "DISCONNECTED"},
                                "1": {"text": "CONNECTED"}
                            },
                            "type": "value"
                        }
                    ],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "green", "value": 1}
                        ]
                    }
                }
            }
        },

        # ----------------------------------------------------
        # PANEL 9: Time Since Last Successful Consumer Processing
        # ----------------------------------------------------

        {
            "type": "timeseries",
            "title": "9. Time Since Last Successful Consumer Processing (Seconds)",
            "description": (
                "Shows elapsed wall-clock time since the last message "
                "was successfully persisted in MongoDB and committed in Kafka."
            ),
            "gridPos": {
                "h": 8, "w": 12, "x": 0, "y": 32
            },
            "targets": [
                {
                    "expr": (
                        "time() - "
                        "welding_consumer_last_successful_processing_timestamp_seconds"
                    ),
                    "legendFormat": "{{topic}}",
                    "refId": "A"
                }
            ]
        },

        # ----------------------------------------------------
        # PANEL 10: Last Committed Kafka Offset
        # ----------------------------------------------------

        {
            "type": "timeseries",
            "title": "10. Last Committed Kafka Offset",
            "description": (
                "Displays the latest committed Kafka offset "
                "for each consumed topic."
            ),
            "gridPos": {
                "h": 8, "w": 12, "x": 12, "y": 32
            },
            "targets": [
                {
                    "expr": "welding_consumer_last_committed_offset",
                    "legendFormat": "{{topic}}",
                    "refId": "A"
                }
            ]
        },

        # ----------------------------------------------------
        # PANEL 11: Time Since Last Successful Producer Send
        # ----------------------------------------------------

        {
            "type": "timeseries",
            "title": "11. Time Since Last Successful Producer Send (Seconds)",
            "description": (
                "Shows elapsed wall-clock time since the Producer "
                "last received a successful Kafka broker acknowledgement."
            ),
            "gridPos": {
                "h": 8, "w": 24, "x": 0, "y": 40
            },
            "targets": [
                {
                    "expr": (
                        "time() - "
                        "welding_producer_last_successful_send_timestamp_seconds"
                    ),
                    "legendFormat": "Producer",
                    "refId": "A"
                }
            ]
        }
    ]
}


# ============================================================
# 5. Write dashboard JSON file
# ============================================================

with open(
    "grafana/dashboards/welding_dashboard.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(dash_data, f, indent=2)


# ============================================================
# 6. Configure Grafana Alert Rules
# ============================================================

alert_rules = """apiVersion: 1

groups:
  - orgId: 1
    name: Welding Digital Twin Health
    folder: Welding Digital Twin
    interval: 1m

    rules:

      # 1. Producer unavailable
      - uid: producer-unavailable
        title: Producer Unavailable
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus
            model:
              datasource:
                type: prometheus
                uid: prometheus
              editorMode: code
              expr: '(up{job="welding_producer"} == 0) or absent(up{job="welding_producer"})'
              instant: true
              refId: A
              intervalMs: 1000
              maxDataPoints: 43200
        noDataState: NoData
        execErrState: Error
        for: 1m
        annotations:
          summary: Producer is unavailable
          description: Prometheus cannot scrape the Producer metrics endpoint.
        labels:
          severity: critical
        isPaused: false

      # 2. Consumer unavailable
      - uid: consumer-unavailable
        title: Consumer Unavailable
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus
            model:
              datasource:
                type: prometheus
                uid: prometheus
              editorMode: code
              expr: '(up{job="welding_consumer"} == 0) or absent(up{job="welding_consumer"})'
              instant: true
              refId: A
              intervalMs: 1000
              maxDataPoints: 43200
        noDataState: NoData
        execErrState: Error
        for: 1m
        annotations:
          summary: Consumer is unavailable
          description: Prometheus cannot scrape the Consumer metrics endpoint.
        labels:
          severity: critical
        isPaused: false

      # 3. Producer Kafka connection
      - uid: producer-kafka-disconnected
        title: Producer Kafka Disconnected
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus
            model:
              datasource:
                type: prometheus
                uid: prometheus
              editorMode: code
              expr: 'welding_producer_kafka_connected == 0'
              instant: true
              refId: A
              intervalMs: 1000
              maxDataPoints: 43200
        noDataState: NoData
        execErrState: Error
        for: 1m
        annotations:
          summary: Producer Kafka connection lost
          description: The Producer reports that its Kafka connection is unavailable.
        labels:
          severity: critical
        isPaused: false

      # 4. Consumer Kafka connection
      - uid: consumer-kafka-disconnected
        title: Consumer Kafka Disconnected
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus
            model:
              datasource:
                type: prometheus
                uid: prometheus
              editorMode: code
              expr: 'welding_consumer_kafka_connected == 0'
              instant: true
              refId: A
              intervalMs: 1000
              maxDataPoints: 43200
        noDataState: NoData
        execErrState: Error
        for: 1m
        annotations:
          summary: Consumer Kafka connection lost
          description: The Consumer reports that its Kafka connection is unavailable.
        labels:
          severity: critical
        isPaused: false

      # 5. MongoDB connection
      - uid: mongodb-disconnected
        title: MongoDB Disconnected
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus
            model:
              datasource:
                type: prometheus
                uid: prometheus
              editorMode: code
              expr: 'welding_consumer_mongodb_connected == 0'
              instant: true
              refId: A
              intervalMs: 1000
              maxDataPoints: 43200
        noDataState: NoData
        execErrState: Error
        for: 1m
        annotations:
          summary: MongoDB connection lost
          description: The Consumer cannot connect to MongoDB.
        labels:
          severity: critical
        isPaused: false

      # 6. Producer send errors
      - uid: producer-send-errors
        title: Producer Send Errors
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus
            model:
              datasource:
                type: prometheus
                uid: prometheus
              editorMode: code
              expr: 'sum(rate(welding_producer_send_errors_total[5m])) > 0'
              instant: true
              refId: A
              intervalMs: 1000
              maxDataPoints: 43200
        noDataState: NoData
        execErrState: Error
        for: 2m
        annotations:
          summary: Producer is experiencing send errors
          description: Producer send errors have been detected during the last five minutes.
        labels:
          severity: warning
        isPaused: false

      # 7. Consumer processing errors
      - uid: consumer-processing-errors
        title: Consumer Processing Errors
        condition: A
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: prometheus
            model:
              datasource:
                type: prometheus
                uid: prometheus
              editorMode: code
              expr: 'sum(rate(welding_consumer_processing_errors_total[5m])) > 0'
              instant: true
              refId: A
              intervalMs: 1000
              maxDataPoints: 43200
        noDataState: NoData
        execErrState: Error
        for: 2m
        annotations:
          summary: Consumer is experiencing processing errors
          description: Consumer processing errors have been detected during the last five minutes.
        labels:
          severity: warning
        isPaused: false
"""

with open(
    "grafana/provisioning/alerting/welding_alerts.yml",
    "w",
    encoding="utf-8"
) as f:
    f.write(alert_rules)


# ============================================================
# 7. Completion message
# ============================================================

print("Grafana provisioning files created successfully.")
print("Dashboard: Welding Digital Twin - System Health Monitoring")
print("Total dashboard panels: 11")
print("Total alert rules: 7")