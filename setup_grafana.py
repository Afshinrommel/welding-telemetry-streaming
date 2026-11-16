import os
import json

os.makedirs('grafana/provisioning/datasources', exist_ok=True)
os.makedirs('grafana/provisioning/dashboards', exist_ok=True)
os.makedirs('grafana/dashboards', exist_ok=True)

with open('grafana/provisioning/datasources/prometheus.yml', 'w', encoding='utf-8') as f:
    f.write("apiVersion: 1\ndatasources:\n  - name: Prometheus\n    type: prometheus\n    url: http://prometheus:9090\n    access: proxy\n    isDefault: true\n")

with open('grafana/provisioning/dashboards/default.yml', 'w', encoding='utf-8') as f:
    f.write("apiVersion: 1\nproviders:\n  - name: 'DefaultDashboards'\n    orgId: 1\n    folder: ''\n    type: file\n    disableDeletion: false\n    updateIntervalSeconds: 10\n    options:\n      path: /var/lib/grafana/dashboards\n")

dash_data = {
  "title": "Welding Digital Twin - Telemetry Health",
  "timezone": "browser",
  "refresh": "5s",
  "schemaVersion": 39,
  "panels": [
    {
      "type": "timeseries",
      "title": "Producer vs Consumer Throughput (Messages/sec)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        { "expr": "rate(welding_producer_messages_sent_total[1m])", "legendFormat": "Producer Sent", "refId": "A" },
        { "expr": "rate(welding_consumer_messages_processed_total[1m])", "legendFormat": "Consumer Processed", "refId": "B" }
      ]
    },
    {
      "type": "timeseries",
      "title": "Simulation Time Sync (Lag Monitor)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "targets": [
        { "expr": "welding_producer_current_sim_time_seconds", "legendFormat": "Producer Sim Time", "refId": "A" },
        { "expr": "welding_consumer_last_sim_time_seconds", "legendFormat": "Consumer Sim Time", "refId": "B" }
      ]
    },
    {
      "type": "gauge",
      "title": "MongoDB Average Write Latency (Seconds)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [
        { "expr": "rate(welding_consumer_db_write_duration_seconds_sum[1m]) / rate(welding_consumer_db_write_duration_seconds_count[1m])", "refId": "A" }
      ]
    }
  ]
}
with open('grafana/dashboards/welding_dashboard.json', 'w', encoding='utf-8') as f:
    json.dump(dash_data, f, indent=2)

print("✅ All Grafana provisioning files created successfully!")