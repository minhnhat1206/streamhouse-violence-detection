"""Generate Grafana violence-incidents-v2 dashboard JSON.
Uses Prometheus for KPI/chart panels (guaranteed to work)
and Infinity for table panel.
"""
import json
import os

PROM_DS    = {"type": "prometheus", "uid": "PBFA97CFB590B2093"}
INFINITY_DS = {"type": "yesoreyeram-infinity-datasource", "uid": "dfnwf3hut0bnkf"}
CHATBOT = "http://chatbot:5002"

def prom_stat(expr, legend=""):
    return {
        "refId": "A", "datasource": PROM_DS,
        "expr": expr, "legendFormat": legend,
        "instant": True, "range": False
    }

def prom_target(expr, legend=""):
    return {
        "refId": "A", "datasource": PROM_DS,
        "expr": expr, "legendFormat": legend,
        "instant": False, "range": True
    }

def inf_table_target(url, root, cols):
    return {
        "refId": "A", "type": "json", "source": "url", "url": url,
        "root_selector": root, "columns": cols, "parser": "backend", "format": "table"
    }

panels = []
y = 0

# --- Row 1: KPI ---
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 200,
               "title": "KPI Overview - Violence Incident Statistics", "type": "row"})
y += 1

# ── Row 1: KPI Stats (Prometheus — guaranteed to work) ─────────────────────
kpis = [
    (1, "Violent Incidents (24h)",    "violence_incidents_24h_total", "red",    0),
    (2, "Violent Incidents (7 days)", "violence_incidents_7d_total",  "orange", 6),
    (3, "Active Cameras (24h)",       "violence_cameras_active",      "green",  12),
    (4, "Avg Risk Score (24h)",       "violence_avg_risk_score",      "yellow", 18),
]
for pid, title, metric, color, xpos in kpis:
    panels.append({
        "datasource": PROM_DS,
        "fieldConfig": {
            "defaults": {"color": {"fixedColor": color, "mode": "fixed"},
                         "mappings": [], "unit": "short"},
            "overrides": []
        },
        "gridPos": {"h": 4, "w": 6, "x": xpos, "y": y},
        "id": pid,
        "options": {
            "colorMode": "background", "graphMode": "none",
            "justifyMode": "auto", "orientation": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "textMode": "auto"
        },
        "targets": [prom_stat(metric)],
        "title": title, "type": "stat"
    })
y += 4

# ── Row 2: Trends ──────────────────────────────────────────────────────────
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 201,
               "title": "Incident Trends and Distribution", "type": "row"})
y += 1

# Timeseries: incidents over time using Prometheus range query
panels.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"},
                                  "custom": {"lineWidth": 2, "fillOpacity": 15}},
                    "overrides": []},
    "gridPos": {"h": 8, "w": 14, "x": 0, "y": y}, "id": 5,
    "options": {"tooltip": {"mode": "multi"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [prom_target("violence_incidents_24h_total", "Violent Incidents (24h)")],
    "title": "Violent Incidents Trend", "type": "timeseries"
})

# Piechart: event type distribution using Prometheus labels
panels.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 10, "x": 14, "y": y}, "id": 6,
    "options": {"pieType": "pie",
                "legend": {"displayMode": "table", "placement": "right"},
                "tooltip": {"mode": "single"}},
    "targets": [prom_stat(
        'violence_incidents_by_type',
        "{{event_type}}"
    )],
    "title": "Incident Type Distribution", "type": "piechart"
})
y += 8

# ── Row 3: Camera & Location ───────────────────────────────────────────────
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 202,
               "title": "Camera and Location Hotspot", "type": "row"})
y += 1

# Barchart: top cameras
panels.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": y}, "id": 7,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [prom_stat("topk(15, violence_incidents_by_camera)", "{{camera_id}}")],
    "title": "Top Cameras by Incident Count (7d)", "type": "barchart"
})

# Barchart: top locations
panels.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 12, "x": 12, "y": y}, "id": 8,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [prom_stat("topk(10, violence_incidents_by_location)", "{{location}}")],
    "title": "Top Locations by Incident Count", "type": "barchart"
})
y += 8

# ── Row 4: Streamhouse Layer Health ────────────────────────────────────────
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 204,
               "title": "Streamhouse Layer Counts", "type": "row"})
y += 1

for pid, title, metric, color, xpos in [
    (10, "HOT Layer (Fluss)",   "streamhouse_hot_rows_total",  "red",   0),
    (11, "WARM Layer (Paimon)", "streamhouse_warm_rows_total", "orange", 8),
    (12, "COLD Layer (Iceberg)","streamhouse_cold_rows_total", "blue",  16),
]:
    panels.append({
        "datasource": PROM_DS,
        "fieldConfig": {"defaults": {"color": {"fixedColor": color, "mode": "fixed"}, "unit": "short"}, "overrides": []},
        "gridPos": {"h": 3, "w": 8, "x": xpos, "y": y},
        "id": pid,
        "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "auto",
                    "orientation": "auto",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "textMode": "auto"},
        "targets": [prom_stat(metric)],
        "title": title, "type": "stat"
    })
y += 3

# ── Row 5: Recent Incidents (Infinity table) ────────────────────────────────
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 203,
               "title": "Recent Incidents", "type": "row"})
y += 1

panels.append({
    "datasource": INFINITY_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}}, "overrides": []},
    "gridPos": {"h": 10, "w": 24, "x": 0, "y": y}, "id": 9,
    "options": {"showHeader": True},
    "targets": [inf_table_target(f"{CHATBOT}/api/recent-incidents?limit=50", "",
        [{"selector": "camera_id", "text": "Camera", "type": "string"},
         {"selector": "timestamp", "text": "Time", "type": "string"},
         {"selector": "violence_score", "text": "Risk Score", "type": "number"},
         {"selector": "label", "text": "Type", "type": "string"},
         {"selector": "location", "text": "Location", "type": "string"}])],
    "title": "Recent Violent Incidents (Latest 50)", "type": "table"
})

dashboard = {
    "__inputs": [], "__requires": [{"type": "grafana", "id": "grafana", "name": "Grafana", "version": "9.0.0"}],
    "annotations": {"list": []}, "editable": True, "id": None, "links": [],
    "panels": panels, "refresh": "30s", "schemaVersion": 36,
    "tags": ["streamhouse", "violence"],
    "title": "Violence Incidents Analytics",
    "uid": "violence-incidents-v2", "version": 6
}

script_dir = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(script_dir)

paths = [
    os.path.join(root, "config", "grafana", "provisioning", "dashboards", "violence_incidents_v2.json"),
    os.path.join(root, "deploy", "config", "grafana", "provisioning", "dashboards", "violence_incidents_v2.json"),
]
for path in paths:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)
    print(f"Saved: {path}")
