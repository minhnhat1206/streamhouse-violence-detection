"""Generate Grafana violence-incidents-v2 dashboard JSON using dedicated KPI endpoints."""
import json
import os

INFINITY_DS = {"type": "yesoreyeram-infinity-datasource", "uid": "dfnwf3hut0bnkf"}
CHATBOT = "http://chatbot:5002"

def kpi_target(field):
    """Dedicated KPI endpoint returns [{value: N}] — avoids numeric-long frame issue."""
    return {
        "refId": "A", "type": "json", "source": "url",
        "url": f"{CHATBOT}/api/grafana/kpi/{field}",
        "root_selector": "",
        "columns": [
            {"selector": "time", "text": "time", "type": "timestamp"},
            {"selector": "value", "text": "value", "type": "number"}
        ],
        "parser": "backend", "format": "timeseries"
    }

def arr_target(url, root, cols):
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

kpis = [
    (1, "Violent Incidents (24h)",  "violent_24h",    "red",    0),
    (2, "Violent Incidents (7 days)","violent_7d",    "orange", 6),
    (3, "Active Cameras (24h)",     "cameras_active", "green",  12),
    (4, "Avg Risk Score (24h)",     "avg_risk_score", "yellow", 18),
]
for pid, title, field, color, xpos in kpis:
    panels.append({
        "datasource": INFINITY_DS,
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
        "targets": [kpi_target(field)],
        "title": title, "type": "stat"
    })
y += 4

# --- Row 2: Trends ---
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 201,
               "title": "Incident Trends and Distribution", "type": "row"})
y += 1

panels.append({
    "datasource": INFINITY_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"},
                                  "custom": {"lineWidth": 2, "fillOpacity": 15}},
                    "overrides": []},
    "gridPos": {"h": 8, "w": 14, "x": 0, "y": y}, "id": 5,
    "options": {"tooltip": {"mode": "multi"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [arr_target(f"{CHATBOT}/api/stats", "alertsPerHour",
        [{"selector": "name", "text": "Date", "type": "string"},
         {"selector": "alerts", "text": "Alerts", "type": "number"}])],
    "title": "Incidents per Day - Trend", "type": "timeseries"
})

panels.append({
    "datasource": INFINITY_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 10, "x": 14, "y": y}, "id": 6,
    "options": {"pieType": "pie",
                "legend": {"displayMode": "table", "placement": "right"},
                "tooltip": {"mode": "single"}},
    "targets": [arr_target(f"{CHATBOT}/api/stats", "alertTypes",
        [{"selector": "name", "text": "Event Type", "type": "string"},
         {"selector": "value", "text": "Count", "type": "number"}])],
    "title": "Incident Type Distribution", "type": "piechart"
})
y += 8

# --- Row 3: Camera & Location ---
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 202,
               "title": "Camera and Location Hotspot", "type": "row"})
y += 1

panels.append({
    "datasource": INFINITY_DS,
    "fieldConfig": {"defaults": {"color": {"fixedColor": "red", "mode": "fixed"}},
                    "overrides": []},
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": y}, "id": 7,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [arr_target(f"{CHATBOT}/api/grafana/cameras", "",
        [{"selector": "camera_id", "text": "Camera", "type": "string"},
         {"selector": "incident_count", "text": "Incidents", "type": "number"}])],
    "title": "Top Cameras by Incident Count (7d)", "type": "barchart"
})

panels.append({
    "datasource": INFINITY_DS,
    "fieldConfig": {"defaults": {"color": {"fixedColor": "orange", "mode": "fixed"}},
                    "overrides": []},
    "gridPos": {"h": 8, "w": 12, "x": 12, "y": y}, "id": 8,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [arr_target(f"{CHATBOT}/api/stats", "topLocations",
        [{"selector": "name", "text": "Location", "type": "string"},
         {"selector": "alerts", "text": "Alerts", "type": "number"}])],
    "title": "Top Locations by Incident Count", "type": "barchart"
})
y += 8

# --- Row 4: Recent Incidents ---
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 203,
               "title": "Recent Incidents", "type": "row"})
y += 1

panels.append({
    "datasource": INFINITY_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}}, "overrides": []},
    "gridPos": {"h": 10, "w": 24, "x": 0, "y": y}, "id": 9,
    "options": {"showHeader": True},
    "targets": [arr_target(f"{CHATBOT}/api/recent-incidents?limit=50", "",
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
    "uid": "violence-incidents-v2", "version": 5
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
