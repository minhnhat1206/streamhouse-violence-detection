"""Generate violence-security-monitor and violence_analytics dashboards using Prometheus."""
import json, os

PROM_DS = {"type": "prometheus", "uid": "PBFA97CFB590B2093"}
script_dir = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(script_dir)

def prom_stat(expr):
    return {"refId": "A", "datasource": PROM_DS, "expr": expr, "instant": True}

def prom_ts(expr, legend=""):
    return {"refId": "A", "datasource": PROM_DS, "expr": expr,
            "legendFormat": legend, "instant": False, "range": True}

def stat_panel(pid, title, metric, color, xpos, y, w=6, h=4, unit="short"):
    return {
        "datasource": PROM_DS,
        "fieldConfig": {
            "defaults": {"color": {"fixedColor": color, "mode": "fixed"},
                         "mappings": [], "unit": unit},
            "overrides": []
        },
        "gridPos": {"h": h, "w": w, "x": xpos, "y": y},
        "id": pid,
        "options": {
            "colorMode": "background", "graphMode": "none",
            "justifyMode": "auto", "orientation": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "textMode": "auto"
        },
        "targets": [prom_stat(metric)],
        "title": title, "type": "stat"
    }


# ═══════════════════════════════════════════════════════
# Dashboard 1: violence-security-monitor
# ═══════════════════════════════════════════════════════
panels = []
y = 0

# Row: KPI
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 100,
               "title": "KPI — Violence Statistics", "type": "row"})
y += 1

panels += [
    stat_panel(1, "SU KIEN BAO LUC (24H)",  "violence_incidents_24h_total", "red",    0, y),
    stat_panel(2, "SU KIEN BAO LUC (7 NGAY)","violence_incidents_7d_total", "orange", 6, y),
    stat_panel(3, "CAMERA HOAT DONG",        "violence_cameras_active",     "green",  12, y),
    stat_panel(4, "DIEM RUI RO TB",          "violence_avg_risk_score",     "yellow", 18, y),
]
y += 4

# Row: Trends
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 101,
               "title": "Phan tich theo loai & camera", "type": "row"})
y += 1

panels.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": y}, "id": 5,
    "options": {"pieType": "pie",
                "legend": {"displayMode": "table", "placement": "right"},
                "tooltip": {"mode": "single"}},
    "targets": [prom_stat("violence_incidents_by_type{job='chatbot'}")],
    "title": "Phan phoi loai su kien", "type": "piechart"
})

panels.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 12, "x": 12, "y": y}, "id": 6,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [{"refId": "A", "datasource": PROM_DS,
                 "expr": "topk(10, violence_incidents_by_camera)", "instant": True,
                 "legendFormat": "{{camera_id}}"}],
    "title": "Top Cameras by Incident Count (7d)", "type": "barchart"
})
y += 8

# Row: Locations
panels.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 102,
               "title": "Diem nong theo dia diem", "type": "row"})
y += 1

panels.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 24, "x": 0, "y": y}, "id": 7,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [{"refId": "A", "datasource": PROM_DS,
                 "expr": "topk(10, violence_incidents_by_location)", "instant": True,
                 "legendFormat": "{{location}}"}],
    "title": "Top Locations by Incident Count", "type": "barchart"
})

security_dashboard = {
    "__inputs": [], "__requires": [{"type": "grafana", "id": "grafana", "name": "Grafana", "version": "9.0.0"}],
    "annotations": {"list": []}, "editable": True, "id": None, "links": [],
    "panels": panels, "refresh": "1m", "schemaVersion": 36,
    "tags": ["streamhouse", "security"],
    "title": "HE THONG GIAM SAT AN NINH DO THI",
    "uid": "violence-security-monitor", "version": 2
}


# ═══════════════════════════════════════════════════════
# Dashboard 2: violence_analytics
# ═══════════════════════════════════════════════════════
panels2 = []
y = 0

# Row: KPI Overview
panels2.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 200,
                "title": "KPI Overview (7 days)", "type": "row"})
y += 1

panels2 += [
    stat_panel(1, "Total Incidents (7d)",  "violence_incidents_7d_total",  "red",    0,  y),
    stat_panel(2, "Violent Incidents (7d)", "violence_incidents_7d_total",  "orange", 6,  y),
    stat_panel(3, "Avg Risk Score",         "violence_avg_risk_score",      "yellow", 12, y),
    stat_panel(4, "Active Cameras",         "violence_cameras_active",      "green",  18, y),
    # Layer counts
    stat_panel(5, "HOT Rows (Fluss)",  "streamhouse_hot_rows_total",  "red",    0,  y+4, w=8, h=3),
    stat_panel(6, "WARM Rows (Paimon)","streamhouse_warm_rows_total", "orange", 8,  y+4, w=8, h=3),
    stat_panel(7, "COLD Rows (Iceberg)","streamhouse_cold_rows_total","blue",   16, y+4, w=8, h=3),
]
y += 7

# Row: Distribution
panels2.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 201,
                "title": "Distribution Analysis", "type": "row"})
y += 1

panels2.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 8, "x": 0, "y": y}, "id": 10,
    "options": {"pieType": "donut",
                "legend": {"displayMode": "table", "placement": "right"},
                "tooltip": {"mode": "single"}},
    "targets": [prom_stat("violence_incidents_by_type")],
    "title": "Event Type Distribution", "type": "piechart"
})

panels2.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 8, "x": 8, "y": y}, "id": 11,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [{"refId": "A", "datasource": PROM_DS,
                 "expr": "topk(10, violence_incidents_by_camera)", "instant": True,
                 "legendFormat": "{{camera_id}}"}],
    "title": "Top Cameras (7d)", "type": "barchart"
})

panels2.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
    "gridPos": {"h": 8, "w": 8, "x": 16, "y": y}, "id": 12,
    "options": {"orientation": "horizontal", "barRadius": 0.05,
                "tooltip": {"mode": "single"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [{"refId": "A", "datasource": PROM_DS,
                 "expr": "topk(10, violence_incidents_by_location)", "instant": True,
                 "legendFormat": "{{location}}"}],
    "title": "Top Locations", "type": "barchart"
})
y += 8

# Row: Timeseries
panels2.append({"collapsed": False, "gridPos": {"h":1,"w":24,"x":0,"y":y}, "id": 202,
                "title": "Trend Over Time", "type": "row"})
y += 1

panels2.append({
    "datasource": PROM_DS,
    "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"},
                                  "custom": {"lineWidth": 2, "fillOpacity": 15}},
                    "overrides": []},
    "gridPos": {"h": 8, "w": 24, "x": 0, "y": y}, "id": 13,
    "options": {"tooltip": {"mode": "multi"},
                "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [
        {"refId": "A", "datasource": PROM_DS,
         "expr": "violence_incidents_24h_total", "legendFormat": "Violent Incidents (24h)"},
        {"refId": "B", "datasource": PROM_DS,
         "expr": "streamhouse_warm_rows_total", "legendFormat": "WARM Layer Total"},
    ],
    "title": "Streamhouse Metrics Trend", "type": "timeseries"
})

analytics_dashboard = {
    "__inputs": [], "__requires": [{"type": "grafana", "id": "grafana", "name": "Grafana", "version": "9.0.0"}],
    "annotations": {"list": []}, "editable": True, "id": None, "links": [],
    "panels": panels2, "refresh": "1m", "schemaVersion": 36,
    "tags": ["streamhouse", "analytics"],
    "title": "Violence Analytics Dashboard",
    "uid": "violence_analytics", "version": 2
}

# Save both dashboards to both paths
for dash, fname in [
    (security_dashboard, "violence_security_dashboard.json"),
    (analytics_dashboard, "violence_analytics_dashboard.json"),
]:
    for base in ["config", "deploy/config"]:
        path = os.path.join(root, base, "grafana", "provisioning", "dashboards", fname)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(dash, f, indent=2, ensure_ascii=False)
        print(f"Saved: {path}")
