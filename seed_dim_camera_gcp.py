import requests, time

BASE = "http://localhost:8083/v1"
SID = "83422188-30e7-415f-bea8-48c2f811460f"

def gw(sid, sql, timeout=90):
    op = requests.post(f"{BASE}/sessions/{sid}/statements", json={"statement": sql}).json()["operationHandle"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = requests.get(f"{BASE}/sessions/{sid}/operations/{op}/status").json().get("status")
        if st in ("FINISHED", "ERROR"):
            r = requests.get(f"{BASE}/sessions/{sid}/operations/{op}/result/0").json()
            return st, r.get("errors", [])
        time.sleep(2)
    return "TIMEOUT", []

cameras = [
    ("cam_01", "Duong Nguyen Hue",        "Phuong Ben Nghe",          "Quan 1", 10.77845, 106.70014),
    ("cam_02", "Duong Le Loi",            "Phuong Nguyen Thai Binh",  "Quan 1", 10.77322, 106.69453),
    ("cam_03", "Duong Nguyen Thai Hoc",   "Phuong Ben Thanh",         "Quan 1", 10.77407, 106.70229),
    ("cam_04", "Duong Le Thanh Ton",      "Phuong Cau Ong Lanh",      "Quan 1", 10.77613, 106.69705),
    ("cam_05", "Duong Pasteur",           "Phuong Pham Ngu Lao",      "Quan 1", 10.77157, 106.70435),
    ("cam_06", "Duong Tran Hung Dao",     "Phuong Tan Dinh",          "Quan 1", 10.77336, 106.70019),
    ("cam_07", "Duong Dong Khoi",         "Phuong Da Kao",            "Quan 1", 10.77833, 106.69332),
    ("cam_08", "Duong Hai Ba Trung",      "Phuong Ben Thanh",         "Quan 1", 10.78446, 106.70214),
    ("cam_09", "Duong Nguyen Du",         "Phuong Nguyen Cu Trinh",   "Quan 1", 10.77002, 106.70027),
    ("cam_10", "Duong Vo Van Kiet",       "Phuong Cau Kho",           "Quan 1", 10.78266, 106.70826),
    ("cam_11", "Duong Nguyen Cong Tru",   "Phuong Tan Dinh",          "Quan 1", 10.77552, 106.70748),
    ("cam_12", "Duong CT Me Linh",        "Phuong Nguyen Thai Binh",  "Quan 1", 10.77956, 106.70549),
    ("cam_13", "Duong Ham Nghi",          "Phuong Pham Ngu Lao",      "Quan 1", 10.78320, 106.69630),
    ("cam_14", "Duong Nguyen Binh Khiem", "Phuong Ben Nghe",          "Quan 1", 10.78074, 106.70235),
    ("cam_15", "Duong Truong Dinh",       "Phuong Da Kao",            "Quan 1", 10.77709, 106.69288),
]

rows_sql = ",\n    ".join(
    "('{cid}', '{loc}', '{ward}', '{dist}', {lat}, {lon}, 'ACTIVE', TIMESTAMP '2025-01-01 00:00:00')".format(
        cid=cid, loc=loc, ward=ward, dist=dist, lat=lat, lon=lon)
    for cid, loc, ward, dist, lat, lon in cameras
)

sql = "INSERT INTO dim_camera VALUES\n    " + rows_sql
print("Inserting 15 cameras...")
st, err = gw(SID, sql, timeout=90)
print("Status:", st)
if err:
    print("Error:", str(err)[:300])
else:
    print("OK - verifying count...")
    time.sleep(5)
    # verify via SHOW TABLES and simple select
    st2, rows2 = (lambda op: (
        requests.get(f"{BASE}/sessions/{SID}/operations/{op}/result/0").json()
    ))(requests.post(f"{BASE}/sessions/{SID}/statements",
        json={"statement": "SELECT camera_id, location FROM dim_camera LIMIT 5"}).json()["operationHandle"])
    time.sleep(8)
