import json, urllib.request, subprocess

def get(url):
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())

jobs = get("http://localhost:8081/jobs/overview")["jobs"]
running = [j for j in jobs if j["state"]=="RUNNING"]

print("=== FLINK JOBS ===")
for j in running:
    jid = j["jid"]
    d = get("http://localhost:8081/jobs/" + jid)
    dur_min = d["duration"] // 1000 // 60
    print("\n[" + d["name"] + "] up=" + str(dur_min) + "min")
    for v in d.get("vertices", []):
        m = v.get("metrics", {})
        print("  " + v["name"][:65])
        print("    in=" + str(m.get("read-records","?")) + " out=" + str(m.get("write-records","?")))

print("\n=== KAFKA OFFSETS ===")
for topic in ["urban-safety-alerts", "hot-violence-alerts-valid"]:
    r = subprocess.run(
        ["docker","exec","kafka","bash","-c",
         "/opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell "
         "--bootstrap-server localhost:9092 --topic " + topic + " --time -1"],
        capture_output=True, text=True)
    lines = [x for x in r.stdout.strip().split("\n") if ":" in x]
    total = sum(int(x.split(":")[-1]) for x in lines)
    print(topic + ": total_offset=" + str(total))

print("\n=== PAIMON DATA ===")
r = subprocess.run(
    ["docker","exec","trino-coordinator","trino",
     "--server","localhost:8080","--catalog","paimon","--schema","security",
     "--execute","SELECT COUNT(*) FROM violence_incidents"],
    capture_output=True, text=True)
print("violence_incidents: " + r.stdout.strip().replace('"','').strip())

print("\n=== FLUSS HOT (layer-counts via chatbot) ===")
r = subprocess.run(["docker","exec","chatbot","curl","-s","http://localhost:5000/api/layer-counts"],
    capture_output=True, text=True)
print(r.stdout[:200])
