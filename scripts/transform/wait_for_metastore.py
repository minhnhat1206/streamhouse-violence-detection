"""Wait for Hive Metastore to be ready on port 9083."""
import socket
import sys
import time

HOST = "hive-metastore"
PORT = 9083
MAX_RETRIES = 15
DELAY = 5

for i in range(1, MAX_RETRIES + 1):
    try:
        s = socket.create_connection((HOST, PORT), timeout=3)
        s.close()
        print(f"[OK] Hive Metastore is READY at {HOST}:{PORT}")
        sys.exit(0)
    except (ConnectionRefusedError, OSError):
        print(f"[WAIT] Hive Metastore not ready (attempt {i}/{MAX_RETRIES})...")
        time.sleep(DELAY)

print(f"[TIMEOUT] Hive Metastore not ready after {MAX_RETRIES * DELAY}s")
sys.exit(1)
