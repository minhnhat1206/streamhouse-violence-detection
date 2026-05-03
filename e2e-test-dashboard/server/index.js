const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");
const { spawn } = require("child_process");
const os = require("os");
const path = require("path");

// Auto-detect project root (parent of e2e-test-dashboard)
const PROJECT_CWD = process.env.PROJECT_CWD || path.resolve(__dirname, "..", "..");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/ws" });

// Track running processes per stepId
const runningProcesses = new Map();

// ─── WebSocket handler ─────────────────────────────────────────────────────
wss.on("connection", (ws) => {
  console.log("[WS] Client connected");

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return;
    }

    if (msg.type === "run") {
      const { stepId, command } = msg;
      console.log(`[RUN] Step ${stepId}: ${command}`);

      // Kill any existing process for this step
      if (runningProcesses.has(stepId)) {
        const old = runningProcesses.get(stepId);
        try { old.kill("SIGTERM"); } catch (_) {}
        runningProcesses.delete(stepId);
      }

      // Spawn process — use cmd /c on Windows to handle docker compose etc.
      const proc = spawn("cmd", ["/c", command], {
        cwd: PROJECT_CWD,
        shell: false,
        env: { ...process.env }
      });

      runningProcesses.set(stepId, proc);

      const sendLine = (line, stream) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "output", stepId, line, stream }));
        }
      };

      const buffer = { stdout: "", stderr: "" };

      const flush = (stream) => {
        const lines = buffer[stream].split("\n");
        buffer[stream] = lines.pop(); // keep incomplete line
        lines.forEach((l) => {
          if (l.trim()) sendLine(l, stream);
        });
      };

      proc.stdout.on("data", (d) => {
        buffer.stdout += d.toString();
        flush("stdout");
      });
      proc.stderr.on("data", (d) => {
        buffer.stderr += d.toString();
        flush("stderr");
      });

      proc.on("close", (code) => {
        // flush remaining
        if (buffer.stdout.trim()) sendLine(buffer.stdout, "stdout");
        if (buffer.stderr.trim()) sendLine(buffer.stderr, "stderr");
        runningProcesses.delete(stepId);
        console.log(`[DONE] Step ${stepId} exited with code ${code}`);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "done", stepId, code: code ?? 0 }));
        }
      });

      proc.on("error", (err) => {
        runningProcesses.delete(stepId);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "error", stepId, message: err.message }));
        }
      });
    }

    if (msg.type === "stop") {
      const { stepId } = msg;
      const proc = runningProcesses.get(stepId);
      if (proc) {
        try {
          // Windows: use taskkill to kill process tree
          spawn("taskkill", ["/PID", proc.pid.toString(), "/F", "/T"]);
        } catch (_) {
          proc.kill("SIGTERM");
        }
        runningProcesses.delete(stepId);
        console.log(`[STOP] Step ${stepId} killed`);
      }
    }
  });

  ws.on("close", () => {
    console.log("[WS] Client disconnected");
  });
});

// ─── REST endpoints ────────────────────────────────────────────────────────
app.get("/api/status", async (req, res) => {
  try {
    // Get docker container count
    const { execSync } = require("child_process");
    let services = 0;
    let flinkJobs = 0;

    try {
      const out = execSync(
        'docker ps --format "{{.Names}}" 2>NUL',
        { timeout: 3000, encoding: "utf8" }
      );
      const names = out.split("\n").filter(Boolean);
      services = names.length;
      // Count flink-related jobs via jobmanager API
      try {
        const jobsOut = execSync(
          'curl -s http://localhost:8081/jobs/overview 2>NUL',
          { timeout: 2000, encoding: "utf8" }
        );
        const jobsData = JSON.parse(jobsOut);
        flinkJobs = jobsData?.jobs?.filter((j) => j.state === "RUNNING").length || 0;
      } catch (_) {}
    } catch (_) {}

    // RAM usage
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMem = totalMem - freeMem;
    const ramUsage = +(usedMem / 1024 / 1024 / 1024).toFixed(2);
    const ramTotal = +(totalMem / 1024 / 1024 / 1024).toFixed(0);

    // Data layer metrics (mock for now — in production, query Flink/Trino)
    const dataLayers = {
      hot: {
        technology: "Apache Fluss",
        table: "hot_violence_alerts",
        recordCount: Math.floor(Math.random() * 5000) + 500,
        latencyMs: Math.floor(Math.random() * 100) + 20,
        retention: "1-2 hours",
        status: "HEALTHY",
      },
      warm: {
        technology: "Apache Paimon",
        tables: ["violence_incidents", "daily_incident_stats", "camera_stats"],
        recordCount: 103956,
        latencyMs: 180000,
        retention: "7-30 days",
        status: "HEALTHY",
      },
      cold: {
        technology: "Apache Iceberg",
        table: "historical_violence_incidents",
        recordCount: Math.floor(Math.random() * 100) + 10,
        latencyMs: Math.floor(Math.random() * 5000) + 2700,
        retention: "Years (archival)",
        status: "HEALTHY",
      },
    };

    res.json({
      services,
      totalServices: 13,
      flinkJobs,
      ramUsage,
      ramTotal,
      dataLayers,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/health", (req, res) => {
  res.json({ status: "ok", pid: process.pid });
});

// ─── Start server ──────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`\n🚀 E2E Dashboard Backend running at http://localhost:${PORT}`);
  console.log(`   WebSocket: ws://localhost:${PORT}/ws`);
  console.log(`   Status:    http://localhost:${PORT}/api/status`);
  console.log(`\n   PROJECT_CWD: ${PROJECT_CWD}\n`);
});
