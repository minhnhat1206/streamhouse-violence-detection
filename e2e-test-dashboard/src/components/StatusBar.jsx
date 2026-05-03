import { useEffect, useState } from "react";
import { Activity, Cpu, Server, Clock, Wifi, WifiOff } from "lucide-react";

function useUptime(startTime) {
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    if (!startTime) return;
    const update = () => setElapsed(Math.floor((Date.now() - startTime) / 1000));
    update();
    const timer = setInterval(update, 1000);
    return () => clearInterval(timer);
  }, [startTime]);

  const h = Math.floor(elapsed / 3600);
  const m = Math.floor((elapsed % 3600) / 60);
  const s = elapsed % 60;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function Separator() {
  return <div style={{ width: "1px", height: "18px", background: "#1e293b", flexShrink: 0 }} />;
}

function Stat({ icon: Icon, label, value, color, subBar }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
      <Icon size={12} style={{ color: color || "#475569", flexShrink: 0 }} />
      <span style={{ fontSize: "11.5px", color: "#475569" }}>{label}:</span>
      <span style={{ fontSize: "11.5px", fontFamily: "var(--font-mono)", color: color || "#94a3b8" }}>
        {value}
      </span>
      {subBar}
    </div>
  );
}

export default function StatusBar({ systemStatus, startTime }) {
  const uptime = useUptime(startTime);

  const {
    services     = 0,
    totalServices= 13,
    flinkJobs    = 0,
    ramUsage     = 0,
    ramTotal     = 16,
    connected    = false,
  } = systemStatus || {};

  const ramPct   = Math.round((ramUsage / ramTotal) * 100) || 0;
  const ramColor = ramPct > 85 ? "#ef4444" : ramPct > 70 ? "#eab308" : "#22c55e";

  return (
    <footer
      style={{
        height: "36px",
        display: "flex",
        alignItems: "center",
        padding: "0 16px",
        gap: "14px",
        borderTop: "1px solid rgba(30,41,59,0.9)",
        background: "rgba(4,9,20,0.97)",
        flexShrink: 0,
        zIndex: 10,
      }}
    >
      {/* RAM */}
      <Stat
        icon={Cpu}
        label="RAM"
        value={`${ramUsage.toFixed(1)}/${ramTotal}GB`}
        color={ramColor}
        subBar={
          <div
            style={{
              width: "52px",
              height: "4px",
              borderRadius: "99px",
              background: "#1e293b",
              overflow: "hidden",
              flexShrink: 0,
            }}
          >
            <div
              style={{
                height: "100%",
                width: `${ramPct}%`,
                borderRadius: "99px",
                background: ramColor,
                transition: "width 0.5s ease",
              }}
            />
          </div>
        }
      />

      <Separator />

      {/* Flink Jobs */}
      <Stat
        icon={Activity}
        label="Flink Jobs"
        value={`${flinkJobs}/4`}
        color={flinkJobs === 4 ? "#22c55e" : flinkJobs > 0 ? "#60a5fa" : "#475569"}
      />

      <Separator />

      {/* Services */}
      <Stat
        icon={Server}
        label="Containers"
        value={`${services}/${totalServices}`}
        color={services > 0 ? "#60a5fa" : "#475569"}
      />

      <Separator />

      {/* Uptime */}
      <Stat
        icon={Clock}
        label="Uptime"
        value={startTime ? uptime : "--:--:--"}
      />

      {/* Right: connection */}
      <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: "5px" }}>
        <div
          style={{
            width: "6px",
            height: "6px",
            borderRadius: "50%",
            background: connected ? "#22c55e" : "#475569",
            boxShadow: connected ? "0 0 6px #22c55e" : "none",
            flexShrink: 0,
          }}
        />
        <span style={{ fontSize: "11px", color: connected ? "#4ade80" : "#475569" }}>
          {connected ? "Backend Connected" : "Backend Offline — run: npm run server"}
        </span>
      </div>
    </footer>
  );
}
