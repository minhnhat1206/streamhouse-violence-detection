import { STATUS } from "../data/pipeline-steps";

const STATUS_CFG = {
  [STATUS.PENDING]: { color: "#475569", bg: "rgba(71,85,105,0.15)", border: "rgba(71,85,105,0.25)", dot: "#475569" },
  [STATUS.RUNNING]: { color: "#60a5fa", bg: "rgba(59,130,246,0.12)", border: "rgba(59,130,246,0.45)", dot: "#60a5fa" },
  [STATUS.DONE]:    { color: "#4ade80", bg: "rgba(34,197,94,0.1)",   border: "rgba(34,197,94,0.35)",  dot: "#4ade80" },
  [STATUS.ERROR]:   { color: "#f87171", bg: "rgba(239,68,68,0.1)",   border: "rgba(239,68,68,0.3)",   dot: "#f87171" },
  [STATUS.SKIPPED]: { color: "#a78bfa", bg: "rgba(167,139,250,0.1)", border: "rgba(167,139,250,0.3)", dot: "#a78bfa" },
  partial:          { color: "#fbbf24", bg: "rgba(251,191,36,0.08)", border: "rgba(251,191,36,0.3)",  dot: "#fbbf24" },
};

function nodeStatus(stepIds, stepStatuses) {
  if (!stepIds?.length) return STATUS.PENDING;
  const ss = stepIds.map(id => stepStatuses[id] || STATUS.PENDING);
  if (ss.some(s => s === STATUS.RUNNING)) return STATUS.RUNNING;
  if (ss.some(s => s === STATUS.ERROR))   return STATUS.ERROR;
  if (ss.every(s => s === STATUS.DONE || s === STATUS.SKIPPED)) return STATUS.DONE;
  if (ss.some(s => s === STATUS.DONE || s === STATUS.SKIPPED))  return "partial";
  return STATUS.PENDING;
}

function PipeNode({ icon, label, sub, stepIds, stepStatuses, compact }) {
  const st  = nodeStatus(stepIds, stepStatuses);
  const cfg = STATUS_CFG[st] || STATUS_CFG[STATUS.PENDING];
  const glow = st === STATUS.RUNNING ? `0 0 10px ${cfg.dot}44` : "none";

  return (
    <div style={{
      display: "flex", alignItems: "center", gap: "7px",
      padding: compact ? "5px 8px" : "8px 10px",
      borderRadius: "7px",
      background: cfg.bg,
      border: `1px solid ${cfg.border}`,
      boxShadow: glow,
      transition: "all 0.35s",
    }}>
      <span style={{ fontSize: compact ? "12px" : "14px", flexShrink: 0, lineHeight: 1 }}>{icon}</span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{
          fontSize: compact ? "10px" : "11px",
          fontWeight: 600,
          color: cfg.color,
          lineHeight: 1.2,
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
        }}>
          {label}
        </div>
        <div style={{
          fontSize: "9px",
          color: "#475569",
          lineHeight: 1.3,
          marginTop: "1px",
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
        }}>
          {sub}
        </div>
      </div>
      <div
        className={st === STATUS.RUNNING ? "animate-pulse-glow" : ""}
        style={{
          width: "6px", height: "6px", borderRadius: "50%",
          background: cfg.dot, flexShrink: 0,
        }}
      />
    </div>
  );
}

function Arrow({ label } = {}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", padding: "2px 0" }}>
      <div style={{ width: "1px", height: "6px", background: "#1e293b" }} />
      {label && (
        <span style={{ fontSize: "8px", color: "#334155", fontFamily: "var(--font-mono)", margin: "1px 0" }}>
          {label}
        </span>
      )}
      <div style={{ color: "#1e293b", fontSize: "9px", lineHeight: 1 }}>▼</div>
    </div>
  );
}

export default function PipelineDiagram({ stepStatuses }) {
  const ss = stepStatuses || {};

  return (
    <div style={{
      width: "220px",
      flexShrink: 0,
      borderLeft: "1px solid rgba(30,41,59,0.8)",
      background: "rgba(4,9,20,0.65)",
      overflowY: "auto",
      display: "flex",
      flexDirection: "column",
    }}>
      {/* Header */}
      <div style={{
        padding: "11px 12px 9px",
        borderBottom: "1px solid rgba(30,41,59,0.8)",
        flexShrink: 0,
      }}>
        <div style={{ fontSize: "11px", fontWeight: 700, color: "#60a5fa", textTransform: "uppercase", letterSpacing: "0.07em" }}>
          ⚡ Pipeline Flow
        </div>
        <div style={{ fontSize: "9px", color: "#334155", marginTop: "2px", fontFamily: "var(--font-mono)" }}>
          Live architecture status
        </div>
      </div>

      {/* Diagram nodes */}
      <div style={{ padding: "10px", flex: 1, display: "flex", flexDirection: "column" }}>

        <PipeNode
          icon="🐳" label="Infrastructure" sub="Docker + Services"
          stepIds={["0.1", "1.1", "2.1", "2.2"]} stepStatuses={ss}
        />
        <Arrow />

        <PipeNode
          icon="📷" label="Data Ingestion" sub="Mock Inference"
          stepIds={["3.1"]} stepStatuses={ss}
        />
        <Arrow />

        <PipeNode
          icon="⚡" label="Flink ETL" sub="Stream Processing"
          stepIds={["4.1", "4.2"]} stepStatuses={ss}
        />
        <Arrow label="validate + route" />

        {/* Storage group */}
        <div style={{
          border: "1px solid rgba(30,41,59,0.9)",
          borderRadius: "8px",
          padding: "6px",
          background: "rgba(2,6,23,0.5)",
          display: "flex",
          flexDirection: "column",
          gap: "4px",
        }}>
          <div style={{
            fontSize: "8px", color: "#334155", textTransform: "uppercase",
            letterSpacing: "0.08em", paddingLeft: "2px", marginBottom: "1px",
          }}>
            Storage Layers
          </div>
          <PipeNode
            icon="🔥" label="Fluss HOT" sub="&lt;100ms latency"
            stepIds={["5.1", "5.2", "5.3"]} stepStatuses={ss} compact
          />
          <PipeNode
            icon="🌿" label="Paimon WARM" sub="1-10 min, ACID"
            stepIds={["6.1", "6.2", "6.3"]} stepStatuses={ss} compact
          />
          <PipeNode
            icon="🧊" label="Iceberg COLD" sub="Historical archive"
            stepIds={["7.1", "7.1b", "7.2", "7.3"]} stepStatuses={ss} compact
          />
        </div>
        <Arrow />

        <PipeNode
          icon="🔍" label="Trino SQL" sub="Federated Queries"
          stepIds={["8.1", "8.2"]} stepStatuses={ss}
        />
        <Arrow />

        <PipeNode
          icon="🤖" label="Chatbot RAG" sub="Agentic + NL Query"
          stepIds={["9.1", "10.1", "11.1"]} stepStatuses={ss}
        />
        <Arrow />

        <PipeNode
          icon="✅" label="E2E Verify" sub="Full pipeline test"
          stepIds={["12.1", "12.2"]} stepStatuses={ss}
        />
      </div>

      {/* Legend */}
      <div style={{
        padding: "8px 10px 10px",
        borderTop: "1px solid rgba(30,41,59,0.8)",
        flexShrink: 0,
      }}>
        <div style={{ fontSize: "8px", color: "#334155", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: "5px" }}>
          Legend
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: "3px" }}>
          {[
            { dot: "#475569", label: "Pending" },
            { dot: "#fbbf24", label: "Partial" },
            { dot: "#60a5fa", label: "Running" },
            { dot: "#4ade80", label: "Complete" },
            { dot: "#f87171", label: "Error" },
          ].map(({ dot, label }) => (
            <div key={label} style={{ display: "flex", alignItems: "center", gap: "5px" }}>
              <div style={{ width: "5px", height: "5px", borderRadius: "50%", background: dot, flexShrink: 0 }} />
              <span style={{ fontSize: "9px", color: "#475569" }}>{label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
