import { useState, useCallback, useEffect } from "react";
import { PIPELINE_PHASES, STATUS, TOTAL_STEPS } from "./data/pipeline-steps";
import { useCommandRunner } from "./hooks/useCommandRunner";
import Sidebar from "./components/Sidebar";
import StepDetail from "./components/StepDetail";
import StatusBar from "./components/StatusBar";
import LiveStreamsPanel from "./components/LiveStreamsPanel";
import DataLayerStatusPanel from "./components/DataLayerStatusPanel";
import DataMetricsPanel from "./components/DataMetricsPanel";
import { Shield, Wifi, WifiOff, X, Play, Loader2 } from "lucide-react";

/* ── Toast ───────────────────────────────────────────────────────────────── */
let _toastId = 0;

function Toast({ toasts, removeToast }) {
  return (
    <div className="toast-container">
      {toasts.map((t) => (
        <div key={t.id} className={`toast show ${t.type}`}>
          <span className="flex-1">{t.message}</span>
          <button
            onClick={() => removeToast(t.id)}
            className="opacity-50 hover:opacity-100 transition-opacity flex-shrink-0"
          >
            <X size={12} />
          </button>
        </div>
      ))}
    </div>
  );
}

/* ── App ─────────────────────────────────────────────────────────────────── */
// Load persisted state from localStorage
const loadPersistedState = () => {
  try {
    const saved = localStorage.getItem("e2e-pipeline-state");
    return saved ? JSON.parse(saved) : {};
  } catch { return {}; }
};

export default function App() {
  const [activePhase, setActivePhase]     = useState(0);
  const [stepStatuses, setStepStatuses]   = useState(loadPersistedState);
  const [stepOutputs, setStepOutputs]     = useState({});
  const [startTime, setStartTime]         = useState(null);
  const [toasts, setToasts]               = useState([]);
  const [systemStatus, setSystemStatus]   = useState({
    services: 0,
    totalServices: 13,
    flinkJobs: 0,
    ramUsage: 0,
    ramTotal: 16,
    connected: false,
  });
  const [rightPanelMode, setRightPanelMode] = useState(
    localStorage.getItem("e2e-dashboard-panel") || null
  );

  /* ── Toasts ── */
  const addToast = useCallback((message, type = "info") => {
    const id = ++_toastId;
    setToasts((prev) => [...prev, { id, message, type }]);
    setTimeout(() => setToasts((prev) => prev.filter((t) => t.id !== id)), 5000);
  }, []);

  const removeToast = useCallback((id) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  /* ── WebSocket callbacks ── */
  const handleOutput = useCallback((stepId, line, stream) => {
    setStepOutputs((prev) => ({
      ...prev,
      [stepId]: [...(prev[stepId] || []).slice(-499), { text: line, stream }],
    }));
  }, []);

  const handleDone = useCallback((stepId, code) => {
    const success = code === 0;
    setStepStatuses((prev) => ({
      ...prev,
      [stepId]: success ? STATUS.DONE : STATUS.ERROR,
    }));
    addToast(
      success
        ? `✅ Step ${stepId} completed successfully`
        : `❌ Step ${stepId} failed (exit ${code})`,
      success ? "success" : "error"
    );
  }, [addToast]);

  const handleError = useCallback((stepId, message) => {
    setStepStatuses((prev) => ({ ...prev, [stepId]: STATUS.ERROR }));
    setStepOutputs((prev) => ({
      ...prev,
      [stepId]: [...(prev[stepId] || []), { text: `[ERROR] ${message}`, stream: "stderr" }],
    }));
    addToast(`❌ Step ${stepId}: ${message}`, "error");
  }, [addToast]);

  const { connected, runCommand, stopCommand } = useCommandRunner({
    onOutput: handleOutput,
    onDone: handleDone,
    onError: handleError,
  });

  /* ── Persist step statuses to localStorage ── */
  useEffect(() => {
    try {
      localStorage.setItem("e2e-pipeline-state", JSON.stringify(stepStatuses));
    } catch {}
  }, [stepStatuses]);

  /* ── Persist right panel mode to localStorage ── */
  useEffect(() => {
    localStorage.setItem("e2e-dashboard-panel", rightPanelMode || "");
  }, [rightPanelMode]);

  /* ── Sync connection state into systemStatus ── */
  useEffect(() => {
    setSystemStatus((prev) => ({ ...prev, connected }));
  }, [connected]);

  /* ── Poll /api/status every 6s — SILENT on failure (ECONNREFUSED expected when server is off) ── */
  useEffect(() => {
    let mounted = true;
    const poll = async () => {
      if (!mounted) return;
      try {
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), 3000);
        const res = await fetch("/api/status", { signal: ctrl.signal });
        clearTimeout(timer);
        if (res.ok && mounted) {
          const data = await res.json();
          setSystemStatus((prev) => ({ ...prev, ...data, connected }));
        }
      } catch {
        // Silently ignore — server may not be running yet
      }
    };
    poll();
    const interval = setInterval(poll, 6000);
    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [connected]);

  /* ── Handlers ── */
  const handleRun = useCallback((step) => {
    if (!startTime) setStartTime(Date.now());
    setStepStatuses((prev) => ({ ...prev, [step.id]: STATUS.RUNNING }));
    setStepOutputs((prev) => ({
      ...prev,
      [step.id]: [{ text: `$ ${step.command}`, stream: "cmd" }],
    }));
    const ok = runCommand(step.id, step.command);
    if (!ok) {
      addToast("⚠ Backend not connected. Run: npm run server", "error");
      setStepStatuses((prev) => ({ ...prev, [step.id]: STATUS.ERROR }));
    }
  }, [runCommand, startTime, addToast]);

  const handleStop = useCallback((stepId) => {
    stopCommand(stepId);
    setStepStatuses((prev) => ({ ...prev, [stepId]: STATUS.ERROR }));
    addToast(`⏹ Step ${stepId} stopped`, "info");
  }, [stopCommand, addToast]);

  const handleSkip = useCallback((step) => {
    setStepStatuses((prev) => ({ ...prev, [step.id]: STATUS.SKIPPED }));
    addToast(`⏭ Step ${step.id} skipped — ${step.skipReason || "Already done"}`, "info");
  }, [addToast]);

  const handleReset = useCallback(() => {
    setStepStatuses({});
    setStepOutputs({});
    setStartTime(null);
    localStorage.removeItem("e2e-pipeline-state");
    addToast("🔄 Pipeline state reset", "info");
  }, [addToast]);

  /* ── Run All Logic ── */
  const [isRunningAll, setIsRunningAll] = useState(false);

  const handleRunAll = useCallback(() => {
    if (isRunningAll) {
      setIsRunningAll(false);
      addToast("⏹ Automatic execution stopped", "info");
      return;
    }
    
    // Find first incomplete step with dependencies satisfied
    const allSteps = PIPELINE_PHASES.flatMap(p => p.steps);
    const next = allSteps.find(s => 
      (stepStatuses[s.id] !== STATUS.DONE && stepStatuses[s.id] !== STATUS.SKIPPED) &&
      (!s.dependencies?.length || s.dependencies.every(d => stepStatuses[d] === STATUS.DONE || stepStatuses[d] === STATUS.SKIPPED))
    );

    if (next) {
      setIsRunningAll(true);
      handleRun(next);
      addToast("🚀 Starting automatic pipeline execution", "success");
    } else {
      addToast("✨ All steps are already completed!", "success");
    }
  }, [isRunningAll, stepStatuses, handleRun, addToast]);

  // Handle sequence
  useEffect(() => {
    if (!isRunningAll) return;
    const allSteps = PIPELINE_PHASES.flatMap(p => p.steps);
    const currentRunning = allSteps.find(s => stepStatuses[s.id] === STATUS.RUNNING);
    
    if (!currentRunning) {
      const next = allSteps.find(s => 
        (stepStatuses[s.id] !== STATUS.DONE && stepStatuses[s.id] !== STATUS.SKIPPED && stepStatuses[s.id] !== STATUS.ERROR) &&
        (!s.dependencies?.length || s.dependencies.every(d => stepStatuses[d] === STATUS.DONE || stepStatuses[d] === STATUS.SKIPPED))
      );

      if (next) {
        const timer = setTimeout(() => handleRun(next), 1500);
        return () => clearTimeout(timer);
      } else {
        setIsRunningAll(false);
        addToast("🏁 Pipeline execution finished", "success");
      }
    }
  }, [isRunningAll, stepStatuses, handleRun, addToast]);

  const completedSteps = Object.values(stepStatuses).filter(
    (s) => s === STATUS.DONE || s === STATUS.SKIPPED
  ).length;

  const activePhaseData = PIPELINE_PHASES.find((p) => p.id === activePhase);
  const pct = TOTAL_STEPS ? Math.round((completedSteps / TOTAL_STEPS) * 100) : 0;

  return (
    <div className="flex flex-col" style={{ height: "100vh", width: "100vw", overflow: "hidden", background: "#080f1e" }}>

      {/* ── Header ── */}
      <header
        style={{
          height: "54px",
          display: "flex",
          alignItems: "center",
          padding: "0 20px",
          gap: "16px",
          borderBottom: "1px solid rgba(51,65,85,0.7)",
          background: "rgba(8,15,30,0.97)",
          backdropFilter: "blur(16px)",
          flexShrink: 0,
          zIndex: 10,
        }}
      >
        {/* Logo */}
        <div style={{ display: "flex", alignItems: "center", gap: "10px", flexShrink: 0 }}>
          <Shield size={20} style={{ color: "#60a5fa", filter: "drop-shadow(0 0 8px #3b82f6)" }} />
          <div>
            <div style={{ fontSize: "14px", fontWeight: 700, color: "#f1f5f9", letterSpacing: "-0.01em", lineHeight: 1.2 }}>
              E2E Pipeline Test
            </div>
            <div style={{ fontSize: "11px", color: "#475569", fontFamily: "var(--font-mono)" }}>
              Streamhouse Architecture
            </div>
          </div>
        </div>

        {/* Divider */}
        <div style={{ width: "1px", height: "28px", background: "#1e293b", flexShrink: 0 }} />

        {/* Progress bar + label */}
        <div style={{ flex: 1, display: "flex", alignItems: "center", gap: "12px" }}>
          <div style={{ flex: 1, display: "flex", alignItems: "center", gap: "10px" }}>
            <div
              style={{ flex: 1, height: "5px", borderRadius: "99px", background: "#1e293b", overflow: "hidden" }}
            >
              <div
                style={{
                  height: "100%",
                  width: `${pct}%`,
                  borderRadius: "99px",
                  background: pct === 100
                    ? "linear-gradient(90deg, #22c55e, #4ade80)"
                    : "linear-gradient(90deg, #2563eb, #60a5fa)",
                  transition: "width 0.6s ease",
                }}
              />
            </div>
            <span style={{ fontSize: "12px", color: "#64748b", fontFamily: "var(--font-mono)", flexShrink: 0 }}>
              {completedSteps}/{TOTAL_STEPS} steps ({pct}%)
            </span>
          </div>
        </div>
        {/* Run All Button */}
        <button
          onClick={handleRunAll}
          className={`btn ${isRunningAll ? "btn-danger" : "btn-primary"} flex items-center gap-2`}
          style={{ padding: "6px 14px", height: "32px", fontSize: "12px", borderRadius: "8px" }}
          disabled={!connected}
        >
          {isRunningAll ? (
            <><Loader2 size={12} className="animate-spin" /> Stop Auto</>
          ) : (
            <><Play size={12} fill="currentColor" /> Run All</>
          )}
        </button>

        {/* Reset Button */}
        <button
          onClick={handleReset}
          className="btn btn-ghost flex items-center gap-2"
          style={{ padding: "6px 14px", height: "32px", fontSize: "12px", borderRadius: "8px" }}
          title="Reset all step statuses"
        >
          🔄 Reset
        </button>

        <div style={{ width: "1px", height: "28px", background: "#1e293b", flexShrink: 0 }} />

        {/* Right Panel Toggle Buttons */}
        <div style={{ display: "flex", gap: "6px", flexShrink: 0 }}>
          <button
            onClick={() => setRightPanelMode(rightPanelMode === "streams" ? null : "streams")}
            className="btn btn-ghost flex items-center gap-2"
            style={{
              padding: "6px 12px",
              height: "32px",
              fontSize: "12px",
              borderRadius: "8px",
              borderColor: rightPanelMode === "streams" ? "#10b981" : "transparent",
              borderWidth: "1px",
              ...(rightPanelMode === "streams" && { background: "rgba(16,185,129,0.1)" }),
            }}
            title="Toggle live streams panel"
          >
            🎥 Streams
          </button>
          <button
            onClick={() => setRightPanelMode(rightPanelMode === "data-layers" ? null : "data-layers")}
            className="btn btn-ghost flex items-center gap-2"
            style={{
              padding: "6px 12px",
              height: "32px",
              fontSize: "12px",
              borderRadius: "8px",
              borderColor: rightPanelMode === "data-layers" ? "#10b981" : "transparent",
              borderWidth: "1px",
              ...(rightPanelMode === "data-layers" && { background: "rgba(16,185,129,0.1)" }),
            }}
            title="Toggle data layers panel"
          >
            📊 Data Layers
          </button>
          <button
            onClick={() => setRightPanelMode(rightPanelMode === "metrics" ? null : "metrics")}
            className="btn btn-ghost flex items-center gap-2"
            style={{
              padding: "6px 12px",
              height: "32px",
              fontSize: "12px",
              borderRadius: "8px",
              borderColor: rightPanelMode === "metrics" ? "#10b981" : "transparent",
              borderWidth: "1px",
              ...(rightPanelMode === "metrics" && { background: "rgba(16,185,129,0.1)" }),
            }}
            title="Toggle metrics panel"
          >
            📈 Metrics
          </button>
        </div>

        <div style={{ width: "1px", height: "28px", background: "#1e293b", flexShrink: 0 }} />

        {/* Phase indicator */}
        {activePhaseData && (
          <>
            <div style={{ width: "1px", height: "28px", background: "#1e293b", flexShrink: 0 }} />
            <div style={{ display: "flex", alignItems: "center", gap: "8px", flexShrink: 0 }}>
              <span style={{ fontSize: "18px" }}>{activePhaseData.icon}</span>
              <div>
                <div style={{ fontSize: "10px", color: "#475569", textTransform: "uppercase", letterSpacing: "0.08em" }}>
                  Phase {activePhaseData.id}
                </div>
                <div style={{ fontSize: "12px", color: "#93c5fd", fontWeight: 600 }}>
                  {activePhaseData.title}
                </div>
              </div>
            </div>
          </>
        )}

        <div style={{ width: "1px", height: "28px", background: "#1e293b", flexShrink: 0 }} />

        {/* Connection badge */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "6px",
            padding: "5px 12px",
            borderRadius: "8px",
            fontSize: "12px",
            fontWeight: 500,
            background: connected ? "rgba(34,197,94,0.1)" : "rgba(239,68,68,0.08)",
            border: `1px solid ${connected ? "rgba(34,197,94,0.3)" : "rgba(239,68,68,0.25)"}`,
            color: connected ? "#4ade80" : "#f87171",
            flexShrink: 0,
          }}
        >
          {connected
            ? <><Wifi size={12} /> Backend Online</>
            : <><WifiOff size={12} /> npm run server</>
          }
        </div>
      </header>

      {/* ── Body ── */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>
        <Sidebar
          stepStatuses={stepStatuses}
          activePhase={activePhase}
          onSelectPhase={setActivePhase}
          completedSteps={completedSteps}
          totalSteps={TOTAL_STEPS}
        />
        <StepDetail
          phase={activePhaseData}
          stepStatuses={stepStatuses}
          stepOutputs={stepOutputs}
          connected={connected}
          onRun={handleRun}
          onStop={handleStop}
          onSkip={handleSkip}
        />

        {/* ── Right Panel ── */}
        {rightPanelMode && (
          <div
            style={{
              width: "360px",
              borderLeft: "1px solid rgba(51,65,85,0.7)",
              background: "rgba(8,15,30,0.95)",
              overflow: "hidden",
              display: "flex",
              flexDirection: "column",
            }}
          >
            {rightPanelMode === "streams" && <LiveStreamsPanel />}
            {rightPanelMode === "data-layers" && <DataLayerStatusPanel />}
            {rightPanelMode === "metrics" && <DataMetricsPanel />}
          </div>
        )}
      </div>

      {/* ── Footer ── */}
      <StatusBar systemStatus={{ ...systemStatus, connected }} startTime={startTime} />

      {/* ── Toasts ── */}
      <Toast toasts={toasts} removeToast={removeToast} />
    </div>
  );
}
