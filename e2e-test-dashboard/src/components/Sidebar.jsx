import { PIPELINE_PHASES, STATUS } from "../data/pipeline-steps";
import { CheckCircle2, XCircle, Clock, Loader2, AlertCircle, ChevronRight } from "lucide-react";

function PhaseStatusDot({ status, isRunning }) {
  if (isRunning) {
    return (
      <span
        className="w-2.5 h-2.5 rounded-full flex-shrink-0 animate-pulse-glow"
        style={{ background: "#3b82f6", color: "#3b82f6" }}
      />
    );
  }
  const colors = {
    [STATUS.DONE]:    "#22c55e",
    [STATUS.ERROR]:   "#ef4444",
    [STATUS.SKIPPED]: "#a78bfa",
    [STATUS.PENDING]: "#334155",
  };
  return (
    <span
      className="w-2.5 h-2.5 rounded-full flex-shrink-0"
      style={{ background: colors[status] || "#334155" }}
    />
  );
}

export default function Sidebar({
  stepStatuses,
  activePhase,
  onSelectPhase,
  completedSteps,
  totalSteps,
}) {
  const pct = totalSteps ? Math.round((completedSteps / totalSteps) * 100) : 0;

  return (
    <aside
      className="flex flex-col border-r border-slate-700/70"
      style={{ width: "240px", minWidth: "220px", background: "rgba(9,15,29,0.8)" }}
    >
      {/* Progress header */}
      <div className="px-4 py-4 border-b border-slate-700/70 space-y-2.5">
        <div className="flex items-center justify-between">
          <span className="text-[11px] font-bold uppercase tracking-widest text-slate-500">
            Pipeline Phases
          </span>
          <span className="font-mono text-[11px] text-slate-400">
            {completedSteps}/{totalSteps}
          </span>
        </div>
        <div className="progress-bar">
          <div
            className="progress-fill"
            style={{
              width: `${pct}%`,
              background: pct === 100
                ? "linear-gradient(90deg, #22c55e, #4ade80)"
                : "linear-gradient(90deg, #2563eb, #60a5fa)",
            }}
          />
        </div>
        <div className="text-[11px] text-slate-500 text-right">{pct}% complete</div>
      </div>

      {/* Phase list */}
      <nav className="flex-1 overflow-y-auto py-2 px-2 space-y-0.5">
        {PIPELINE_PHASES.map((phase) => {
          const phaseStepIds = phase.steps.map((s) => s.id);
          const statuses = phaseStepIds.map((id) => stepStatuses[id] || STATUS.PENDING);

          const hasRunning = statuses.some((s) => s === STATUS.RUNNING);
          const hasError   = statuses.some((s) => s === STATUS.ERROR);
          const allDone    = statuses.every((s) => s === STATUS.DONE || s === STATUS.SKIPPED);
          const doneCount  = statuses.filter((s) => s === STATUS.DONE || s === STATUS.SKIPPED).length;

          let phaseStatus = STATUS.PENDING;
          if (hasRunning)      phaseStatus = STATUS.RUNNING;
          else if (hasError)   phaseStatus = STATUS.ERROR;
          else if (allDone)    phaseStatus = STATUS.DONE;

          const isActive = activePhase === phase.id;

          const titleColor = isActive
            ? "#93c5fd"
            : phaseStatus === STATUS.DONE    ? "#4ade80"
            : phaseStatus === STATUS.ERROR   ? "#f87171"
            : phaseStatus === STATUS.RUNNING ? "#60a5fa"
            : "#cbd5e1";

          return (
            <button
              key={phase.id}
              className={`sidebar-phase ${isActive ? "active" : ""}`}
              onClick={() => onSelectPhase(phase.id)}
            >
              <div className="flex items-center gap-2.5">
                {/* Icon */}
                <span className="text-[18px] leading-none w-6 text-center flex-shrink-0">
                  {phase.icon}
                </span>

                {/* Content */}
                <div className="flex-1 min-w-0">
                  <div
                    className="text-[13px] font-semibold truncate"
                    style={{ color: titleColor }}
                  >
                    {phase.title}
                  </div>
                  <div className="flex items-center gap-1.5 mt-0.5">
                    <PhaseStatusDot
                      status={phaseStatus}
                      isRunning={hasRunning}
                    />
                    <span className="text-[11px] text-slate-500 font-mono">
                      {doneCount}/{phaseStepIds.length}
                    </span>
                    <span className="text-[10px] text-slate-600">steps</span>
                  </div>
                </div>

                {/* Active indicator */}
                {isActive && (
                  <ChevronRight size={13} className="text-blue-400 flex-shrink-0" />
                )}
              </div>
            </button>
          );
        })}
      </nav>

      {/* Footer */}
      <div
        className="px-4 py-3 border-t border-slate-700/70 text-center"
        style={{ background: "rgba(0,0,0,0.2)" }}
      >
        <span className="text-[11px] text-slate-600">
          {PIPELINE_PHASES.length} phases · {totalSteps} steps total
        </span>
      </div>
    </aside>
  );
}
