import ReactMarkdown from "react-markdown";
import { CheckCircle2, XCircle, Loader2, Clock, AlertCircle, Link2 } from "lucide-react";
import { STATUS } from "../data/pipeline-steps";
import CommandBox from "./CommandBox";
import TerminalOutput from "./TerminalOutput";
import ActionButtons from "./ActionButtons";

/* ── Status badge ──────────────────────────────────────── */
function StatusBadge({ status }) {
  const cfg = {
    [STATUS.PENDING]: { label: "Pending",  color: "#94a3b8", bg: "rgba(148,163,184,0.12)", border: "rgba(148,163,184,0.25)", Icon: Clock },
    [STATUS.RUNNING]: { label: "Running",  color: "#60a5fa", bg: "rgba(59,130,246,0.12)",  border: "rgba(59,130,246,0.35)",  Icon: Loader2 },
    [STATUS.DONE]:    { label: "Done",     color: "#4ade80", bg: "rgba(34,197,94,0.12)",   border: "rgba(34,197,94,0.3)",    Icon: CheckCircle2 },
    [STATUS.ERROR]:   { label: "Error",    color: "#f87171", bg: "rgba(239,68,68,0.12)",   border: "rgba(239,68,68,0.3)",    Icon: XCircle },
    [STATUS.SKIPPED]: { label: "Skipped",  color: "#a78bfa", bg: "rgba(167,139,250,0.12)", border: "rgba(167,139,250,0.3)",  Icon: AlertCircle },
  };
  const { label, color, bg, border, Icon } = cfg[status] || cfg[STATUS.PENDING];
  const spin = status === STATUS.RUNNING;
  return (
    <span
      className="badge"
      style={{ color, background: bg, border: `1px solid ${border}` }}
    >
      <Icon size={10} className={spin ? "animate-spin" : ""} />
      {label}
    </span>
  );
}

/* ── Dep badge ─────────────────────────────────────────── */
function DepBadge({ depId, depStatus }) {
  const ok = depStatus === STATUS.DONE || depStatus === STATUS.SKIPPED;
  return (
    <span
      className="badge"
      style={{
        background: ok ? "rgba(34,197,94,0.1)" : "rgba(239,68,68,0.1)",
        color:      ok ? "#4ade80" : "#f87171",
        border:     `1px solid ${ok ? "rgba(34,197,94,0.3)" : "rgba(239,68,68,0.25)"}`,
      }}
    >
      {ok ? <CheckCircle2 size={9} /> : <Clock size={9} />}
      Step {depId}
    </span>
  );
}

/* ── Tag badge ─────────────────────────────────────────── */
function TagBadge({ tag }) {
  return (
    <span
      className="badge"
      style={{ background: "rgba(15,23,42,0.8)", color: "#64748b", border: "1px solid #1e293b" }}
    >
      {tag}
    </span>
  );
}

/* ── Markdown prose renderer ───────────────────────────── */
const MarkdownComponents = {
  p: ({ children }) => <p className="text-slate-300 text-[13.5px] leading-relaxed">{children}</p>,
  strong: ({ children }) => <strong className="text-white font-semibold">{children}</strong>,
  em: ({ children }) => <em className="text-slate-400 italic">{children}</em>,
  code: ({ children }) => (
    <code className="font-mono text-[12px] bg-blue-950/50 text-sky-300 px-1.5 py-0.5 rounded border border-sky-900/40">
      {children}
    </code>
  ),
  a: ({ href, children }) => (
    <a href={href} target="_blank" rel="noopener noreferrer" className="text-blue-400 underline hover:text-blue-300">
      {children}
    </a>
  ),
};

/* ── Single step card ──────────────────────────────────── */
function StepCard({ step, status, outputs, depsSatisfied, stepStatuses, onRun, onStop, onSkip, connected }) {
  const borderColor =
    status === STATUS.DONE    ? "rgba(34,197,94,0.35)"   :
    status === STATUS.ERROR   ? "rgba(239,68,68,0.35)"   :
    status === STATUS.RUNNING ? "rgba(59,130,246,0.5)"   :
    "rgba(51,65,85,0.5)";

  const glowColor =
    status === STATUS.RUNNING ? "0 0 24px rgba(59,130,246,0.12)" :
    status === STATUS.DONE    ? "0 0 20px rgba(34,197,94,0.08)"  : "none";

  return (
    <div
      className="glass-card p-6 space-y-5 animate-fade-up"
      style={{ borderColor, boxShadow: glowColor, transition: "border-color 0.4s, box-shadow 0.4s" }}
    >
      {/* ── Header ── */}
      <div className="flex items-start gap-3">
        {/* Step number bubble */}
        <div
          className="w-9 h-9 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0 font-mono"
          style={{
            background: status === STATUS.DONE    ? "rgba(34,197,94,0.2)"   :
                        status === STATUS.ERROR   ? "rgba(239,68,68,0.2)"   :
                        status === STATUS.RUNNING ? "rgba(59,130,246,0.2)"  : "rgba(51,65,85,0.4)",
            color:      status === STATUS.DONE    ? "#4ade80"   :
                        status === STATUS.ERROR   ? "#f87171"   :
                        status === STATUS.RUNNING ? "#60a5fa"   : "#64748b",
            border: `1.5px solid ${borderColor}`,
          }}
        >
          {status === STATUS.RUNNING
            ? <Loader2 size={14} className="animate-spin" />
            : step.id}
        </div>

        {/* Title + badges */}
        <div className="flex-1 min-w-0">
          <h2 className="font-semibold text-white text-[15px] leading-snug">
            Step {step.id} — {step.title}
          </h2>
          <div className="flex flex-wrap items-center gap-1.5 mt-2">
            <StatusBadge status={status} />
            {step.estimatedTime && (
              <span className="text-[11px] text-slate-500 font-mono bg-slate-800/60 px-2 py-0.5 rounded-full border border-slate-700">
                ⏱ {step.estimatedTime}
              </span>
            )}
            {step.optional && (
              <span className="badge" style={{ background: "rgba(167,139,250,0.12)", color: "#a78bfa", border: "1px solid rgba(124,58,237,0.35)" }}>
                Optional
              </span>
            )}
            {step.tags?.map((t) => <TagBadge key={t} tag={t} />)}
          </div>
        </div>
      </div>

      {/* ── Description (markdown) ── */}
      <div className="rounded-xl p-4 space-y-1" style={{ background: "rgba(2,6,23,0.5)", border: "1px solid rgba(30,41,59,0.8)" }}>
        <div className="section-label">
          <span>📋</span> Description
        </div>
        <div className="prose">
          <ReactMarkdown components={MarkdownComponents}>
            {step.description}
          </ReactMarkdown>
        </div>
      </div>

      {/* ── Dependencies ── */}
      {step.dependencies?.length > 0 && (
        <div className="space-y-2">
          <div className="section-label">
            <Link2 size={11} /> Requires
          </div>
          <div className="flex flex-wrap gap-1.5">
            {step.dependencies.map((dep) => (
              <DepBadge key={dep} depId={dep} depStatus={stepStatuses[dep]} />
            ))}
          </div>
        </div>
      )}

      {/* ── Command ── */}
      <div className="space-y-2">
        <div className="section-label">
          <span>💻</span> Command
        </div>
        <CommandBox command={step.command} />
      </div>

      {/* ── Actions ── */}
      <ActionButtons
        step={step}
        status={status}
        depsSatisfied={depsSatisfied}
        connected={connected}
        onRun={() => onRun(step)}
        onStop={() => onStop(step.id)}
        onSkip={() => onSkip(step)}
      />

      {/* ── Terminal output ── */}
      <TerminalOutput lines={outputs} stepId={step.id} />

      {/* ── Verification ── */}
      {step.verification?.length > 0 && (
        <div className="space-y-2">
          <div className="section-label">
            <CheckCircle2 size={11} /> Verification Checklist
          </div>
          <ul className="space-y-1.5">
            {step.verification.map((item, i) => (
              <li key={i} className="flex items-start gap-2.5 text-[13px]" style={{ color: '#94a3b8' }}>
                <span
                  style={{
                    marginTop: '4px',
                    width: '14px',
                    height: '14px',
                    borderRadius: '50%',
                    border: '1.5px solid #334155',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    flexShrink: 0,
                  }}
                >
                  <span style={{ width: '5px', height: '5px', borderRadius: '50%', background: '#475569' }} />
                </span>
                <span className="leading-relaxed">{item}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

/* ── Main StepDetail component ─────────────────────────── */
export default function StepDetail({
  phase,
  stepStatuses,
  stepOutputs,
  connected,
  onRun,
  onStop,
  onSkip,
}) {
  if (!phase) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center space-y-3">
          <div className="text-5xl">🔒</div>
          <div className="text-lg font-semibold text-slate-300">
            Smart Security Monitoring
          </div>
          <div className="text-sm text-slate-500">
            Select a phase from the sidebar to begin the pipeline test
          </div>
        </div>
      </div>
    );
  }

  const isDependencySatisfied = (step) => {
    if (!step.dependencies?.length) return true;
    return step.dependencies.every(
      (dep) => stepStatuses[dep] === STATUS.DONE || stepStatuses[dep] === STATUS.SKIPPED
    );
  };

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="p-6 space-y-6 max-w-4xl mx-auto">
        {/* Phase header */}
        <div className="flex items-center gap-4 pb-5 border-b border-slate-700/70">
          <span className="text-4xl">{phase.icon}</span>
          <div>
            <div
              className="text-[11px] font-bold uppercase tracking-widest mb-1"
              style={{ color: "#3b82f6" }}
            >
              Phase {String(phase.id).padStart(2, "0")}
            </div>
            <h1 className="text-2xl font-bold text-white tracking-tight">
              {phase.title}
            </h1>
            <p className="text-sm text-slate-400 mt-1">{phase.description}</p>
          </div>
        </div>

        {/* Step cards */}
        {phase.steps.map((step) => (
          <StepCard
            key={step.id}
            step={step}
            status={stepStatuses[step.id] || STATUS.PENDING}
            outputs={stepOutputs[step.id] || []}
            depsSatisfied={isDependencySatisfied(step)}
            stepStatuses={stepStatuses}
            connected={connected}
            onRun={onRun}
            onStop={onStop}
            onSkip={onSkip}
          />
        ))}
      </div>
    </div>
  );
}
