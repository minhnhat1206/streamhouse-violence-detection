import { Play, Square, Copy, RefreshCw, Check, SkipForward } from "lucide-react";
import { useState } from "react";
import { STATUS } from "../data/pipeline-steps";

export default function ActionButtons({
  step,
  status,
  depsSatisfied,
  connected,
  onRun,
  onStop,
  onSkip,
}) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(step.command);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (e) {}
  };

  const isRunning = status === STATUS.RUNNING;
  const isDone    = status === STATUS.DONE;
  const canRun    = depsSatisfied && connected && !isRunning;

  return (
    <div className="flex flex-wrap items-center gap-2">
      {!isRunning ? (
        <button
          className={`btn ${isDone ? "btn-ghost" : "btn-primary"}`}
          onClick={onRun}
          disabled={!canRun}
          title={!connected ? "Backend not connected" : !depsSatisfied ? "Dependencies not met" : ""}
        >
          <Play size={13} />
          {isDone ? "Re-run" : "Run"}
        </button>
      ) : (
        <button className="btn btn-danger" onClick={onStop}>
          <Square size={13} />
          Stop
        </button>
      )}

      <button
        className="btn btn-ghost"
        onClick={handleCopy}
        title="Copy command to clipboard"
      >
        {copied ? (
          <><Check size={13} className="text-green-400" /> Copied</>
        ) : (
          <><Copy size={13} /> Copy</>
        )}
      </button>

      {(isDone || status === STATUS.ERROR) && (
        <button
          className="btn btn-ghost"
          onClick={onRun}
          disabled={!canRun}
          title="Re-run this step"
        >
          <RefreshCw size={13} />
          Re-run
        </button>
      )}

      {!connected && (
        <span className="text-xs text-red-400 ml-2">
          ⚠ Backend disconnected
        </span>
      )}

      {!depsSatisfied && connected && (
        <span className="text-xs text-yellow-400 ml-2">
          ⚠ Complete dependencies first
        </span>
      )}

      {step.skippable && !isDone && !isRunning && status !== STATUS.SKIPPED && (
        <button
          className="btn btn-ghost"
          onClick={onSkip}
          title={step.skipReason || "Skip this step"}
        >
          <SkipForward size={13} className="text-violet-400" />
          Skip
        </button>
      )}

      {step.optional && (
        <span className="badge ml-auto" style={{background:'rgba(167,139,250,0.15)', color:'#a78bfa', border:'1px solid #7c3aed'}}>
          Optional
        </span>
      )}
    </div>
  );
}
