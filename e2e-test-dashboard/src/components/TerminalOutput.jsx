import { useEffect, useRef } from "react";
import { Download, Terminal } from "lucide-react";

function classifyLine(text) {
  const l = text.toLowerCase();
  if (l.includes("error") || l.includes("fatal") || l.includes("failed") || l.includes("exception") || l.includes("traceback"))
    return "line-error";
  if (l.includes("warn") || l.includes("warning"))
    return "line-warn";
  if (l.includes("[+]") || l.includes("started") || l.includes("running") || l.includes("created") || l.includes("done") || l.includes("healthy"))
    return "line-info";
  if (text.startsWith("$") || text.startsWith(">"))
    return "line-cmd";
  return "";
}

export default function TerminalOutput({ lines, stepId }) {
  const endRef  = useRef(null);
  const termRef = useRef(null);

  // Auto-scroll to bottom when new lines arrive
  useEffect(() => {
    if (termRef.current) {
      termRef.current.scrollTop = termRef.current.scrollHeight;
    }
  }, [lines]);

  const handleExport = () => {
    const content = lines.map((l) => l.text).join("\n");
    const blob = new Blob([content], { type: "text/plain" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = `step-${stepId}-output.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-2">
      {/* Label row */}
      <div className="flex items-center justify-between">
        <div className="section-label">
          <Terminal size={11} /> Terminal Output
          {lines.length > 0 && (
            <span
              className="badge ml-2"
              style={{ background: "rgba(15,23,42,0.8)", color: "#475569", border: "1px solid #1e293b" }}
            >
              {lines.length} lines
            </span>
          )}
        </div>
        {lines.length > 0 && (
          <button className="btn btn-ghost" style={{ padding: "4px 10px", fontSize: "11.5px" }} onClick={handleExport}>
            <Download size={11} />
            Export
          </button>
        )}
      </div>

      {/* Terminal box */}
      <div
        ref={termRef}
        className="terminal"
        style={{ height: "240px", position: "relative" }}
        id={`terminal-${stepId}`}
      >
        {lines.length === 0 ? (
          <span style={{ color: "#1e3a5f", fontStyle: "italic" }}>
            {"// Waiting for command output..."}
          </span>
        ) : (
          lines.slice(-500).map((line, i) => (
            <div key={i} className={classifyLine(line.text)}>
              {line.text || " "}
            </div>
          ))
        )}
        <div ref={endRef} />
      </div>
    </div>
  );
}
