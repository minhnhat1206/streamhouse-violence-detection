import { Copy, Check } from "lucide-react";
import { useState } from "react";

export default function CommandBox({ command }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(command);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (e) {
      console.error("Copy failed:", e);
    }
  };

  return (
    <div className="relative group">
      <div className="cmd-block pr-10">{command}</div>
      <button
        onClick={handleCopy}
        className="absolute top-2 right-2 p-1.5 rounded-md bg-slate-700 hover:bg-slate-600 transition-colors opacity-0 group-hover:opacity-100"
        title="Copy command"
      >
        {copied ? (
          <Check size={13} className="text-green-400" />
        ) : (
          <Copy size={13} className="text-slate-400" />
        )}
      </button>
    </div>
  );
}
