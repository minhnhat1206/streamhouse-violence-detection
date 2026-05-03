import { useEffect, useRef, useCallback, useState } from "react";

export function useCommandRunner({ onOutput, onDone, onError }) {
  const wsRef = useRef(null);
  const [connected, setConnected] = useState(false);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(
      `ws://${window.location.hostname}:3001/ws`
    );
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => {
      setConnected(false);
      // Auto-reconnect after 3s
      setTimeout(connect, 3000);
    };
    ws.onerror = () => setConnected(false);

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === "output") {
          onOutput?.(msg.stepId, msg.line, msg.stream);
        } else if (msg.type === "done") {
          onDone?.(msg.stepId, msg.code);
        } else if (msg.type === "error") {
          onError?.(msg.stepId, msg.message);
        }
      } catch (e) {
        console.error("WS parse error:", e);
      }
    };
  }, [onOutput, onDone, onError]);

  useEffect(() => {
    connect();
    return () => wsRef.current?.close();
  }, [connect]);

  const runCommand = useCallback(
    (stepId, command) => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send(
          JSON.stringify({ type: "run", stepId, command })
        );
        return true;
      }
      return false;
    },
    []
  );

  const stopCommand = useCallback((stepId) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: "stop", stepId }));
    }
  }, []);

  return { connected, runCommand, stopCommand };
}
