import { Alert, AnalyticsData, BackendChatResponse } from '../types';

const BACKEND = 'http://localhost:5002';

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, options);
  if (!res.ok) {
    throw new Error(`Backend error ${res.status}: ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export async function fetchRecentIncidents(limit = 50): Promise<Alert[]> {
  const rows = await fetchJson<Record<string, unknown>[]>(
    `${BACKEND}/api/recent-incidents?limit=${limit}`,
  );
  return rows.map((r) => ({
    event_id: String(r.event_id ?? ''),
    timestamp: String(r.timestamp ?? ''),
    location: String(r.location ?? r.camera_id ?? ''),
    violence_score: Number(r.violence_score ?? 0),
    label: (r.label as Alert['label']) ?? 'Anomaly',
    model_version: String(r.model_version ?? 'v2.1.0'),
    clip_link: String(r.clip_link ?? '#'),
    status: (r.status as Alert['status']) ?? 'Unreviewed',
  }));
}

export async function fetchStats(): Promise<AnalyticsData> {
  return fetchJson<AnalyticsData>(`${BACKEND}/api/stats`);
}

export async function sendChatMessage(
  query: string,
  history: { role: string; content: string }[] = [],
): Promise<BackendChatResponse> {
  return fetchJson<BackendChatResponse>(`${BACKEND}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, history }),
  });
}
