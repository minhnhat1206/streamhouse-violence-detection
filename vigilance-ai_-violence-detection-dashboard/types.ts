export enum CameraStatus {
  NORMAL = 'Normal',
  VIOLENCE_DETECTED = 'Violence Detected',
  OFFLINE = 'Offline',
}

export interface Camera {
  id: string;
  city: string;
  district: string;
  ward: string;
  specificLocation: string;
  status: CameraStatus;
}

export type AlertStatus = 'Unreviewed' | 'Reviewed' | 'False Alarm';

export interface Alert {
  event_id: string;
  timestamp: string;
  location: string;
  violence_score: number;
  label: 'Fight' | 'Crowd' | 'Anomaly';
  model_version: string;
  clip_link: string;
  status: AlertStatus;
}

export interface ChatMessage {
  role: 'user' | 'model';
  content: string;
  layer?: string;
  citations?: BackendChatCitations;
}

export interface BackendChatCitations {
  source_table: string;
  data_layer: string;
  time_period: string;
  row_count?: number;
}

export interface BackendChatResponse {
  answer: string;
  sql_used?: string;
  citations: BackendChatCitations;
  layer: string;
  confidence: number;
  duration_ms: number;
}

export interface AnalyticsData {
  alertsPerHour: { name: string; alerts: number }[];
  topLocations: { name: string; alerts: number }[];
  alertTypes: { name: string; value: number }[];
  avgScore: { name: string; score: number }[];
}
