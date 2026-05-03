import { useState, useEffect } from 'react';
import { ChevronDown, ChevronUp, RefreshCw } from 'lucide-react';

function LayerColumn({ layer, data, isRefreshing }) {
  const [isExpanded, setIsExpanded] = useState(true);

  const statusColor = {
    HEALTHY: 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/50',
    DEGRADED: 'bg-yellow-500/20 text-yellow-400 border border-yellow-500/50',
    OFFLINE: 'bg-red-500/20 text-red-400 border border-red-500/50',
  }[data.status] || 'bg-slate-500/20 text-slate-400';

  const formatLatency = (ms) => {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}min`;
  };

  return (
    <div className="bg-slate-900/50 rounded-xl border border-slate-800 p-4 flex-1 flex flex-col gap-3">
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div>
          <h4 className="text-sm font-semibold text-slate-100">{layer}</h4>
          <p className="text-xs text-slate-500">{data.technology}</p>
        </div>
        <div className={`px-2 py-1 rounded-lg text-xs font-medium whitespace-nowrap ${statusColor}`}>
          {data.status}
        </div>
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="bg-slate-950 rounded-lg p-2">
          <p className="text-slate-500">Records</p>
          <p className="text-sm font-mono text-slate-200">
            {data.recordCount.toLocaleString()}
          </p>
        </div>
        <div className="bg-slate-950 rounded-lg p-2">
          <p className="text-slate-500">Latency</p>
          <p className="text-sm font-mono text-slate-200">
            {formatLatency(data.latencyMs)}
          </p>
        </div>
        <div className="col-span-2 bg-slate-950 rounded-lg p-2">
          <p className="text-slate-500">Retention</p>
          <p className="text-sm font-mono text-slate-200">{data.retention}</p>
        </div>
      </div>

      {/* Tables Summary */}
      <div className="text-xs">
        <p className="text-slate-500 mb-1">Tables</p>
        <div className="space-y-1">
          {(Array.isArray(data.tables) ? data.tables : [data.table]).map((table) => (
            <div key={table} className="bg-slate-950 rounded px-2 py-1">
              <p className="text-slate-300 font-mono text-[10px]">{table}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Sample Data */}
      <div>
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="w-full flex items-center gap-2 text-xs font-medium text-slate-400 hover:text-slate-300 transition-colors mb-2"
        >
          {isExpanded ? (
            <ChevronUp size={14} />
          ) : (
            <ChevronDown size={14} />
          )}
          Sample Data ({data.sampleData.length} rows)
        </button>

        {isExpanded && (
          <div className="bg-slate-950 rounded-lg overflow-hidden">
            <div className="overflow-x-auto max-h-48 overflow-y-auto">
              <table className="w-full text-[10px]">
                <thead className="sticky top-0 bg-slate-900 border-b border-slate-800">
                  <tr>
                    {data.sampleData[0] &&
                      Object.keys(data.sampleData[0]).map((key) => (
                        <th
                          key={key}
                          className="px-2 py-1 text-left text-slate-400 font-medium whitespace-nowrap"
                        >
                          {key}
                        </th>
                      ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {data.sampleData.map((row, idx) => (
                    <tr key={idx} className="hover:bg-slate-800/50">
                      {Object.values(row).map((val, vidx) => (
                        <td
                          key={vidx}
                          className="px-2 py-1 text-slate-300 font-mono whitespace-nowrap overflow-hidden text-ellipsis max-w-[80px]"
                          title={String(val)}
                        >
                          {typeof val === 'number' && val > 1000000
                            ? val.toLocaleString()
                            : String(val)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>

      {/* Last Updated */}
      <div className="text-[10px] text-slate-500">
        Updated: {data.lastUpdated}
      </div>
    </div>
  );
}

// Mock data generator
const generateMockData = () => {
  const now = new Date();
  return {
    hot: {
      status: 'HEALTHY',
      technology: 'Apache Fluss',
      table: 'hot_violence_alerts',
      recordCount: Math.floor(Math.random() * 5000) + 500,
      latencyMs: Math.floor(Math.random() * 100) + 20,
      retention: '1-2 hours',
      tables: ['hot_violence_alerts'],
      sampleData: [
        {
          incident_id: 'inc_0001a2',
          camera_id: 'cam_04',
          risk_score: '0.87',
          timestamp: '2026-05-02 10:34:22',
        },
        {
          incident_id: 'inc_0001b3',
          camera_id: 'cam_07',
          risk_score: '0.92',
          timestamp: '2026-05-02 10:33:45',
        },
        {
          incident_id: 'inc_0001c4',
          camera_id: 'cam_12',
          risk_score: '0.71',
          timestamp: '2026-05-02 10:33:18',
        },
        {
          incident_id: 'inc_0001d5',
          camera_id: 'cam_09',
          risk_score: '0.64',
          timestamp: '2026-05-02 10:32:52',
        },
        {
          incident_id: 'inc_0001e6',
          camera_id: 'cam_15',
          risk_score: '0.79',
          timestamp: '2026-05-02 10:32:11',
        },
      ],
      lastUpdated: now.toLocaleTimeString(),
    },
    warm: {
      status: 'HEALTHY',
      technology: 'Apache Paimon',
      recordCount: 103956,
      latencyMs: 180000,
      retention: '7-30 days',
      tables: [
        'violence_incidents',
        'daily_incident_stats',
        'camera_stats',
      ],
      sampleData: [
        {
          incident_id: 'inc_000234',
          camera_id: 'cam_04',
          location: 'Front Door',
          is_violent: true,
        },
        {
          incident_id: 'inc_000245',
          camera_id: 'cam_07',
          location: 'Lobby',
          is_violent: true,
        },
        {
          incident_id: 'inc_000267',
          camera_id: 'cam_12',
          location: 'Building Top',
          is_violent: false,
        },
        {
          incident_id: 'inc_000278',
          camera_id: 'cam_09',
          location: 'East Stairs',
          is_violent: true,
        },
        {
          incident_id: 'inc_000289',
          camera_id: 'cam_01',
          location: 'Main Gate',
          is_violent: false,
        },
      ],
      lastUpdated: now.toLocaleTimeString(),
    },
    cold: {
      status: 'HEALTHY',
      technology: 'Apache Iceberg',
      table: 'historical_violence_incidents',
      recordCount: Math.floor(Math.random() * 100) + 10,
      latencyMs: Math.floor(Math.random() * 5000) + 2700,
      retention: 'Years (archival)',
      tables: ['historical_violence_incidents'],
      sampleData: [
        {
          incident_id: 'arc_001234',
          camera_id: 'cam_04',
          date: '2026-04-28',
          count: 42,
        },
        {
          incident_id: 'arc_001245',
          camera_id: 'cam_07',
          date: '2026-04-27',
          count: 38,
        },
        {
          incident_id: 'arc_001267',
          camera_id: 'cam_12',
          date: '2026-04-26',
          count: 29,
        },
        {
          incident_id: 'arc_001278',
          camera_id: 'cam_09',
          date: '2026-04-25',
          count: 35,
        },
        {
          incident_id: 'arc_001289',
          camera_id: 'cam_01',
          date: '2026-04-24',
          count: 41,
        },
      ],
      lastUpdated: now.toLocaleTimeString(),
    },
  };
};

export default function DataLayerStatusPanel() {
  const [data, setData] = useState(generateMockData());
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Simulate polling for new data every 5 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      setData(generateMockData());
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleRefresh = () => {
    setIsRefreshing(true);
    setData(generateMockData());
    setTimeout(() => setIsRefreshing(false), 300);
  };

  return (
    <div className="flex flex-col h-full gap-4 p-4 overflow-y-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-slate-100">Data Layers</h3>
        <button
          onClick={handleRefresh}
          disabled={isRefreshing}
          className="p-1 hover:bg-slate-800 rounded-lg transition-colors disabled:opacity-50"
          title="Refresh data"
        >
          <RefreshCw
            size={16}
            className={`text-slate-400 hover:text-slate-300 ${isRefreshing ? 'animate-spin' : ''}`}
          />
        </button>
      </div>

      {/* Layer Columns */}
      <div className="flex flex-col gap-3">
        <LayerColumn
          layer="🔥 HOT"
          data={data.hot}
          isRefreshing={isRefreshing}
        />
        <LayerColumn
          layer="🌊 WARM"
          data={data.warm}
          isRefreshing={isRefreshing}
        />
        <LayerColumn
          layer="🧊 COLD"
          data={data.cold}
          isRefreshing={isRefreshing}
        />
      </div>

      {/* Data Flow Diagram */}
      <div className="bg-slate-950/50 rounded-lg border border-slate-800 p-3 text-xs">
        <p className="text-slate-500 mb-2 font-semibold">Data Flow</p>
        <div className="space-y-1 text-slate-400 font-mono text-[10px]">
          <p>Kafka ↓</p>
          <p>Flink Validator ↓</p>
          <p>├─ Fluss (HOT) [&lt;100ms]</p>
          <p>├─ Paimon (WARM) [1-10min]</p>
          <p>└─ Iceberg (COLD) [archive]</p>
        </div>
      </div>
    </div>
  );
}
