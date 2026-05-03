import { useState, useEffect } from 'react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { TrendingUp } from 'lucide-react';

// Generate mock metrics
const generateMetrics = () => {
  const now = new Date();
  const timeChart = [];
  for (let i = 29; i >= 0; i--) {
    const time = new Date(now.getTime() - i * 60000);
    timeChart.push({
      time: time.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      }),
      records: Math.floor(Math.random() * 2000) + 500,
    });
  }

  const throughputChart = [];
  const intervals = ['10:00', '10:05', '10:10', '10:15', '10:20', '10:25', '10:30'];
  for (const interval of intervals) {
    throughputChart.push({
      time: interval,
      validator: Math.floor(Math.random() * 500) + 200,
      kafka_fluss: Math.floor(Math.random() * 400) + 150,
      kafka_paimon: Math.floor(Math.random() * 350) + 100,
    });
  }

  return {
    kafkaLag: Math.floor(Math.random() * 100) + 10,
    flinkThroughput: Math.floor(Math.random() * 1000) + 500,
    dataCompleteness: (Math.random() * 5 + 95).toFixed(1),
    queryLatency: {
      p50: Math.floor(Math.random() * 500) + 100,
      p95: Math.floor(Math.random() * 2000) + 500,
    },
    timeChart,
    throughputChart,
  };
};

function MetricCard({ title, value, unit, trend, color = 'emerald' }) {
  const bgColor = {
    emerald: 'bg-emerald-500/10 border-emerald-500/30',
    yellow: 'bg-yellow-500/10 border-yellow-500/30',
    blue: 'bg-blue-500/10 border-blue-500/30',
  }[color];

  const textColor = {
    emerald: 'text-emerald-400',
    yellow: 'text-yellow-400',
    blue: 'text-blue-400',
  }[color];

  const trendColor = trend >= 0 ? 'text-emerald-400' : 'text-red-400';

  return (
    <div className={`rounded-lg border ${bgColor} p-3`}>
      <p className="text-xs text-slate-500 mb-1">{title}</p>
      <div className="flex items-baseline justify-between gap-2">
        <p className={`text-2xl font-bold ${textColor}`}>
          {typeof value === 'number' ? value.toLocaleString() : value}
        </p>
        <p className="text-xs text-slate-500">{unit}</p>
      </div>
      {trend !== undefined && (
        <div className={`text-xs ${trendColor} mt-1 flex items-center gap-1`}>
          <TrendingUp size={12} />
          {trend >= 0 ? '+' : ''}{trend}%
        </div>
      )}
    </div>
  );
}

export default function DataMetricsPanel() {
  const [metrics, setMetrics] = useState(generateMetrics());
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Simulate polling for new metrics every 5 seconds
  useEffect(() => {
    if (!autoRefresh) return;

    const interval = setInterval(() => {
      setMetrics(generateMetrics());
    }, 5000);

    return () => clearInterval(interval);
  }, [autoRefresh]);

  return (
    <div className="flex flex-col h-full gap-4 p-4 overflow-y-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-slate-100">Metrics</h3>
        <button
          onClick={() => setAutoRefresh(!autoRefresh)}
          className={`text-xs px-2 py-1 rounded-lg transition-colors ${
            autoRefresh
              ? 'bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30'
              : 'bg-slate-800 text-slate-400 hover:bg-slate-700'
          }`}
        >
          {autoRefresh ? 'Auto' : 'Paused'}
        </button>
      </div>

      {/* Metric Cards Grid */}
      <div className="grid grid-cols-2 gap-2">
        <MetricCard
          title="Kafka Lag"
          value={metrics.kafkaLag}
          unit="msgs"
          trend={Math.random() * 20 - 10}
          color="emerald"
        />
        <MetricCard
          title="Flink Throughput"
          value={metrics.flinkThroughput}
          unit="msgs/s"
          trend={Math.random() * 30 - 5}
          color="blue"
        />
        <MetricCard
          title="Data Completeness"
          value={metrics.dataCompleteness}
          unit="%"
          trend={Math.random() * 2}
          color="emerald"
        />
        <MetricCard
          title="Query Latency (P50)"
          value={metrics.queryLatency.p50}
          unit="ms"
          trend={Math.random() * 50 - 25}
          color="yellow"
        />
      </div>

      {/* Charts */}
      <div className="space-y-3">
        {/* Record Count Chart */}
        <div className="bg-slate-900/50 rounded-lg border border-slate-800 p-3">
          <p className="text-xs font-semibold text-slate-300 mb-2">
            Record Count (30-min rolling)
          </p>
          <ResponsiveContainer width="100%" height={120}>
            <LineChart data={metrics.timeChart}>
              <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 10 }}
                stroke="#64748b"
              />
              <YAxis
                tick={{ fontSize: 10 }}
                stroke="#64748b"
                width={35}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1e293b',
                  border: '1px solid #475569',
                  borderRadius: '6px',
                }}
                labelStyle={{ color: '#f1f5f9' }}
              />
              <Line
                type="monotone"
                dataKey="records"
                stroke="#10b981"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Throughput Chart */}
        <div className="bg-slate-900/50 rounded-lg border border-slate-800 p-3">
          <p className="text-xs font-semibold text-slate-300 mb-2">
            Flink Throughput (5-min buckets)
          </p>
          <ResponsiveContainer width="100%" height={120}>
            <BarChart data={metrics.throughputChart}>
              <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 10 }}
                stroke="#64748b"
              />
              <YAxis
                tick={{ fontSize: 10 }}
                stroke="#64748b"
                width={35}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1e293b',
                  border: '1px solid #475569',
                  borderRadius: '6px',
                }}
                labelStyle={{ color: '#f1f5f9' }}
              />
              <Bar
                dataKey="validator"
                fill="#3b82f6"
                radius={[4, 4, 0, 0]}
              />
              <Bar
                dataKey="kafka_fluss"
                fill="#8b5cf6"
                radius={[4, 4, 0, 0]}
              />
              <Bar
                dataKey="kafka_paimon"
                fill="#ec4899"
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Legend */}
      <div className="bg-slate-950/50 rounded-lg border border-slate-800 p-2 text-[10px] space-y-1">
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded-sm bg-blue-500" />
          <span className="text-slate-400">Validator</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded-sm bg-purple-500" />
          <span className="text-slate-400">Kafka → Fluss</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded-sm bg-pink-500" />
          <span className="text-slate-400">Kafka → Paimon</span>
        </div>
      </div>

      {/* Footer */}
      <div className="text-[10px] text-slate-500 mt-auto">
        Auto-refreshing every 5s • Data for visualization only
      </div>
    </div>
  );
}
