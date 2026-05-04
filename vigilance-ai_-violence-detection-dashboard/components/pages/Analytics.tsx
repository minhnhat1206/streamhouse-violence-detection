import React, { useState, useEffect } from 'react';
import { AreaChart, Area, BarChart, Bar, Cell, CartesianGrid, Legend, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { getAnalyticsData } from '../../services/mockData';
import { fetchStats } from '../../services/backendService';
import { AnalyticsData } from '../../types';

const ChartCard: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
    <div className="bg-slate-900/50 p-6 rounded-xl border border-slate-800 shadow-lg">
        <h3 className="text-lg font-semibold text-white mb-4">{title}</h3>
        <div className="h-72">
            {children}
        </div>
    </div>
);

const Analytics: React.FC = () => {
    const [data, setData] = useState<AnalyticsData>(getAnalyticsData());

    useEffect(() => {
        let cancelled = false;

        async function load() {
            try {
                const realData = await fetchStats();
                if (!cancelled) setData(realData);
            } catch {
                // Backend unavailable — keep existing mock data, refresh silently
            }
        }

        load();
        const interval = setInterval(load, 60_000);
        return () => {
            cancelled = true;
            clearInterval(interval);
        };
    }, []);

    const PIE_COLORS = ['#10b981', '#f59e0b', '#3b82f6', '#ef4444', '#8b5cf6'];
    
    const tooltipStyle = {
      backgroundColor: 'rgb(30 41 59 / 1)', 
      border: '1px solid rgb(51 65 85 / 1)',
    };
    const itemStyle = { color: '#cbd5e1' };

    return (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="lg:col-span-2">
                <ChartCard title="Alerts per Hour (Last 24h)">
                    <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={data.alertsPerHour} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
                            <defs>
                                <linearGradient id="colorAlerts" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#10b981" stopOpacity={0.8}/>
                                <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.3)" />
                            <XAxis dataKey="name" stroke="#94a3b8" fontSize={12} />
                            <YAxis stroke="#94a3b8" fontSize={12} allowDecimals={false} />
                            <Tooltip contentStyle={tooltipStyle} itemStyle={itemStyle} />
                            <Area type="monotone" dataKey="alerts" stroke="#10b981" fillOpacity={1} fill="url(#colorAlerts)" />
                        </AreaChart>
                    </ResponsiveContainer>
                </ChartCard>
            </div>
            
            <ChartCard title="Top 5 Locations by Alert Count">
                <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={data.topLocations} layout="vertical" margin={{ top: 5, right: 20, left: 30, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.3)" />
                        <XAxis type="number" stroke="#94a3b8" fontSize={12} allowDecimals={false} />
                        <YAxis type="category" dataKey="name" stroke="#94a3b8" fontSize={12} width={120} tick={{ fill: '#e2e8f0' }} />
                        <Tooltip contentStyle={tooltipStyle} cursor={{fill: 'rgba(51, 65, 85, 0.5)'}} itemStyle={itemStyle}/>
                        <Bar dataKey="alerts" fill="#10b981" />
                    </BarChart>
                </ResponsiveContainer>
            </ChartCard>
            
            <ChartCard title="Distribution by Alert Type">
                <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                        <Pie data={data.alertTypes} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} fill="#8884d8" label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                            {data.alertTypes.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={PIE_COLORS[index % PIE_COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip contentStyle={tooltipStyle} itemStyle={itemStyle} />
                        <Legend />
                    </PieChart>
                </ResponsiveContainer>
            </ChartCard>
            
            <div className="lg:col-span-2">
                 <ChartCard title="Average Violence Score (Last 7 Days)">
                    <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={data.avgScore} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.3)" />
                            <XAxis dataKey="name" stroke="#94a3b8" fontSize={12} />
                            <YAxis domain={[0.5, 1]} stroke="#94a3b8" fontSize={12} />
                            <Tooltip contentStyle={tooltipStyle} itemStyle={itemStyle} />
                            <Legend />
                            <Line type="monotone" dataKey="score" stroke="#ef4444" strokeWidth={2} activeDot={{ r: 8 }} />
                        </LineChart>
                    </ResponsiveContainer>
                </ChartCard>
            </div>
        </div>
    );
};

export default Analytics;
