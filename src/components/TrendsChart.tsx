import React from 'react';
import {
  ResponsiveContainer,
  ComposedChart,
  Line,
  Area,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  BarChart
} from 'recharts';
import type { TrendData } from '../lib/queries';
import styles from './Charts.module.css';

interface TrendsChartProps {
  data: TrendData[];
  unit: string;
}

export const TrendsChart: React.FC<TrendsChartProps> = ({ data, unit }) => {
  if (!data || data.length === 0) {
    return <div className={styles.empty}>No data found for this selection.</div>;
  }

  const formatXAxis = (tickItem: string) => {
    // Expected format: YYYYqQ (e.g., 2024q1)
    if (tickItem.toLowerCase().endsWith('q1')) {
      return tickItem.toUpperCase().replace('Q', ' Q');
    }
    return '';
  };

  const chartData = data.map(d => ({
    ...d,
    priceRange: [d.p25_price, d.p75_price]
  }));

  return (
    <div className={styles.chartsGrid}>
      <div className={styles.chartWrapper}>
        <h3>Median Price ({unit})</h3>
        <ResponsiveContainer width="100%" height={350}>
          <ComposedChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 20 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
            <XAxis 
              dataKey="year_quarter" 
              fontSize={11} 
              tick={{ fill: '#64748b' }} 
              axisLine={{ stroke: '#e2e8f0' }}
              tickFormatter={formatXAxis}
              interval={0}
            />
            <YAxis 
              fontSize={12} 
              tick={{ fill: '#64748b' }} 
              axisLine={{ stroke: '#e2e8f0' }}
              tickFormatter={(value) => `$${value.toFixed(2)}`}
            />
            <Tooltip 
              contentStyle={{ backgroundColor: '#fff', borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
              formatter={(value: any, name: any) => {
                const nameStr = String(name);
                if (value === null || value === undefined) return ['N/A', nameStr];
                if (nameStr.includes('Price') || nameStr.includes('IQR')) {
                  if (Array.isArray(value)) {
                    return [`$${Number(value[0]).toFixed(2)} - $${Number(value[1]).toFixed(2)}`, nameStr];
                  }
                  return [`$${Number(value).toFixed(2)}`, nameStr];
                }
                return [value.toLocaleString(), nameStr];
              }}
            />
            <Legend verticalAlign="top" height={36}/>
            <Area
              type="monotone"
              dataKey="priceRange"
              stroke="none"
              fill="#00b0f6"
              fillOpacity={0.15}
              name="IQR (P25-P75 Range)"
              connectNulls
            />
            <Line
              type="monotone"
              dataKey="median_price"
              stroke="#00b0f6"
              strokeWidth={3}
              dot={{ r: 4, fill: '#00b0f6', strokeWidth: 2, stroke: '#fff' }}
              activeDot={{ r: 6 }}
              name="Median Price"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <div className={styles.chartWrapper}>
        <h3>Contract Volume</h3>
        <ResponsiveContainer width="100%" height={250}>
          <BarChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 20 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
            <XAxis 
              dataKey="year_quarter" 
              fontSize={11} 
              tick={{ fill: '#64748b' }} 
              axisLine={{ stroke: '#e2e8f0' }}
              tickFormatter={formatXAxis}
              interval={0}
            />
            <YAxis 
              fontSize={12} 
              tick={{ fill: '#64748b' }} 
              axisLine={{ stroke: '#e2e8f0' }}
            />
            <Tooltip 
               contentStyle={{ backgroundColor: '#fff', borderRadius: '8px', border: '1px solid #e2e8f0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
               formatter={(value: any, name: any) => {
                 if (value === null || value === undefined) return ['N/A', String(name)];
                 return [Number(value).toLocaleString(), String(name)];
               }}
            />
            <Bar 
              dataKey="contract_count" 
              fill="#00b0f6" 
              fillOpacity={0.6}
              radius={[4, 4, 0, 0]} 
              name="Contract Count"
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};
