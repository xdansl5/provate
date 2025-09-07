import React, { createContext, useContext, useEffect, useState, useCallback } from 'react';

export interface LogEntry {
  id: string;
  timestamp: string;
  level: "INFO" | "WARN" | "ERROR" | "DEBUG";
  source: string;
  message: string;
  ip?: string;
  status?: number;
  responseTime?: number;
  endpoint?: string;
  userId?: string;
  sessionId?: string;
}

export interface Metric {
  title: string;
  value: string;
  change: string;
  trend: "up" | "down";
  icon: React.ReactNode;
  status: "success" | "warning" | "error" | "info";
}

export interface ChartData {
  time: string;
  requests: number;
  errors: number;
  responseTime: number;
}

export interface QueryResult {
  [key: string]: any;
}

interface RealTimeMetrics {
  eventsPerSec: number;
  errorRate: number;
  avgResponseTime: number;
  activeSessions: number;
  dataProcessed: number;
  totalRequests: number;
  totalErrors: number;
  lastUpdate: string;
}

interface DataContextType {
  logs: LogEntry[];
  metrics: Metric[];
  chartData: ChartData[];
  isStreaming: boolean;
  setIsStreaming: (streaming: boolean) => void;
  executeQuery: (query: string) => Promise<{ results: QueryResult[]; executionTime: string }>;
  clearLogs: () => void;
  getAnomalies: () => LogEntry[];
  sseConnected: boolean;
  realTimeMetrics: RealTimeMetrics | null;
  loading: boolean;
}

const DataContext = createContext<DataContextType | undefined>(undefined);

export const useData = () => {
  const context = useContext(DataContext);
  if (!context) {
    throw new Error('useData must be used within a DataProvider');
  }
  return context;
};

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:4000';

export const DataProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [isStreaming, setIsStreaming] = useState(true);
  const [sseConnected, setSseConnected] = useState(false);
  const [realTimeMetrics, setRealTimeMetrics] = useState<RealTimeMetrics | null>(null);
  const [loading, setLoading] = useState(true);

  // Fetch initial data
  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        setLoading(true);
        
        // Fetch recent logs
        const logsResponse = await fetch(`${API_BASE_URL}/api/logs?limit=100`);
        if (logsResponse.ok) {
          const recentLogs = await logsResponse.json();
          setLogs(recentLogs);
        }
        
        // Fetch current metrics
        const metricsResponse = await fetch(`${API_BASE_URL}/api/metrics`);
        if (metricsResponse.ok) {
          const metrics = await metricsResponse.json();
          setRealTimeMetrics(metrics);
        }
        
        // Initialize chart data from recent logs
        const hourlyData = new Map<string, { requests: number; errors: number; totalResponseTime: number }>();
        
        logs.forEach(log => {
          const timestamp = new Date(log.timestamp);
          const hour = timestamp.toISOString().substring(0, 16).replace('T', ' ');
          
          if (!hourlyData.has(hour)) {
            hourlyData.set(hour, { requests: 0, errors: 0, totalResponseTime: 0 });
          }
          
          const data = hourlyData.get(hour)!;
          data.requests++;
          if (log.level === "ERROR") {
            data.errors++;
          }
          data.totalResponseTime += log.responseTime || 0;
        });
        
        const chartDataArray = Array.from(hourlyData.entries()).slice(-24).map(([hour, data]) => ({
          time: hour.substring(11), // Just the time part
          requests: data.requests,
          errors: data.errors,
          responseTime: Math.round(data.totalResponseTime / data.requests)
        }));
        
        setChartData(chartDataArray);
        
      } catch (error) {
        console.error('Failed to fetch initial data:', error);
      } finally {
        setLoading(false);
      }
    };
    
    fetchInitialData();
  }, []);

  // Connect to SSE stream for real-time updates
  useEffect(() => {
    if (!isStreaming) return;

    const sseUrl = `${API_BASE_URL}/events`;
    let es: EventSource | null = null;

    try {
      es = new EventSource(sseUrl, { withCredentials: false });
    } catch (err) {
      console.error('Failed to create EventSource:', err);
      setSseConnected(false);
      return;
    }

    es.onopen = () => {
      console.log('SSE connection established');
      setSseConnected(true);
    };

    es.onmessage = (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data);
        
        if (data.type === 'hello') {
          return; // Ignore hello messages
        }
        
        if (data.type === 'metrics') {
          // Update real-time metrics
          setRealTimeMetrics(data.data);
          
          // Update chart data with new metrics
          setChartData(prev => {
            const newEntry: ChartData = {
              time: new Date().toLocaleTimeString('en-US', { 
                hour12: false, 
                hour: '2-digit', 
                minute: '2-digit' 
              }),
              requests: Math.round(data.data.eventsPerSec * 60), // Convert to requests per hour
              errors: Math.round((data.data.errorRate / 100) * data.data.eventsPerSec * 60),
              responseTime: data.data.avgResponseTime
            };
            
            return [...prev.slice(1), newEntry];
          });
        } else {
          // New log entry
          setLogs(prev => [data as LogEntry, ...prev.slice(0, 999)]);
        }
      } catch (e) {
        console.error('Failed to parse SSE message:', e);
      }
    };

    es.onerror = (error) => {
      console.error('SSE connection error:', error);
      setSseConnected(false);
      if (es) {
        try { es.close(); } catch {}
      }
    };

    return () => {
      setSseConnected(false);
      if (es) {
        try { es.close(); } catch {}
      }
    };
  }, [isStreaming]);

  // Execute real Spark SQL queries
  const executeQuery = useCallback(async (query: string): Promise<{ results: QueryResult[]; executionTime: string }> => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      if (!response.ok) {
        throw new Error(`Query failed: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      console.error('Query execution error:', error);
      throw error;
    }
  }, []);

  const clearLogs = useCallback(() => {
    setLogs([]);
  }, []);

  const getAnomalies = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/anomalies`);
      if (response.ok) {
        return await response.json();
      }
      return [];
    } catch (error) {
      console.error('Failed to fetch anomalies:', error);
      return [];
    }
  }, []);

  // Generate metrics based on real-time data
  const metrics: Metric[] = realTimeMetrics ? [
    {
      title: "Events/sec",
      value: realTimeMetrics.eventsPerSec.toFixed(1),
      change: "+5.2%",
      trend: "up",
      icon: <div>📊</div>,
      status: "success"
    },
    {
      title: "Error Rate",
      value: `${realTimeMetrics.errorRate.toFixed(2)}%`,
      change: realTimeMetrics.errorRate > 2 ? "+2.1%" : "-1.3%",
      trend: realTimeMetrics.errorRate > 2 ? "up" : "down",
      icon: <div>⚠️</div>,
      status: realTimeMetrics.errorRate > 5 ? "error" : realTimeMetrics.errorRate > 2 ? "warning" : "success"
    },
    {
      title: "Avg Response Time",
      value: `${realTimeMetrics.avgResponseTime}ms`,
      change: realTimeMetrics.avgResponseTime > 150 ? "+3.2%" : "-1.8%",
      trend: realTimeMetrics.avgResponseTime > 150 ? "up" : "down",
      icon: <div>⏱️</div>,
      status: realTimeMetrics.avgResponseTime > 200 ? "warning" : "success"
    },
    {
      title: "Active Sessions",
      value: realTimeMetrics.activeSessions.toLocaleString(),
      change: "+12.5%",
      trend: "up",
      icon: <div>👥</div>,
      status: "info"
    },
    {
      title: "Data Processed",
      value: `${realTimeMetrics.dataProcessed.toFixed(1)} GB`,
      change: "+8.7%",
      trend: "up",
      icon: <div>💾</div>,
      status: "success"
    },
    {
      title: "Total Requests",
      value: realTimeMetrics.totalRequests.toLocaleString(),
      change: "+15.3%",
      trend: "up",
      icon: <div>🔄</div>,
      status: "info"
    }
  ] : [];

  return (
    <DataContext.Provider value={{
      logs,
      metrics,
      chartData,
      isStreaming,
      setIsStreaming,
      executeQuery,
      clearLogs,
      getAnomalies,
      sseConnected,
      realTimeMetrics,
      loading
    }}>
      {children}
    </DataContext.Provider>
  );
};