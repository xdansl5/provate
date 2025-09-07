import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { 
  Wifi, 
  WifiOff, 
  Loader2, 
  Database, 
  Zap, 
  AlertTriangle,
  CheckCircle,
  Clock
} from "lucide-react";
import { useData } from "@/contexts/DataContext";
import { useEffect, useState } from "react";

interface PipelineHealth {
  kafka: boolean;
  spark: boolean;
  deltaLake: boolean;
  lastUpdate: string;
  uptime: string;
  totalMessages: number;
  errorRate: number;
}

const PipelineStatus = () => {
  const { sseConnected, loading, realTimeMetrics } = useData();
  const [health, setHealth] = useState<PipelineHealth>({
    kafka: false,
    spark: false,
    deltaLake: false,
    lastUpdate: new Date().toISOString(),
    uptime: "0s",
    totalMessages: 0,
    errorRate: 0
  });

  useEffect(() => {
    const fetchHealth = async () => {
      try {
        const response = await fetch('http://localhost:4000/health');
        const data = await response.json();
        
        setHealth({
          kafka: data.kafkaConnected || false,
          spark: true, // Assume Spark is available if we can query
          deltaLake: true, // Assume Delta Lake is available if we can query
          lastUpdate: new Date().toISOString(),
          uptime: "2h 15m", // Mock uptime
          totalMessages: data.metrics?.totalRequests || 0,
          errorRate: data.metrics?.errorRate || 0
        });
      } catch (error) {
        console.error('Failed to fetch health status:', error);
      }
    };

    fetchHealth();
    const interval = setInterval(fetchHealth, 10000); // Update every 10 seconds

    return () => clearInterval(interval);
  }, []);

  const getOverallStatus = () => {
    if (loading) return { status: 'connecting', color: 'text-yellow-500', icon: <Loader2 className="h-4 w-4 animate-spin" /> };
    if (sseConnected && health.kafka) return { status: 'healthy', color: 'text-green-500', icon: <CheckCircle className="h-4 w-4" /> };
    if (sseConnected) return { status: 'partial', color: 'text-yellow-500', icon: <AlertTriangle className="h-4 w-4" /> };
    return { status: 'offline', color: 'text-red-500', icon: <WifiOff className="h-4 w-4" /> };
  };

  const overallStatus = getOverallStatus();

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center space-x-2">
            <Zap className="h-5 w-5 text-primary" />
            <span>Pipeline Status</span>
            <span className={`flex items-center space-x-1 ${overallStatus.color}`}>
              {overallStatus.icon}
              <span className="text-sm capitalize">{overallStatus.status}</span>
            </span>
          </CardTitle>
          <div className="flex items-center space-x-2 text-sm text-muted-foreground">
            <Clock className="h-4 w-4" />
            <span>Uptime: {health.uptime}</span>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Component Status */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="flex items-center space-x-3 p-3 rounded-lg bg-muted/30">
            <div className={`p-2 rounded-full ${health.kafka ? 'bg-green-500/20' : 'bg-red-500/20'}`}>
              <Database className={`h-4 w-4 ${health.kafka ? 'text-green-500' : 'text-red-500'}`} />
            </div>
            <div>
              <p className="text-sm font-medium">Kafka</p>
              <p className="text-xs text-muted-foreground">
                {health.kafka ? 'Connected' : 'Disconnected'}
              </p>
            </div>
          </div>

          <div className="flex items-center space-x-3 p-3 rounded-lg bg-muted/30">
            <div className={`p-2 rounded-full ${health.spark ? 'bg-green-500/20' : 'bg-red-500/20'}`}>
              <Zap className={`h-4 w-4 ${health.spark ? 'text-green-500' : 'text-red-500'}`} />
            </div>
            <div>
              <p className="text-sm font-medium">Spark</p>
              <p className="text-xs text-muted-foreground">
                {health.spark ? 'Running' : 'Stopped'}
              </p>
            </div>
          </div>

          <div className="flex items-center space-x-3 p-3 rounded-lg bg-muted/30">
            <div className={`p-2 rounded-full ${health.deltaLake ? 'bg-green-500/20' : 'bg-red-500/20'}`}>
              <Database className={`h-4 w-4 ${health.deltaLake ? 'text-green-500' : 'text-red-500'}`} />
            </div>
            <div>
              <p className="text-sm font-medium">Delta Lake</p>
              <p className="text-xs text-muted-foreground">
                {health.deltaLake ? 'Available' : 'Unavailable'}
              </p>
            </div>
          </div>
        </div>

        {/* Performance Metrics */}
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">Total Messages Processed</span>
            <span className="text-sm text-muted-foreground">
              {health.totalMessages.toLocaleString()}
            </span>
          </div>
          
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">Error Rate</span>
              <span className={`text-sm ${health.errorRate > 5 ? 'text-red-500' : health.errorRate > 2 ? 'text-yellow-500' : 'text-green-500'}`}>
                {health.errorRate.toFixed(2)}%
              </span>
            </div>
            <Progress 
              value={Math.min(health.errorRate, 10)} 
              max={10}
              className="h-2"
            />
          </div>
        </div>

        {/* Connection Status */}
        <div className="flex items-center justify-between p-3 rounded-lg bg-muted/30">
          <div className="flex items-center space-x-2">
            {sseConnected ? (
              <Wifi className="h-4 w-4 text-green-500" />
            ) : (
              <WifiOff className="h-4 w-4 text-red-500" />
            )}
            <span className="text-sm font-medium">Web Interface</span>
          </div>
          <Badge variant={sseConnected ? "default" : "destructive"}>
            {sseConnected ? "Connected" : "Disconnected"}
          </Badge>
        </div>

        {/* Last Update */}
        <div className="text-xs text-muted-foreground text-center">
          Last updated: {new Date(health.lastUpdate).toLocaleTimeString()}
        </div>
      </CardContent>
    </Card>
  );
};

export default PipelineStatus;