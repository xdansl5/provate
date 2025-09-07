import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TrendingUp, TrendingDown, Loader2, Wifi, WifiOff } from "lucide-react";
import { useData } from "@/contexts/DataContext";

const MetricsGrid = () => {
  const { metrics, sseConnected, loading, realTimeMetrics } = useData();

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success": return "text-success";
      case "warning": return "text-warning";
      case "error": return "text-destructive";
      default: return "text-primary";
    }
  };

  const getTrendColor = (trend: string) => {
    return trend === "up" ? "text-success" : "text-destructive";
  };

  if (loading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {Array.from({ length: 6 }).map((_, index) => (
          <Card key={index} className="border-border/50 bg-card/50 backdrop-blur">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Loading...
              </CardTitle>
              <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-muted-foreground mb-1">
                --
              </div>
              <div className="flex items-center text-sm text-muted-foreground">
                <span>Connecting to pipeline...</span>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  if (!sseConnected) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {Array.from({ length: 6 }).map((_, index) => (
          <Card key={index} className="border-border/50 bg-card/50 backdrop-blur">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {metrics[index]?.title || 'Metric'}
              </CardTitle>
              <WifiOff className="h-4 w-4 text-red-500" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-muted-foreground mb-1">
                --
              </div>
              <div className="flex items-center text-sm text-red-500">
                <WifiOff className="h-3 w-3 mr-1" />
                <span>Pipeline disconnected</span>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {metrics.map((metric, index) => (
        <Card key={index} className="group hover:shadow-glow transition-all duration-300 border-border/50 bg-card/50 backdrop-blur">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              {metric.title}
            </CardTitle>
            <div className="flex items-center space-x-1">
              <div className={getStatusColor(metric.status)}>
                {metric.icon}
              </div>
              {sseConnected && (
                <Wifi className="h-3 w-3 text-green-500" />
              )}
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-foreground mb-1">
              {metric.value}
            </div>
            <div className="flex items-center text-sm">
              {metric.trend === "up" ? (
                <TrendingUp className="h-3 w-3 mr-1 text-success" />
              ) : (
                <TrendingDown className="h-3 w-3 mr-1 text-destructive" />
              )}
              <span className={getTrendColor(metric.trend)}>
                {metric.change}
              </span>
              <span className="text-muted-foreground ml-1">from last hour</span>
            </div>
            {realTimeMetrics && (
              <div className="text-xs text-muted-foreground mt-1">
                Last update: {new Date(realTimeMetrics.lastUpdate).toLocaleTimeString()}
              </div>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  );
};

export default MetricsGrid;