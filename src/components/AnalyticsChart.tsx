import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from "recharts";
import { Loader2, Wifi, WifiOff } from "lucide-react";
import { useData } from "@/contexts/DataContext";

const AnalyticsChart = () => {
  const { chartData, sseConnected, loading } = useData();

  const getConnectionStatus = () => {
    if (loading) {
      return { icon: <Loader2 className="h-4 w-4 animate-spin" />, text: "Loading data...", color: "text-muted-foreground" };
    }
    if (sseConnected) {
      return { icon: <Wifi className="h-4 w-4" />, text: "Live data", color: "text-green-500" };
    }
    return { icon: <WifiOff className="h-4 w-4" />, text: "No data", color: "text-red-500" };
  };

  const connectionStatus = getConnectionStatus();

  if (loading) {
    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {Array.from({ length: 2 }).map((_, index) => (
          <Card key={index} className="border-border/50 bg-card/50 backdrop-blur">
            <CardHeader>
              <CardTitle className="text-lg font-semibold flex items-center space-x-2">
                <span>{index === 0 ? "Request Volume" : "Error Rate & Response Time"}</span>
                <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
              </CardTitle>
              <p className="text-sm text-muted-foreground">Connecting to pipeline...</p>
            </CardHeader>
            <CardContent>
              <div className="h-[250px] flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <Loader2 className="h-8 w-8 animate-spin mx-auto mb-2" />
                  <p>Loading real-time data...</p>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  if (!sseConnected || chartData.length === 0) {
    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {Array.from({ length: 2 }).map((_, index) => (
          <Card key={index} className="border-border/50 bg-card/50 backdrop-blur">
            <CardHeader>
              <CardTitle className="text-lg font-semibold flex items-center space-x-2">
                <span>{index === 0 ? "Request Volume" : "Error Rate & Response Time"}</span>
                <WifiOff className="h-4 w-4 text-red-500" />
              </CardTitle>
              <p className="text-sm text-muted-foreground">Pipeline connection required</p>
            </CardHeader>
            <CardContent>
              <div className="h-[250px] flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <WifiOff className="h-12 w-12 mx-auto mb-2 opacity-50" />
                  <p>No data available</p>
                  <p className="text-xs mt-1">Ensure pipeline is running</p>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="text-lg font-semibold flex items-center space-x-2">
            <span>Request Volume</span>
            <span className={`flex items-center space-x-1 ${connectionStatus.color}`}>
              {connectionStatus.icon}
              <span className="text-xs">{connectionStatus.text}</span>
            </span>
          </CardTitle>
          <p className="text-sm text-muted-foreground">Real-time request processing from pipeline</p>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="requestGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.8}/>
                  <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0.1}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis 
                dataKey="time" 
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <YAxis 
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: "8px"
                }}
              />
              <Area
                type="monotone"
                dataKey="requests"
                stroke="hsl(var(--primary))"
                fillOpacity={1}
                fill="url(#requestGradient)"
                strokeWidth={2}
              />
            </AreaChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="text-lg font-semibold flex items-center space-x-2">
            <span>Error Rate & Response Time</span>
            <span className={`flex items-center space-x-1 ${connectionStatus.color}`}>
              {connectionStatus.icon}
              <span className="text-xs">{connectionStatus.text}</span>
            </span>
          </CardTitle>
          <p className="text-sm text-muted-foreground">Performance monitoring from pipeline</p>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis 
                dataKey="time" 
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <YAxis 
                yAxisId="left"
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <YAxis 
                yAxisId="right"
                orientation="right"
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: "8px"
                }}
              />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="errors"
                stroke="hsl(var(--destructive))"
                strokeWidth={2}
                dot={{ fill: "hsl(var(--destructive))", strokeWidth: 2, r: 3 }}
                name="Errors"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="responseTime"
                stroke="hsl(var(--warning))"
                strokeWidth={2}
                dot={{ fill: "hsl(var(--warning))", strokeWidth: 2, r: 3 }}
                name="Response Time (ms)"
              />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    </div>
  );
};

export default AnalyticsChart;