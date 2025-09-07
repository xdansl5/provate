import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Play, Database, Clock, Zap, Wifi, WifiOff, Loader2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { useData } from "@/contexts/DataContext";
import { useToast } from "@/hooks/use-toast";

const SqlQuery = () => {
  const { executeQuery, sseConnected, loading } = useData();
  const { toast } = useToast();
  
  const [query, setQuery] = useState(
    `SELECT 
  date_format(timestamp, 'yyyy-MM-dd HH:mm') as hour,
  count(*) as total_requests,
  sum(case when status >= 400 then 1 else 0 end) as errors,
  avg(response_time) as avg_response_time
FROM delta_lake.logs 
WHERE timestamp >= current_timestamp() - interval 1 day
GROUP BY date_format(timestamp, 'yyyy-MM-dd HH:mm')
ORDER BY hour DESC
LIMIT 10`
  );
  
  const [isRunning, setIsRunning] = useState(false);
  const [results, setResults] = useState<any[]>([]);
  const [executionTime, setExecutionTime] = useState<string | null>(null);

  const handleRunQuery = async () => {
    if (!query.trim()) {
      toast({
        title: "Query Required",
        description: "Please enter a SQL query to execute.",
        variant: "destructive",
      });
      return;
    }
    
    if (!sseConnected) {
      toast({
        title: "Pipeline Not Connected",
        description: "Please ensure the data pipeline is running and connected.",
        variant: "destructive",
      });
      return;
    }
    
    setIsRunning(true);
    setExecutionTime(null);
    
    try {
      const { results: queryResults, executionTime: time } = await executeQuery(query);
      setResults(queryResults);
      setExecutionTime(time);
      
      toast({
        title: "Query Executed Successfully",
        description: `Found ${queryResults.length} rows in ${time}`,
      });
    } catch (error) {
      console.error('Query execution error:', error);
      toast({
        title: "Query Failed",
        description: error instanceof Error ? error.message : "An error occurred while executing the query.",
        variant: "destructive",
      });
    } finally {
      setIsRunning(false);
    }
  };

  const getConnectionStatus = () => {
    if (loading) {
      return { icon: <Loader2 className="h-4 w-4 animate-spin" />, text: "Connecting...", color: "text-muted-foreground" };
    }
    if (sseConnected) {
      return { icon: <Wifi className="h-4 w-4" />, text: "Pipeline Connected", color: "text-green-500" };
    }
    return { icon: <WifiOff className="h-4 w-4" />, text: "Pipeline Disconnected", color: "text-red-500" };
  };

  const connectionStatus = getConnectionStatus();

  const sampleQueries = [
    {
      name: "Top Error Endpoints",
      query: `SELECT endpoint, count(*) as error_count,
       avg(response_time) as avg_response_time
FROM delta_lake.logs 
WHERE status >= 400 AND timestamp >= current_timestamp() - interval 1 hour
GROUP BY endpoint
ORDER BY error_count DESC
LIMIT 5`
    },
    {
      name: "User Session Analysis", 
      query: `SELECT 
  user_id,
  count(distinct session_id) as sessions,
  count(*) as page_views,
  sum(response_time) / count(*) as avg_session_time
FROM delta_lake.logs
WHERE timestamp >= current_date()
GROUP BY user_id
ORDER BY page_views DESC
LIMIT 10`
    },
    {
      name: "Real-time Anomalies",
      query: `SELECT 
  endpoint, source, level,
  count(*) as anomaly_count,
  max(response_time) as max_response_time
FROM delta_lake.logs 
WHERE (level = 'ERROR' OR response_time > 1000)
  AND timestamp >= current_timestamp() - interval 30 minutes
GROUP BY endpoint, source, level
ORDER BY anomaly_count DESC`
    }
  ];

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-lg font-semibold flex items-center space-x-2">
              <Database className="h-5 w-5 text-primary" />
              <span>Spark SQL Query Interface</span>
            </CardTitle>
            <div className="flex items-center space-x-2 text-sm text-muted-foreground">
              <span>Interactive analytics on Delta Lake</span>
              <span className={`flex items-center space-x-1 ${connectionStatus.color}`}>
                {connectionStatus.icon}
                <span>{connectionStatus.text}</span>
              </span>
            </div>
          </div>
          <div className="flex space-x-2">
            <Badge variant="outline" className={`${sseConnected ? 'bg-success/10 text-success border-success/20' : 'bg-red-500/10 text-red-500 border-red-500/20'}`}>
              <Zap className="h-3 w-3 mr-1" />
              {sseConnected ? 'Spark SQL Ready' : 'Pipeline Offline'}
            </Badge>
            <Badge variant="outline" className={`${sseConnected ? 'bg-gradient-primary text-primary-foreground border-primary/20' : 'bg-muted text-muted-foreground border-muted'}`}>
              {sseConnected ? 'Delta Lake Connected' : 'Delta Lake Offline'}
            </Badge>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-2">
          <div className="flex space-x-2 mb-2">
            {sampleQueries.map((sample, index) => (
              <Button
                key={index}
                variant="outline"
                size="sm"
                onClick={() => setQuery(sample.query)}
                className="text-xs"
                disabled={!sseConnected}
              >
                {sample.name}
              </Button>
            ))}
          </div>
          
          <Textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Enter your Spark SQL query..."
            className="min-h-[120px] font-mono text-sm bg-muted/30 border-border/50"
            disabled={!sseConnected}
          />
          
          <div className="flex items-center justify-between">
            <Button 
              onClick={handleRunQuery}
              disabled={isRunning || !sseConnected}
              className="flex items-center space-x-2"
            >
              {isRunning ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
              <span>{isRunning ? "Executing..." : "Execute Query"}</span>
            </Button>
            
            {executionTime && (
              <div className="flex items-center space-x-2 text-sm text-muted-foreground">
                <Clock className="h-4 w-4" />
                <span>Executed in {executionTime}</span>
              </div>
            )}
          </div>
        </div>

        {results.length > 0 && (
          <div className="border border-border/50 rounded-lg bg-muted/20">
            <div className="p-3 border-b border-border/50 bg-muted/30">
              <h4 className="text-sm font-medium">Query Results ({results.length} rows)</h4>
            </div>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow className="border-border/30">
                    {results.length > 0 && Object.keys(results[0]).map((column) => (
                      <TableHead key={column} className="font-mono text-xs">
                        {column}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {results.map((row, index) => (
                    <TableRow key={index} className="border-border/30">
                      {Object.entries(row).map(([key, value]) => (
                        <TableCell key={key} className="font-mono text-xs">
                          {typeof value === 'number' && key.includes('error') && value > 10 ? (
                            <span className="text-destructive">{value}</span>
                          ) : typeof value === 'number' && key.includes('time') && value > 1000 ? (
                            <span className="text-warning">{value}</span>
                          ) : typeof value === 'number' && (key.includes('time') || key.includes('count')) ? (
                            <span>{typeof value === 'number' && value % 1 !== 0 ? value.toFixed(2) : value}</span>
                          ) : (
                            <span>{String(value)}</span>
                          )}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {!sseConnected && (
          <div className="text-center py-8 text-muted-foreground border border-dashed border-border/50 rounded-lg">
            <Database className="h-12 w-12 mx-auto mb-2 opacity-50" />
            <p className="text-sm">Pipeline connection required to execute queries</p>
            <p className="text-xs mt-1">Ensure Kafka, Spark, and Delta Lake are running</p>
          </div>
        )}
      </CardContent>
    </Card>
  );
};

export default SqlQuery;