import Header from "@/components/Header";
import MetricsGrid from "@/components/MetricsGrid";
import LogStream from "@/components/LogStream";
import AnalyticsChart from "@/components/AnalyticsChart";
import SqlQuery from "@/components/SqlQuery";
import PipelineStatus from "@/components/PipelineStatus";

const Index = () => {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      <main className="container mx-auto px-6 py-8 space-y-8">
        {/* Pipeline Status */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Pipeline Health</h2>
            <p className="text-muted-foreground">Real-time status of your data pipeline components</p>
          </div>
          <PipelineStatus />
        </section>

        {/* Metrics Overview */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Platform Overview</h2>
            <p className="text-muted-foreground">Real-time metrics from your Lakehouse architecture</p>
          </div>
          <MetricsGrid />
        </section>

        {/* Analytics Charts */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Analytics Dashboard</h2>
            <p className="text-muted-foreground">Spark Structured Streaming insights</p>
          </div>
          <AnalyticsChart />
        </section>

        {/* Live Data Stream */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Live Data Pipeline</h2>
            <p className="text-muted-foreground">Real-time log ingestion and processing</p>
          </div>
          <LogStream />
        </section>

        {/* SQL Query Interface */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Interactive Analytics</h2>
            <p className="text-muted-foreground">Query your Delta Lake with Spark SQL</p>
          </div>
          <SqlQuery />
        </section>
      </main>
    </div>
  );
};

export default Index;
