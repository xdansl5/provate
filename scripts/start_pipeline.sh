#!/bin/bash

# LogStream Lakehouse Pipeline Startup Script
# This script starts the complete real-time log processing pipeline

set -e

echo "Starting LogStream Lakehouse Pipeline..."
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
check_docker() {
    print_status "Checking Docker availability..."
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Check if Docker Compose is available
check_docker_compose() {
    print_status "Checking Docker Compose availability..."
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose and try again."
        exit 1
    fi
    print_success "Docker Compose is available"
}

# Start infrastructure services
start_services() {
    print_status "Starting infrastructure services..."
    
    # Check if services are already running
    if docker-compose ps | grep -q "Up"; then
        print_warning "Some services are already running. Stopping them first..."
        docker-compose down
    fi
    
    # Start services
    docker-compose up -d
    
    print_status "Waiting for services to be ready..."
    sleep 30
    
    # Check service health
    print_status "Checking service health..."
    
    # Check Kafka
    if curl -s http://localhost:8080/api/clusters > /dev/null; then
        print_success "Kafka is ready"
    else
        print_warning "Kafka might still be starting up..."
    fi
}

# Setup Python environment
setup_python() {
    print_status "Setting up Python environment..."
    
    # Check if Python 3 is available
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed. Please install Python 3 and try again."
        exit 1
    fi
    
    # Install requirements
    if [ -f "requirements.txt" ]; then
        print_status "Installing Python dependencies..."
        pip3 install -r requirements.txt
        print_success "Python dependencies installed"
    else
        print_warning "requirements.txt not found, skipping dependency installation"
    fi
}

# Create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p /tmp/delta-lake/logs
    mkdir -p /tmp/delta-lake/anomalies
    mkdir -p /tmp/delta-lake/ml-enriched-logs
    mkdir -p /tmp/checkpoints/logs
    mkdir -p /tmp/checkpoints/anomalies
    mkdir -p /tmp/checkpoints/ml-logs
    
    print_success "Directories created"
}

# Start the pipeline
start_pipeline() {
    print_status "Starting the ML-powered log processing pipeline..."
    
    # Check if pipeline orchestrator exists
    if [ ! -f "pipeline_orchestrator.py" ]; then
        print_error "pipeline_orchestrator.py not found. Please ensure you're in the correct directory."
        exit 1
    fi
    
    # Start the pipeline in the background
    python3 pipeline_orchestrator.py --action start &
    PIPELINE_PID=$!
    
    print_success "Pipeline started with PID: $PIPELINE_PID"
    echo $PIPELINE_PID > .pipeline.pid
    
    print_status "Pipeline is starting up. This may take a few minutes..."
    print_status "You can monitor the status with: python3 pipeline_orchestrator.py --action status"
}

# Show access information
show_access_info() {
    echo ""
    echo "Access Information"
    echo "===================="
    echo "Kafka UI:         http://localhost:8080"
    echo "MLflow:           http://localhost:5000"
    echo ""
    echo "Pipeline Management"
    echo "======================"
    echo "Check Status:     python3 pipeline_orchestrator.py --action status"
    echo "Run Analytics:    python3 pipeline_orchestrator.py --action analytics"
    echo "Stop Pipeline:    python3 pipeline_orchestrator.py --action stop"
    echo ""
}

# Main execution
main() {
    echo "Starting LogStream Lakehouse Pipeline..."
    
    # Check prerequisites
    check_docker
    check_docker_compose
    
    # Start services
    start_services
    
    # Setup environment
    setup_python
    create_directories
    
    # Start pipeline
    start_pipeline
    
    # Show access information
    show_access_info
    
    print_success "Pipeline startup completed!"
    print_status "The pipeline is now running. Check the URLs above to access the dashboards."
    print_status "Press Ctrl+C to stop the pipeline."
    
    # Wait for user interrupt
    wait $PIPELINE_PID
}

# Trap Ctrl+C and cleanup
cleanup() {
    echo ""
    print_status "Shutting down pipeline..."
    
    if [ -f ".pipeline.pid" ]; then
        PIPELINE_PID=$(cat .pipeline.pid)
        if kill -0 $PIPELINE_PID 2>/dev/null; then
            kill $PIPELINE_PID
            print_success "Pipeline stopped"
        fi
        rm -f .pipeline.pid
    fi
    
    print_status "Stopping infrastructure services..."
    docker-compose down
    
    print_success "Cleanup completed"
    exit 0
}

trap cleanup SIGINT SIGTERM

# Run main function
main "$@"