#!/bin/bash

# Fix for Python 3.12: ensure pip, setuptools, and wheel are up to date
python3 -m ensurepip --upgrade
python3 -m pip install --upgrade pip setuptools wheel

# Setup Environment for Lakehouse Platform
echo "Setting up Lakehouse Analytics Environment"

# Create directories
mkdir -p /tmp/delta-lake/logs
mkdir -p /tmp/delta-lake/anomalies
mkdir -p /tmp/checkpoints/logs
mkdir -p /tmp/checkpoints/anomalies

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Download Spark if not present
SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
SPARK_HOME="/opt/spark"

if [ ! -d "$SPARK_HOME" ]; then
    echo "Downloading Apache Spark..."
    cd /tmp
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
    sudo mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME
    sudo chown -R $USER:$USER $SPARK_HOME
fi

# Configure environment variables
echo "Configuring environment variables..."
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3

# Persist environment variables in .bashrc
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc

# Setup Kafka (using Docker Compose)
echo "Setting up Kafka with Docker..."
cat > docker-compose.yml << EOF
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log
    networks:
      - logstream-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
    volumes:
      - kafka-data:/var/lib/kafka/data
    networks:
      - logstream-network

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    restart: always
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
    networks:
      - logstream-network

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:

networks:
  logstream-network:
    driver: bridge
EOF

# Start Kafka
if command -v docker-compose &> /dev/null; then
    echo "Starting Kafka..."
    docker-compose up -d

    # Wait for Kafka to be ready
    echo "Waiting for Kafka to be ready..."
    sleep 30

    # Create the topic
    docker-compose exec kafka kafka-topics --create --topic web-logs --bootstrap-server localhost:29092 --replication-factor 1 --partitions 3

    echo "Kafka setup completed."
    echo "Kafka UI available at: http://localhost:8080"
else
    echo "Docker Compose not found. Please install Docker and Docker Compose to run Kafka."
fi

echo "Environment setup completed."

echo "Next steps:"
echo "1. Start log generation: python3 log_generator.py --rate 10"
echo "2. Start streaming processing: python3 ml_streaming_processor.py --mode stream"
echo "3. Run analytics: python3 streaming_processor.py --mode analytics"
echo "4. Start anomaly detection: python3 anomaly_detector.py --mode detect"

echo "Monitor Kafka: http://localhost:8080"
