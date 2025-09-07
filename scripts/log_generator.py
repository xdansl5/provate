#!/usr/bin/env python3
"""
Log Generator for ML Training
Generates realistic web application logs with additional ML features
"""

import json
import random
import time
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogGenerator:
    def __init__(self, kafka_servers="localhost:9092", topic="web-logs"):
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Endpoint patterns with realistic traffic distribution
        self.endpoints = {
            "/api/users": {"weight": 0.3, "methods": ["GET", "POST", "PUT", "DELETE"]},
            "/api/products": {"weight": 0.25, "methods": ["GET", "POST", "PUT", "DELETE"]},
            "/api/orders": {"weight": 0.2, "methods": ["GET", "POST", "PUT"]},
            "/api/auth": {"weight": 0.15, "methods": ["POST"]},
            "/api/admin": {"weight": 0.05, "methods": ["GET", "POST", "DELETE"]},
            "/": {"weight": 0.05, "methods": ["GET"]}
        }
        
        # IP address patterns
        self.ip_patterns = {
            "internal": ["192.168.1.{}", "10.0.0.{}", "172.16.0.{}"],
            "external": ["203.0.113.{}", "198.51.100.{}", "45.79.{}"]
        }
        
        # User agents
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15",
            "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0"
        ]
        
        # Status codes with realistic distribution
        self.status_codes = {
            200: 0.85,  # Success
            201: 0.05,  # Created
            400: 0.04,  # Bad Request
            401: 0.02,  # Unauthorized
            403: 0.01,  # Forbidden
            404: 0.02,  # Not Found
            500: 0.01   # Internal Server Error
        }
        
        # Response time patterns by endpoint and method
        self.response_time_patterns = {
            "GET": {"min": 50, "max": 300, "anomaly_threshold": 800},
            "POST": {"min": 100, "max": 500, "anomaly_threshold": 1200},
            "PUT": {"min": 80, "max": 400, "anomaly_threshold": 1000},
            "DELETE": {"min": 60, "max": 250, "anomaly_threshold": 600}
        }

    def generate_ip(self):
        """Generate realistic IP addresses"""
        ip_type = random.choices(["internal", "external"], weights=[0.3, 0.7])[0]
        pattern = random.choice(self.ip_patterns[ip_type])
        
        if ip_type == "internal":
            return pattern.format(random.randint(1, 254))
        else:
            return pattern.format(random.randint(1, 255))

    def generate_endpoint(self):
        """Generate endpoints based on weighted distribution"""
        return random.choices(
            list(self.endpoints.keys()),
            weights=[self.endpoints[ep]["weight"] for ep in self.endpoints.keys()]
        )[0]

    def generate_method(self, endpoint):
        """Generate HTTP method based on endpoint"""
        methods = self.endpoints[endpoint]["methods"]
        return random.choice(methods)

    def generate_status_code(self):
        """Generate status code based on realistic distribution"""
        return random.choices(
            list(self.status_codes.keys()),
            weights=list(self.status_codes.values())
        )[0]

    def generate_response_time(self, method, is_anomaly=False):
        """Generate realistic response time with optional anomalies"""
        pattern = self.response_time_patterns[method]
        
        if is_anomaly:
            # Generate anomalous response time
            return random.randint(pattern["anomaly_threshold"], pattern["anomaly_threshold"] * 2)
        else:
            # Generate normal response time
            return random.randint(pattern["min"], pattern["max"])

    def generate_anomaly(self, log_data):
        """Introduce realistic anomalies"""
        anomaly_type = random.choices(
            ["traffic_spike", "error_surge", "slow_response", "suspicious_ip"],
            weights=[0.1, 0.15, 0.2, 0.05]
        )[0]
        
        if anomaly_type == "traffic_spike":
            # Simulate traffic spike by increasing request rate
            pass  # Handled by generation rate
        elif anomaly_type == "error_surge":
            # Increase error probability
            log_data["status_code"] = random.choice([400, 401, 403, 404, 500])
        elif anomaly_type == "slow_response":
            # Generate slow response time
            log_data["response_time"] = self.generate_response_time(
                log_data["method"], is_anomaly=True
            )
        elif anomaly_type == "suspicious_ip":
            # Generate suspicious IP patterns
            log_data["ip"] = f"185.220.101.{random.randint(1, 255)}"
        
        return log_data

    def generate_log_entry(self, timestamp=None, include_anomalies=True):
        """Generate a single log entry"""
        if timestamp is None:
            timestamp = datetime.now()
        
        endpoint = self.generate_endpoint()
        method = self.generate_method(endpoint)
        status_code = self.generate_status_code()
        
        # Base log entry
        log_entry = {
            "timestamp": timestamp.isoformat(),
            "ip": self.generate_ip(),
            "method": method,
            "endpoint": endpoint,
            "status_code": status_code,
            "response_time": self.generate_response_time(method),
            "user_agent": random.choice(self.user_agents),
            "user_id": f"user_{random.randint(1000, 9999)}" if random.random() > 0.3 else None,
            "session_id": f"session_{random.randint(10000, 99999)}" if random.random() > 0.2 else None,
            "bytes_sent": random.randint(100, 50000),
            "referer": random.choice([
                "https://google.com",
                "https://bing.com",
                "https://example.com",
                None
            ]),
            "request_size": random.randint(50, 2048),
            "query_params": random.choice([
                "?page=1&size=20",
                "?sort=name&order=asc",
                "?filter=active",
                ""
            ])
        }
        
        # Introduce anomalies if enabled
        if include_anomalies and random.random() < 0.05:  # 5% anomaly rate
            log_entry = self.generate_anomaly(log_entry)
        
        return log_entry

    def send_to_kafka(self, log_entry):
        """Send log entry to Kafka"""
        try:
            # Use IP as key for partitioning
            key = log_entry["ip"]
            self.producer.send(self.topic, key=key, value=log_entry)
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")
            return False

    def generate_logs(self, rate=10, duration=None, include_anomalies=True):
        """Generate logs at specified rate"""
        logger.info(f"Starting log generation at {rate} logs/sec")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Kafka servers: {self.kafka_servers}")
        
        start_time = time.time()
        log_count = 0
        
        try:
            while True:
                # Check if duration limit reached
                if duration and (time.time() - start_time) > duration:
                    logger.info(f"⏰ Duration limit reached ({duration}s)")
                    break
                
                # Generate and send log
                log_entry = self.generate_log_entry(include_anomalies=include_anomalies)
                if self.send_to_kafka(log_entry):
                    log_count += 1
                    
                    if log_count % 100 == 0:
                        logger.info(f"Generated {log_count} logs")
                
                # Control rate
                time.sleep(1.0 / rate)
                
        except KeyboardInterrupt:
            logger.info("\nStopping log generation...")
        
        finally:
            self.producer.close()
            logger.info(f"Log generation completed. Total logs: {log_count}")

    def generate_training_dataset(self, num_samples=10000, output_file="training_logs.json"):
        """Generate training dataset for ML models"""
        logger.info(f"Generating training dataset with {num_samples} samples...")
        
        logs = []
        for i in range(num_samples):
            if i % 1000 == 0:
                logger.info(f"Generated {i}/{num_samples} samples...")
            
            log_entry = self.generate_log_entry(include_anomalies=True)
            logs.append(log_entry)
        
        # Save to file
        with open(output_file, 'w') as f:
            json.dump(logs, f, indent=2)
        
        logger.info(f"Training dataset saved to {output_file}")
        return logs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Log generator for ML training')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-logs', help='Kafka topic')
    parser.add_argument('--rate', type=int, default=10, help='Logs per second')
    parser.add_argument('--duration', type=int, help='Duration in seconds (optional)')
    parser.add_argument('--no-anomalies', action='store_true', help='Disable anomaly generation')
    parser.add_argument('--training-data', type=int, help='Generate training dataset with N samples')
    parser.add_argument('--output-file', default='training_logs.json', help='Output file for training data')
    
    args = parser.parse_args()
    
    generator = LogGenerator(args.kafka_servers, args.topic)
    
    if args.training_data:
        generator.generate_training_dataset(args.training_data, args.output_file)
    else:
        generator.generate_logs(
            rate=args.rate,
            duration=args.duration,
            include_anomalies=not args.no_anomalies
        )
    
    