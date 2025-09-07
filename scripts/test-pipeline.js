#!/usr/bin/env node

import { Kafka } from 'kafkajs';

// Configuration
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'web-logs';

// Test data generation
const endpoints = ['/api/users', '/api/orders', '/api/products', '/health', '/metrics', '/api/analytics'];
const methods = ['GET', 'POST', 'PUT', 'DELETE'];
const sources = ['web-server', 'api-gateway', 'mobile-app', 'admin-panel'];
const userAgents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15'
];

function generateLogEntry() {
  const now = Date.now();
  const method = methods[Math.floor(Math.random() * methods.length)];
  const endpoint = endpoints[Math.floor(Math.random() * endpoints.length)];
  const source = sources[Math.floor(Math.random() * sources.length)];
  
  // Generate realistic status codes with some errors
  let statusCode = 200;
  if (Math.random() < 0.05) statusCode = 500; // 5% server errors
  else if (Math.random() < 0.1) statusCode = 404; // 10% not found
  else if (Math.random() < 0.15) statusCode = 400; // 15% bad request
  
  // Generate realistic response times
  let responseTime = Math.floor(Math.random() * 200) + 50; // 50-250ms normal
  if (statusCode >= 400) {
    responseTime = Math.floor(Math.random() * 2000) + 500; // 500-2500ms for errors
  }
  
  // Generate realistic IP addresses
  const ip = `192.168.${Math.floor(Math.random() * 10)}.${Math.floor(Math.random() * 255)}`;
  
  // Generate user and session IDs
  const userId = `user_${Math.floor(Math.random() * 1000) + 1}`;
  const sessionId = `sess_${Math.floor(Math.random() * 10000) + 1000}`;
  
  return {
    timestamp: now,
    method,
    endpoint,
    status_code: statusCode,
    response_time: responseTime,
    ip,
    user_id: userId,
    session_id: sessionId,
    source,
    user_agent: userAgents[Math.floor(Math.random() * userAgents.length)],
    request_size: Math.floor(Math.random() * 1000) + 100,
    response_size: Math.floor(Math.random() * 5000) + 500
  };
}

async function testKafkaConnection() {
  console.log('Testing Kafka connection...');
  
  const kafka = new Kafka({ 
    clientId: 'pipeline-test', 
    brokers: KAFKA_BROKERS 
  });
  
  try {
    const producer = kafka.producer();
    await producer.connect();
    console.log('Kafka producer connected successfully');
    
    // Test topic creation/access
    const admin = kafka.admin();
    await admin.connect();
    
    const topics = await admin.listTopics();
    console.log('Available topics:', topics);
    
    if (!topics.includes(KAFKA_TOPIC)) {
      console.log(`Creating topic: ${KAFKA_TOPIC}`);
      await admin.createTopics({
        topics: [{
          topic: KAFKA_TOPIC,
          numPartitions: 1,
          replicationFactor: 1
        }]
      });
      console.log(`Topic ${KAFKA_TOPIC} created successfully`);
    } else {
      console.log(`Topic ${KAFKA_TOPIC} already exists`);
    }
    
    await admin.disconnect();
    await producer.disconnect();
    
    return true;
  } catch (error) {
    console.error('Kafka connection failed:', error.message);
    return false;
  }
}

async function generateTestData(duration = 60) {
  console.log(`Generating test data for ${duration} seconds...`);
  
  const kafka = new Kafka({ 
    clientId: 'pipeline-test', 
    brokers: KAFKA_BROKERS 
  });
  
  const producer = kafka.producer();
  
  try {
    await producer.connect();
    console.log('Producer connected, starting data generation...');
    
    const startTime = Date.now();
    const endTime = startTime + (duration * 1000);
    let messageCount = 0;
    
    const interval = setInterval(async () => {
      if (Date.now() > endTime) {
        clearInterval(interval);
        await producer.disconnect();
        console.log(`Test data generation completed. Sent ${messageCount} messages.`);
        return;
      }
      
      try {
        const logEntry = generateLogEntry();
        await producer.send({
          topic: KAFKA_TOPIC,
          messages: [{
            value: JSON.stringify(logEntry)
          }]
        });
        
        messageCount++;
        if (messageCount % 10 === 0) {
          console.log(`Sent ${messageCount} messages...`);
        }
      } catch (error) {
        console.error('Error sending message:', error.message);
      }
    }, Math.random() * 1000 + 500); // Random interval between 500-1500ms
    
  } catch (error) {
    console.error('Failed to generate test data:', error.message);
    await producer.disconnect();
  }
}

async function testAPIServer() {
  console.log('Testing API server...');
  
  try {
    const response = await fetch('http://localhost:4000/health');
    const data = await response.json();
    
    console.log('API server is running');
    console.log('Server status:', data);
    
    return true;
  } catch (error) {
    console.error('API server test failed:', error.message);
    return false;
  }
}

async function main() {
  console.log('Pipeline Test Suite');
  console.log('======================\n');
  
  // Test Kafka connection
  const kafkaOk = await testKafkaConnection();
  if (!kafkaOk) {
    console.log('\nKafka test failed. Please ensure Kafka is running.');
    process.exit(1);
  }
  
  // Test API server
  const apiOk = await testAPIServer();
  if (!apiOk) {
    console.log('\nAPI server not available. Please start the server with: npm run server');
  }
  
  // Generate test data
  const duration = parseInt(process.argv[2]) || 60;
  console.log(`\nStarting test data generation for ${duration} seconds...`);
  console.log('Press Ctrl+C to stop early\n');
  
  await generateTestData(duration);
  
  console.log('\nPipeline test completed successfully!');
  console.log('Open http://localhost:5173 to view the real-time dashboard');
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\nTest stopped by user');
  process.exit(0);
});

// Run the test
main().catch(console.error);