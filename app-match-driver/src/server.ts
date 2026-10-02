import { Kafka, logLevel } from 'kafkajs';
import { shutdownTelemetry } from './instrumentation.js';
import { createEachMessageHandler } from './matching-processor.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const inputTopic = process.env.KAFKA_RIDE_TOPIC ?? 'rides';
const resultsTopic = process.env.KAFKA_MATCHING_RESULTS_TOPIC ?? 'driver-matching-results';
const deadLetterTopic = process.env.KAFKA_MATCHING_DEAD_LETTER_TOPIC ?? 'driver-matching-dead-letter';
const groupId = process.env.KAFKA_CONSUMER_GROUP ?? 'app-match-driver';
const maxRetries = Number.parseInt(process.env.MATCHING_MAX_RETRIES ?? '3', 10);
const retryBackoffMs = Number.parseInt(process.env.MATCHING_RETRY_BACKOFF_MS ?? '250', 10);

if (brokers.length === 0 || !Number.isInteger(maxRetries) || maxRetries < 0 || !Number.isInteger(retryBackoffMs) || retryBackoffMs < 0) {
  throw new Error('Kafka brokers and non-negative integer retry settings are required.');
}

const kafka = new Kafka({ clientId: 'app-match-driver', brokers, logLevel: logLevel.NOTHING });
const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
const producer = kafka.producer();

try {
  console.info('Connecting to Kafka', { brokers, groupId, inputTopic, resultsTopic, deadLetterTopic });
  await Promise.all([consumer.connect(), producer.connect()]);
  await consumer.subscribe({ topic: inputTopic, fromBeginning: false });
  await consumer.run({
    autoCommit: false,
    eachMessage: createEachMessageHandler(consumer, producer, {
      inputTopic,
      resultsTopic,
      deadLetterTopic,
      maxRetries,
      retryBackoffMs,
    }),
  });
  console.info('Driver matching service is consuming rides', { inputTopic });
} catch (error) {
  console.error('Unable to start driver matching service', { error });
  await Promise.allSettled([consumer.disconnect(), producer.disconnect()]);
  await shutdownTelemetry();
  process.exitCode = 1;
  process.exit();
}

let shuttingDown = false;
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.info('Stopping driver matching service', { signal });
    await consumer.stop();
    await Promise.all([consumer.disconnect(), producer.disconnect()]);
    await shutdownTelemetry();
    process.exit();
  });
}
