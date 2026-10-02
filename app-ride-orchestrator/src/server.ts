import { Kafka, logLevel } from 'kafkajs';
import { shutdownTelemetry } from './instrumentation.js';
import { RideOrchestrator } from './domain/orchestrator.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const consumerGroup =
  process.env.KAFKA_CONSUMER_GROUP ?? 'app-ride-orchestrator';
const driverTopic =
  process.env.KAFKA_DRIVER_RESULTS_TOPIC ?? 'driver-matching-results';
const pricingTopic =
  process.env.KAFKA_PRICING_RESULTS_TOPIC ?? 'pricing-results';
const stateTopic = process.env.KAFKA_TRIP_STATE_TOPIC ?? 'trip.state';
const confirmedTopic =
  process.env.KAFKA_RIDE_CONFIRMED_TOPIC ?? 'ride.confirmed';
const rejectedTopic = process.env.KAFKA_RIDE_REJECTED_TOPIC ?? 'ride.rejected';
const timeoutMs = 60_000;
const finalRetentionMs = Number.parseInt(
  process.env.TRIP_FINAL_RETENTION_MS ?? `${24 * 60 * 60 * 1_000}`,
  10,
);

if (
  brokers.length === 0 ||
  !Number.isSafeInteger(finalRetentionMs) ||
  finalRetentionMs < 0
) {
  throw new Error(
    'Kafka brokers and a non-negative final retention period are required.',
  );
}

const kafka = new Kafka({
  clientId: 'app-ride-orchestrator',
  brokers,
  logLevel: logLevel.NOTHING,
});
const consumer = kafka.consumer({
  groupId: consumerGroup,
  allowAutoTopicCreation: false,
});
const producer = kafka.producer({
  transactionalId: `${consumerGroup}-transactional`,
  allowAutoTopicCreation: false,
});
const orchestrator = new RideOrchestrator(kafka, consumer, producer, {
  driverTopic,
  pricingTopic,
  stateTopic,
  confirmedTopic,
  rejectedTopic,
  consumerGroup,
  timeoutMs,
  finalRetentionMs,
});

try {
  console.info('Starting ride orchestrator', {
    brokers,
    consumerGroup,
    driverTopic,
    pricingTopic,
    stateTopic,
    confirmedTopic,
    rejectedTopic,
  });
  await orchestrator.start();
  console.info('Ride orchestrator is consuming results', {
    driverTopic,
    pricingTopic,
  });
} catch (error) {
  console.error('Unable to start ride orchestrator', { error });
  await Promise.allSettled([
    consumer.disconnect(),
    producer.disconnect(),
    shutdownTelemetry(),
  ]);
  process.exitCode = 1;
  process.exit();
}

let shuttingDown = false;
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.info('Stopping ride orchestrator', { signal });
    try {
      await orchestrator.stop();
    } finally {
      await shutdownTelemetry();
      process.exit();
    }
  });
}
