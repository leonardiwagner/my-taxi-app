import { RideOrchestrator } from './application/ride-orchestrator.js';
import { loadConfig } from './config.js';
import { createKafkaClients } from './infrastructure/kafka/clients.js';
import { KafkaOutcomeSource } from './infrastructure/kafka/outcome-consumer.js';
import { TransactionalTripWriter } from './infrastructure/kafka/transactional-trip-writer.js';
import { createTripStateReplay } from './infrastructure/kafka/trip-state-replay.js';
import { shutdownTelemetry } from './instrumentation.js';

const config = loadConfig();
const { kafka, consumer, producer } = createKafkaClients(config);
const orchestrator = new RideOrchestrator({
  source: new KafkaOutcomeSource(
    consumer,
    config.topics,
    config.timing.heartbeatIntervalMs,
  ),
  writer: new TransactionalTripWriter(
    producer,
    config.topics,
    config.consumerGroup,
  ),
  restoreState: createTripStateReplay(kafka, config, console),
  timing: config.timing,
});

try {
  console.info('Starting ride orchestrator', {
    brokers: config.brokers,
    consumerGroup: config.consumerGroup,
    ...config.topics,
  });
  await orchestrator.start();
  console.info('Ride orchestrator is consuming results', {
    driverTopic: config.topics.driver,
    pricingTopic: config.topics.pricing,
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
