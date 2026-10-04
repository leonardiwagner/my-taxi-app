import { Kafka, logLevel } from 'kafkajs';
import { shutdownTelemetry } from './instrumentation.js';
import { createTraceAwareLogger } from './trace-context.js';
import { startRideGenerator } from './ride-generator.js';
import { createRideRequestPublisher } from './ride-request-publisher.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const topic = process.env.KAFKA_RIDE_TOPIC ?? 'rides';
const kafka = new Kafka({ clientId: 'app-ride', brokers, logLevel: logLevel.NOTHING });
const logger = createTraceAwareLogger(console);
const producer = kafka.producer();

try {
  logger.info('Connecting to Kafka', { brokers });
  await producer.connect();
  logger.info('Connected to Kafka', { brokers, topic });
} catch (error) {
  logger.error('Unable to connect to Kafka', { brokers, error });
  await shutdownTelemetry();
  process.exitCode = 1;
  process.exit();
}

const generator = startRideGenerator(createRideRequestPublisher(producer, topic), {
  onError: (error) => logger.error('Unable to publish ride request', { error }),
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    logger.info('Stopping ride generator', { signal });
    await generator.stop();
    await producer.disconnect();
    logger.info('Disconnected from Kafka');
    await shutdownTelemetry();
    process.exit();
  });
}
