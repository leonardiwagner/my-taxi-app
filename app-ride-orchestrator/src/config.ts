import type { TimingPolicy } from './application/ports.js';
import type { TopicMap } from './infrastructure/kafka/topics.js';

export interface OrchestratorConfig {
  brokers: string[];
  consumerGroup: string;
  topics: TopicMap;
  timing: TimingPolicy;
}

export function loadConfig(
  env: NodeJS.ProcessEnv = process.env,
): OrchestratorConfig {
  const brokers = (env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);
  const consumerGroup = env.KAFKA_CONSUMER_GROUP ?? 'app-ride-orchestrator';
  const finalRetentionMs = Number.parseInt(
    env.TRIP_FINAL_RETENTION_MS ?? `${24 * 60 * 60 * 1_000}`,
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

  return {
    brokers,
    consumerGroup,
    topics: {
      driver: env.KAFKA_DRIVER_RESULTS_TOPIC ?? 'driver-matching-results',
      pricing: env.KAFKA_PRICING_RESULTS_TOPIC ?? 'pricing-results',
      state: env.KAFKA_TRIP_STATE_TOPIC ?? 'trip.state',
      confirmed: env.KAFKA_RIDE_CONFIRMED_TOPIC ?? 'ride.confirmed',
      rejected: env.KAFKA_RIDE_REJECTED_TOPIC ?? 'ride.rejected',
    },
    timing: {
      timeoutMs: 60_000,
      finalRetentionMs,
      sweepIntervalMs: 1_000,
      replayTimeoutMs: 60_000,
      heartbeatIntervalMs: 5_000,
    },
  };
}
