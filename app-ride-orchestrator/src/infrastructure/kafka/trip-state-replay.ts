import type { Kafka, KafkaMessage } from 'kafkajs';
import type {
  Logger,
  StateRestore,
  TimingPolicy,
} from '../../application/ports.js';
import type { TripStore } from '../../application/trip-store.js';
import { pause } from '../../shared/pause.js';
import { parseTripState } from '../serialization/trip-state-codec.js';
import { createReplayProgress } from './replay-progress.js';
import type { TopicMap } from './topics.js';

export interface TripStateReplayOptions {
  topics: Pick<TopicMap, 'state'>;
  consumerGroup: string;
  timing: Pick<TimingPolicy, 'replayTimeoutMs'>;
}

function restoreFromMessage(
  message: Pick<KafkaMessage, 'key' | 'value'>,
  store: TripStore,
): void {
  const rideId = message.key?.toString();
  if (!rideId) return;

  const value = message.value?.toString();
  store.restore(
    rideId,
    value === undefined ? undefined : parseTripState(value),
  );
}

async function waitForReplay(
  complete: Promise<void>,
  topic: string,
  replayTimeoutMs: number,
): Promise<void> {
  await Promise.race([
    complete,
    pause(replayTimeoutMs).then(() => {
      throw new Error(`Timed out replaying ${topic}.`);
    }),
  ]);
}

export function createTripStateReplay(
  kafka: Kafka,
  options: TripStateReplayOptions,
  logger: Logger,
): StateRestore {
  const stateTopic = options.topics.state;

  return async (store: TripStore): Promise<void> => {
    const admin = kafka.admin();
    const replay = kafka.consumer({
      groupId: `${options.consumerGroup}-state-replay-${process.pid}-${Date.now()}`,
      allowAutoTopicCreation: false,
    });
    await Promise.all([admin.connect(), replay.connect()]);
    try {
      const ends = await admin.fetchTopicOffsets(stateTopic);
      const progress = createReplayProgress(ends);
      if (progress.isComplete()) return;

      await replay.subscribe({ topic: stateTopic, fromBeginning: true });
      await replay.run({
        autoCommit: false,
        partitionsConsumedConcurrently: 1,
        eachMessage: async ({ partition, message }) => {
          restoreFromMessage(message, store);
          progress.markPartitionReached(partition, BigInt(message.offset) + 1n);
        },
      });
      await waitForReplay(
        progress.complete,
        stateTopic,
        options.timing.replayTimeoutMs,
      );
      await replay.stop();
      logger.info('Restored trip state', {
        rides: store.size,
        topic: stateTopic,
      });
    } finally {
      await Promise.allSettled([replay.disconnect(), admin.disconnect()]);
    }
  };
}
