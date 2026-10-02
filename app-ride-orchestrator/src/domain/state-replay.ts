import type { Kafka } from 'kafkajs';
import type { TripState } from '../trip-state.js';
import { parseTripState } from '../trip-state.js';
import type { OrchestratorOptions } from './orchestrator-options.js';

const pause = (milliseconds: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

export async function replayTripStates(
  kafka: Kafka,
  options: OrchestratorOptions,
  trips: Map<string, TripState>,
  logger: Pick<Console, 'info' | 'warn' | 'error'>,
): Promise<void> {
  const admin = kafka.admin();
  const replay = kafka.consumer({
    groupId: `${options.consumerGroup}-state-replay-${process.pid}-${Date.now()}`,
    allowAutoTopicCreation: false,
  });
  await Promise.all([admin.connect(), replay.connect()]);
  try {
    const ends = await admin.fetchTopicOffsets(options.stateTopic);
    const targets = new Map(
      ends.map(({ partition, offset }) => [partition, BigInt(offset)]),
    );
    const reached = new Set(
      ends
        .filter(({ offset }) => BigInt(offset) === 0n)
        .map(({ partition }) => partition),
    );
    let resolveReplay!: () => void;
    const complete = new Promise<void>((resolve) => {
      resolveReplay = resolve;
    });
    const markComplete = (partition: number, nextOffset: bigint) => {
      const target = targets.get(partition);
      if (target !== undefined && nextOffset >= target) reached.add(partition);
      if (reached.size === targets.size) resolveReplay();
    };
    if (reached.size === targets.size) return;

    await replay.subscribe({
      topic: options.stateTopic,
      fromBeginning: true,
    });
    await replay.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      eachMessage: async ({ partition, message }) => {
        const rideId = message.key?.toString();
        if (rideId) {
          const value = message.value?.toString();
          if (value === undefined) trips.delete(rideId);
          else trips.set(rideId, parseTripState(value));
        }
        markComplete(partition, BigInt(message.offset) + 1n);
      },
    });
    await Promise.race([
      complete,
      pause(60_000).then(() => {
        throw new Error(`Timed out replaying ${options.stateTopic}.`);
      }),
    ]);
    await replay.stop();
    logger.info('Restored trip state', {
      rides: trips.size,
      topic: options.stateTopic,
    });
  } finally {
    await Promise.allSettled([replay.disconnect(), admin.disconnect()]);
  }
}
