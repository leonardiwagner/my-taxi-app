import type { Consumer, EachMessagePayload, Producer } from 'kafkajs';
import { createMatchingOutcome, parseRideRequest } from './matching-outcome.js';

export interface MatchingProcessorOptions {
  inputTopic: string;
  resultsTopic: string;
  deadLetterTopic: string;
  maxRetries: number;
  retryBackoffMs: number;
  delay?: () => Promise<void>;
  createOutcome?: typeof createMatchingOutcome;
  publishResult?: (producer: Producer, topic: string, key: string, value: string) => Promise<void>;
  publishDeadLetter?: (producer: Producer, topic: string, key: string, value: string) => Promise<void>;
  commitOffset?: (consumer: Consumer, payload: EachMessagePayload) => Promise<void>;
  wait?: (milliseconds: number) => Promise<void>;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

const wait = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

async function sendRecord(producer: Producer, topic: string, key: string, value: string): Promise<void> {
  await producer.send({ topic, messages: [{ key, value }] });
}

export function createEachMessageHandler(
  consumer: Consumer,
  producer: Producer,
  options: MatchingProcessorOptions,
): (payload: EachMessagePayload) => Promise<void> {
  const logger = options.logger ?? console;
  const publishResult = options.publishResult ?? sendRecord;
  const publishDeadLetter = options.publishDeadLetter ?? sendRecord;
  const commitOffset = options.commitOffset ?? (async (currentConsumer, payload) => {
    await currentConsumer.commitOffsets([
      {
        topic: payload.topic,
        partition: payload.partition,
        offset: (BigInt(payload.message.offset) + 1n).toString(),
      },
    ]);
  });

  return async (payload): Promise<void> => {
    const messageValue = payload.message.value?.toString();
    const messageKey = payload.message.key?.toString();
    let rideId = messageKey ?? 'unknown';

    try {
      const ride = parseRideRequest(messageValue ?? '');
      rideId = ride.id;
      const outcome = await retry(
        () => processRide(ride, options),
        options,
        (error, attempt) => logger.warn('Retrying ride matching', { rideId, attempt, error }),
      );
      await retry(
        () => publishResult(producer, options.resultsTopic, ride.id, JSON.stringify(outcome)),
        options,
        (error, attempt) => logger.warn('Retrying matching result publish', { rideId, attempt, error }),
      );

      if (outcome.status === 'success') {
        logger.info('Driver matched to ride', { rideId, driver: outcome.driver });
      } else {
        logger.info('No driver available for ride', { rideId, error: outcome.error });
      }
    } catch (error) {
      logger.error('Ride matching failed after retries', { rideId, error });
      const deadLetter = JSON.stringify({
        rideId,
        originalKey: messageKey,
        originalValue: messageValue ?? null,
        error: serializeError(error),
        failedAt: new Date().toISOString(),
      });
      await retry(
        () => publishDeadLetter(producer, options.deadLetterTopic, rideId, deadLetter),
        options,
        (retryError, attempt) => logger.warn('Retrying dead-letter publish', { rideId, attempt, error: retryError }),
      );
      logger.error('Ride dead-lettered', { rideId, topic: options.deadLetterTopic });
    }

    await commitOffset(consumer, payload);
  };
}

async function processRide(
  ride: { id: string },
  options: MatchingProcessorOptions,
): Promise<ReturnType<typeof createMatchingOutcome>> {
  await (options.delay ?? (() => wait(5_000 + Math.floor(Math.random() * 5_001)))());
  return (options.createOutcome ?? createMatchingOutcome)(ride);
}

async function retry<T>(
  operation: () => Promise<T>,
  options: MatchingProcessorOptions,
  onRetry: (error: unknown, attempt: number) => void,
): Promise<T> {
  let attempt = 0;

  while (true) {
    try {
      return await operation();
    } catch (error) {
      if (attempt >= options.maxRetries) throw error;
      attempt += 1;
      onRetry(error, attempt);
      await (options.wait ?? wait)(options.retryBackoffMs * attempt);
    }
  }
}

function serializeError(error: unknown): { name: string; message: string } {
  if (error instanceof Error) return { name: error.name, message: error.message };
  return { name: 'Error', message: String(error) };
}
