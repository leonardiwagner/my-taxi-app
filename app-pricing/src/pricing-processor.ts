import type { Consumer, EachMessagePayload, Producer } from 'kafkajs';
import { createPricingOutcome, parseRideRequest } from './pricing-outcome.js';
import {
  createTraceAwareLogger,
  injectTraceHeaders,
  setRideId,
  withConsumerSpan,
} from './trace-context.js';

export interface PricingProcessorOptions {
  inputTopic: string;
  resultsTopic: string;
  deadLetterTopic: string;
  maxRetries: number;
  retryBackoffMs: number;
  delay?: () => Promise<void>;
  createOutcome?: typeof createPricingOutcome;
  publishResult?: (
    producer: Producer,
    topic: string,
    key: string,
    value: string,
  ) => Promise<void>;
  publishDeadLetter?: (
    producer: Producer,
    topic: string,
    key: string,
    value: string,
  ) => Promise<void>;
  commitOffset?: (
    consumer: Consumer,
    payload: EachMessagePayload,
  ) => Promise<void>;
  wait?: (milliseconds: number) => Promise<void>;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

const wait = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

async function sendRecord(
  producer: Producer,
  topic: string,
  key: string,
  value: string,
): Promise<void> {
  await producer.send({
    topic,
    messages: [{ key, value, headers: injectTraceHeaders() }],
  });
}

export function createPricingProcessor(
  consumer: Consumer,
  producer: Producer,
  options: PricingProcessorOptions,
): (payload: EachMessagePayload) => Promise<void> {
  const logger = createTraceAwareLogger(options.logger ?? console);
  const publishResult = options.publishResult ?? sendRecord;
  const publishDeadLetter = options.publishDeadLetter ?? sendRecord;
  const commitOffset =
    options.commitOffset ??
    (async (currentConsumer, payload) => {
      await currentConsumer.commitOffsets([
        {
          topic: payload.topic,
          partition: payload.partition,
          offset: (BigInt(payload.message.offset) + 1n).toString(),
        },
      ]);
    });

  return async (payload): Promise<void> =>
    withConsumerSpan(payload, 'app-pricing', 'price-ride', async (span) => {
      const messageValue = payload.message.value?.toString();
      const messageKey = payload.message.key?.toString();
      const fallbackRideId = messageKey ?? 'unknown';
      let rideId = fallbackRideId;

      try {
        const ride = parseRideRequest(messageValue ?? '');
        rideId = ride.id;
        setRideId(span, rideId);
        let outcome: ReturnType<typeof createPricingOutcome> | undefined;

        await retry(
          async () => {
            outcome = await processRide(ride, options);
          },
          options,
          (error, attempt) =>
            logger.warn('Retrying ride processing', { rideId, attempt, error }),
        );

        if (!outcome) throw new Error('Pricing completed without an outcome.');
        await retry(
          () =>
            publishResult(
              producer,
              options.resultsTopic,
              ride.id,
              JSON.stringify(outcome),
            ),
          options,
          (error, attempt) =>
            logger.warn('Retrying pricing result publish', {
              rideId,
              attempt,
              error,
            }),
        );

        if (outcome.status === 'success') {
          logger.info('Ride priced successfully', {
            rideId,
            price: outcome.price,
          });
        } else {
          logger.info('Pricing unavailable for ride', {
            rideId,
            error: outcome.error,
          });
        }
      } catch (error) {
        logger.error('Pricing failed after retries', { rideId, error });
        const deadLetter = JSON.stringify({
          rideId,
          originalKey: messageKey,
          originalValue: messageValue ?? null,
          error: serializeError(error),
          failedAt: new Date().toISOString(),
        });
        await retry(
          () =>
            publishDeadLetter(
              producer,
              options.deadLetterTopic,
              rideId,
              deadLetter,
            ),
          options,
          (retryError, attempt) =>
            logger.warn('Retrying dead-letter publish', {
              rideId,
              attempt,
              error: retryError,
            }),
        );
        logger.error('Ride dead-lettered', {
          rideId,
          topic: options.deadLetterTopic,
        });
      }

      await commitOffset(consumer, payload);
    });
}

async function processRide(
  ride: { id: string },
  options: PricingProcessorOptions,
): Promise<ReturnType<typeof createPricingOutcome>> {
  await (options.delay ??
    (() => wait(1_000 + Math.floor(Math.random() * 2_001)))());
  return (options.createOutcome ?? createPricingOutcome)(ride);
}

async function retry(
  operation: () => Promise<void>,
  options: PricingProcessorOptions,
  onRetry: (error: unknown, attempt: number) => void,
): Promise<void> {
  let attempt = 0;

  while (true) {
    try {
      await operation();
      return;
    } catch (error) {
      if (attempt >= options.maxRetries) {
        throw error;
      }

      attempt += 1;
      onRetry(error, attempt);
      await (options.wait ?? wait)(options.retryBackoffMs * attempt);
    }
  }
}

function serializeError(error: unknown): { name: string; message: string } {
  if (error instanceof Error) {
    return { name: error.name, message: error.message };
  }

  return { name: 'Error', message: String(error) };
}

export function createEachMessageHandler(
  consumer: Consumer,
  producer: Producer,
  options: PricingProcessorOptions,
): (payload: EachMessagePayload) => Promise<void> {
  return createPricingProcessor(consumer, producer, options);
}
