import { generateRideRequest, type RideRequest } from './ride-request.js';
import type { RideRequestPublisher } from './ride-request-publisher.js';
import { trace } from '@opentelemetry/api';
import { createTraceAwareLogger } from './trace-context.js';

const tracer = trace.getTracer('app-ride');
const logger = createTraceAwareLogger(console);

interface RideGeneratorOptions {
  intervalMs?: number;
  createRideRequest?: () => RideRequest;
  onError?: (error: unknown) => void;
}

export interface RideGenerator {
  stop(): Promise<void>;
}

export function startRideGenerator(
  publisher: RideRequestPublisher,
  options: RideGeneratorOptions = {},
): RideGenerator {
  const intervalMs = options.intervalMs ?? 1_000;
  const createRide = options.createRideRequest ?? generateRideRequest;
  const onError = options.onError ?? console.error;
  let inFlightPublish: Promise<void> | undefined;

  const publishRide = (): void => {
    if (inFlightPublish) {
      return;
    }

    const rideRequest = createRide();
    inFlightPublish = tracer
      .startActiveSpan(
        'generate ride request',
        {
          attributes: {
            'processing.step': 'generate-ride-request',
            rideId: rideRequest.id,
          },
        },
        async (span) => {
          try {
            logger.info('Generated ride request', {
              ...rideRequest,
              rideId: rideRequest.id,
            });
            await publisher.publish(rideRequest);
          } catch (error) {
            onError(error);
          } finally {
            span.end();
          }
        },
      )
      .finally(() => {
        inFlightPublish = undefined;
      });
  };

  const interval = setInterval(publishRide, intervalMs);

  return {
    async stop(): Promise<void> {
      clearInterval(interval);
      await inFlightPublish;
    },
  };
}
