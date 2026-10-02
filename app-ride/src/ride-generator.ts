import { generateRideRequest, type RideRequest } from './ride-request.js';
import type { RideRequestPublisher } from './ride-request-publisher.js';

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
    console.info('Generated ride request', rideRequest);

    inFlightPublish = publisher.publish(rideRequest).catch(onError).finally(() => {
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
