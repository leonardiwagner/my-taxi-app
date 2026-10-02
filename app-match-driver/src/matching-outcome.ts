export interface RideRequest {
  id: string;
}

export type MatchingOutcome =
  | {
      status: 'success';
      rideId: string;
      driver: { id: string; name: string };
      matchedAt: string;
    }
  | {
      status: 'unavailable';
      rideId: string;
      error: { code: 'DRIVER_UNAVAILABLE'; message: string };
      matchedAt: string;
    };

export interface MatchingOutcomeOptions {
  random?: () => number;
  now?: () => Date;
}

export const MOCK_DRIVERS = [
  { id: 'driver-001', name: 'Alex Morgan' },
  { id: 'driver-002', name: 'Sam Taylor' },
  { id: 'driver-003', name: 'Jordan Lee' },
] as const;

const UNAVAILABLE_MESSAGE = 'No drivers are currently available.';

export function createMatchingOutcome(
  ride: RideRequest,
  options: MatchingOutcomeOptions = {},
): MatchingOutcome {
  const random = options.random ?? Math.random;
  const matchedAt = (options.now ?? (() => new Date()))().toISOString();

  if (random() < 0.05) {
    return {
      status: 'unavailable',
      rideId: ride.id,
      error: { code: 'DRIVER_UNAVAILABLE', message: UNAVAILABLE_MESSAGE },
      matchedAt,
    };
  }

  const driver = MOCK_DRIVERS[Math.floor(random() * MOCK_DRIVERS.length)];
  if (!driver) throw new Error('No mock drivers are configured.');

  return { status: 'success', rideId: ride.id, driver, matchedAt };
}

export function parseRideRequest(value: string): RideRequest {
  const parsed: unknown = JSON.parse(value);

  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('id' in parsed) ||
    typeof parsed.id !== 'string' ||
    parsed.id.length === 0
  ) {
    throw new Error('Ride request must contain a non-empty string id.');
  }

  return { id: parsed.id };
}
