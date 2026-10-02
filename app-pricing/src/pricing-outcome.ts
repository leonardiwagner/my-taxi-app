export interface RideRequest {
  id: string;
}

export type PricingOutcome =
  | {
      status: 'success';
      rideId: string;
      price: { amount: number; currency: string };
      pricedAt: string;
    }
  | {
      status: 'unavailable';
      rideId: string;
      error: { code: 'PRICING_UNAVAILABLE'; message: string };
      pricedAt: string;
    };

export interface PricingOutcomeOptions {
  random?: () => number;
  now?: () => Date;
  currency?: string;
  minimumPrice?: number;
  maximumPrice?: number;
}

const UNAVAILABLE_MESSAGE = 'Pricing is temporarily unavailable.';

export function createPricingOutcome(
  ride: RideRequest,
  options: PricingOutcomeOptions = {},
): PricingOutcome {
  const random = options.random ?? Math.random;
  const now = options.now ?? (() => new Date());
  const pricedAt = now().toISOString();

  if (random() < 0.05) {
    return {
      status: 'unavailable',
      rideId: ride.id,
      error: { code: 'PRICING_UNAVAILABLE', message: UNAVAILABLE_MESSAGE },
      pricedAt,
    };
  }

  const minimumPrice = options.minimumPrice ?? 10;
  const maximumPrice = options.maximumPrice ?? 100;
  const amount = Math.round((minimumPrice + random() * (maximumPrice - minimumPrice)) * 100) / 100;

  return {
    status: 'success',
    rideId: ride.id,
    price: { amount, currency: options.currency ?? 'EUR' },
    pricedAt,
  };
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
