export type OutcomeKind = 'driver' | 'pricing';

export interface DriverSuccess {
  status: 'success';
  rideId: string;
  driver: { id: string; name: string };
  matchedAt: string;
}

export interface PricingSuccess {
  status: 'success';
  rideId: string;
  price: { amount: number; currency: string };
  pricedAt: string;
}

export interface UnavailableOutcome {
  status: 'unavailable';
  rideId: string;
  error: { code: string; message: string };
}

export type DriverOutcome = DriverSuccess | UnavailableOutcome;
export type PricingOutcome = PricingSuccess | UnavailableOutcome;

export type OutcomeInput =
  | { kind: 'driver'; outcome: DriverOutcome }
  | { kind: 'pricing'; outcome: PricingOutcome };
