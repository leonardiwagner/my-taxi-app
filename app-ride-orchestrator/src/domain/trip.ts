import type { DriverOutcome, PricingOutcome } from './outcomes.js';

export type TripStatus = 'PENDING' | 'CONFIRMED' | 'REJECTED';

export interface RejectionReason {
  source: 'driver-matching' | 'pricing' | 'timeout';
  code: string;
  message: string;
}

export interface TripState {
  rideId: string;
  status: TripStatus;
  startedAt: string;
  updatedAt: string;
  finalAt?: string;
  driverOutcome?: DriverOutcome;
  pricingOutcome?: PricingOutcome;
  rejectionReason?: RejectionReason;
}
