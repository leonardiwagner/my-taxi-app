import type { DriverSuccess, PricingSuccess } from './outcomes.js';
import type { RejectionReason } from './trip.js';

export type RideEvent =
  | {
      type: 'ride.confirmed';
      rideId: string;
      confirmedAt: string;
      driver: DriverSuccess['driver'];
      price: PricingSuccess['price'];
    }
  | {
      type: 'ride.rejected';
      rideId: string;
      rejectedAt: string;
      reason: RejectionReason;
    };
