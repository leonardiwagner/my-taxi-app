import type { OutcomeInput } from './outcomes.js';
import type { RideEvent } from './ride-events.js';
import type { TripState } from './trip.js';

export interface Transition {
  state: TripState;
  event?: RideEvent;
  changed: boolean;
}

export function applyOutcome(
  current: TripState | undefined,
  input: OutcomeInput,
  now: Date,
): Transition {
  const { kind, outcome } = input;
  const timestamp = now.toISOString();
  if (current && current.status !== 'PENDING')
    return { state: current, changed: false };

  const state: TripState = current
    ? { ...current }
    : {
        rideId: outcome.rideId,
        status: 'PENDING',
        startedAt: timestamp,
        updatedAt: timestamp,
      };
  if (
    (kind === 'driver' && state.driverOutcome) ||
    (kind === 'pricing' && state.pricingOutcome)
  ) {
    return { state, changed: false };
  }

  if (kind === 'driver') state.driverOutcome = outcome;
  else state.pricingOutcome = outcome;
  state.updatedAt = timestamp;

  if (outcome.status === 'unavailable') {
    const source = kind === 'driver' ? 'driver-matching' : 'pricing';
    state.status = 'REJECTED';
    state.finalAt = timestamp;
    state.rejectionReason = { source, ...outcome.error };
    return {
      state,
      changed: true,
      event: {
        type: 'ride.rejected',
        rideId: state.rideId,
        rejectedAt: timestamp,
        reason: state.rejectionReason,
      },
    };
  }

  if (
    state.driverOutcome?.status === 'success' &&
    state.pricingOutcome?.status === 'success'
  ) {
    state.status = 'CONFIRMED';
    state.finalAt = timestamp;
    return {
      state,
      changed: true,
      event: {
        type: 'ride.confirmed',
        rideId: state.rideId,
        confirmedAt: timestamp,
        driver: state.driverOutcome.driver,
        price: state.pricingOutcome.price,
      },
    };
  }

  return { state, changed: true };
}

export function timeoutTrip(current: TripState, now: Date): Transition {
  if (current.status !== 'PENDING') return { state: current, changed: false };
  const timestamp = now.toISOString();
  const reason = {
    source: 'timeout' as const,
    code: 'TIMEOUT',
    message: 'Ride matching timed out.',
  };
  const state: TripState = {
    ...current,
    status: 'REJECTED',
    finalAt: timestamp,
    updatedAt: timestamp,
    rejectionReason: reason,
  };
  return {
    state,
    changed: true,
    event: {
      type: 'ride.rejected',
      rideId: current.rideId,
      rejectedAt: timestamp,
      reason,
    },
  };
}

export function isTimedOut(
  state: TripState,
  now: Date,
  timeoutMs: number,
): boolean {
  return (
    state.status === 'PENDING' &&
    now.getTime() - Date.parse(state.startedAt) >= timeoutMs
  );
}

export function isRetentionElapsed(
  state: TripState,
  now: Date,
  retentionMs: number,
): boolean {
  return (
    state.status !== 'PENDING' &&
    state.finalAt !== undefined &&
    now.getTime() - Date.parse(state.finalAt) >= retentionMs
  );
}
