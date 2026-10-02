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
export type TripOutcome = DriverOutcome | PricingOutcome;
export type TripStatus = 'PENDING' | 'CONFIRMED' | 'REJECTED';

export interface TripState {
  rideId: string;
  status: TripStatus;
  startedAt: string;
  updatedAt: string;
  finalAt?: string;
  driverOutcome?: DriverOutcome;
  pricingOutcome?: PricingOutcome;
  rejectionReason?: {
    source: 'driver-matching' | 'pricing' | 'timeout';
    code: string;
    message: string;
  };
}

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
      reason: NonNullable<TripState['rejectionReason']>;
    };

export interface Transition {
  state: TripState;
  event?: RideEvent;
  changed: boolean;
}

export function applyOutcome(
  current: TripState | undefined,
  kind: OutcomeKind,
  outcome: TripOutcome,
  now: Date,
): Transition {
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

  if (kind === 'driver') state.driverOutcome = outcome as DriverOutcome;
  else state.pricingOutcome = outcome as PricingOutcome;
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

export function parseOutcome(value: string, kind: OutcomeKind): TripOutcome {
  const parsed: unknown = JSON.parse(value);
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('rideId' in parsed) ||
    typeof parsed.rideId !== 'string' ||
    !parsed.rideId
  ) {
    throw new Error('Outcome must contain a non-empty rideId.');
  }
  if (
    !('status' in parsed) ||
    (parsed.status !== 'success' && parsed.status !== 'unavailable')
  ) {
    throw new Error('Outcome status must be success or unavailable.');
  }
  if (parsed.status === 'unavailable') {
    if (
      !('error' in parsed) ||
      typeof parsed.error !== 'object' ||
      parsed.error === null ||
      !('code' in parsed.error) ||
      !('message' in parsed.error) ||
      typeof parsed.error.code !== 'string' ||
      typeof parsed.error.message !== 'string'
    ) {
      throw new Error(
        'Unavailable outcome must contain an error code and message.',
      );
    }
    return parsed as UnavailableOutcome;
  }
  if (
    kind === 'driver' &&
    (!('driver' in parsed) ||
      typeof parsed.driver !== 'object' ||
      parsed.driver === null ||
      !('id' in parsed.driver) ||
      !('name' in parsed.driver) ||
      typeof parsed.driver.id !== 'string' ||
      typeof parsed.driver.name !== 'string')
  ) {
    throw new Error('Successful driver outcome must contain a driver.');
  }
  if (
    kind === 'pricing' &&
    (!('price' in parsed) ||
      typeof parsed.price !== 'object' ||
      parsed.price === null ||
      !('amount' in parsed.price) ||
      !('currency' in parsed.price) ||
      typeof parsed.price.amount !== 'number' ||
      typeof parsed.price.currency !== 'string')
  ) {
    throw new Error('Successful pricing outcome must contain a price.');
  }
  return parsed as TripOutcome;
}

export function parseTripState(value: string): TripState {
  const parsed: unknown = JSON.parse(value);
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('rideId' in parsed) ||
    typeof parsed.rideId !== 'string' ||
    !('status' in parsed) ||
    !['PENDING', 'CONFIRMED', 'REJECTED'].includes(String(parsed.status)) ||
    !('startedAt' in parsed) ||
    typeof parsed.startedAt !== 'string'
  ) {
    throw new Error('Trip state record is invalid.');
  }
  return parsed as TripState;
}
