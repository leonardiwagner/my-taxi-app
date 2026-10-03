import type { TripState } from '../../domain/trip.js';
import {
  isNonEmptyString,
  isRecord,
  isTimestamp,
  parseJson,
} from './json-guards.js';
import { isDriverOutcome, isPricingOutcome } from './outcome-codec.js';

function isRejectionReason(value: unknown): boolean {
  return (
    isRecord(value) &&
    (value.source === 'driver-matching' ||
      value.source === 'pricing' ||
      value.source === 'timeout') &&
    typeof value.code === 'string' &&
    typeof value.message === 'string'
  );
}

function isTripState(value: unknown): value is TripState {
  if (
    !isRecord(value) ||
    !isNonEmptyString(value.rideId) ||
    (value.status !== 'PENDING' &&
      value.status !== 'CONFIRMED' &&
      value.status !== 'REJECTED') ||
    !isTimestamp(value.startedAt) ||
    !isTimestamp(value.updatedAt)
  ) {
    return false;
  }

  if ('finalAt' in value && !isTimestamp(value.finalAt)) return false;
  if ('driverOutcome' in value && !isDriverOutcome(value.driverOutcome)) {
    return false;
  }
  if ('pricingOutcome' in value && !isPricingOutcome(value.pricingOutcome)) {
    return false;
  }
  if ('rejectionReason' in value && !isRejectionReason(value.rejectionReason)) {
    return false;
  }

  if (
    ('driverOutcome' in value &&
      isRecord(value.driverOutcome) &&
      value.driverOutcome.rideId !== value.rideId) ||
    ('pricingOutcome' in value &&
      isRecord(value.pricingOutcome) &&
      value.pricingOutcome.rideId !== value.rideId)
  ) {
    return false;
  }

  if (value.status === 'CONFIRMED') {
    return (
      isTimestamp(value.finalAt) &&
      isRecord(value.driverOutcome) &&
      value.driverOutcome.status === 'success' &&
      isRecord(value.pricingOutcome) &&
      value.pricingOutcome.status === 'success'
    );
  }
  if (value.status === 'REJECTED') {
    return (
      isTimestamp(value.finalAt) && isRejectionReason(value.rejectionReason)
    );
  }
  if ('finalAt' in value || 'rejectionReason' in value) return false;
  return true;
}

export function parseTripState(value: string): TripState {
  const parsed = parseJson(value);
  if (!isTripState(parsed)) throw new Error('Trip state record is invalid.');
  return parsed;
}
