import type {
  DriverOutcome,
  OutcomeInput,
  OutcomeKind,
  PricingOutcome,
  TripState,
} from '../../domain/trip-state/model.js';

type RecordValue = Record<string, unknown>;

function isRecord(value: unknown): value is RecordValue {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isTimestamp(value: unknown): value is string {
  return typeof value === 'string' && Number.isFinite(Date.parse(value));
}

function parseJson(value: string): unknown {
  return JSON.parse(value) as unknown;
}

function isUnavailableOutcome(value: RecordValue): boolean {
  return (
    value.status === 'unavailable' &&
    isRecord(value.error) &&
    typeof value.error.code === 'string' &&
    typeof value.error.message === 'string'
  );
}

function isDriverSuccess(value: RecordValue): boolean {
  return (
    value.status === 'success' &&
    isRecord(value.driver) &&
    typeof value.driver.id === 'string' &&
    typeof value.driver.name === 'string' &&
    isTimestamp(value.matchedAt)
  );
}

function isPricingSuccess(value: RecordValue): boolean {
  return (
    value.status === 'success' &&
    isRecord(value.price) &&
    typeof value.price.amount === 'number' &&
    Number.isFinite(value.price.amount) &&
    typeof value.price.currency === 'string' &&
    isTimestamp(value.pricedAt)
  );
}

function isOutcome(value: unknown, kind: OutcomeKind): boolean {
  if (!isRecord(value) || !isNonEmptyString(value.rideId)) return false;
  if (isUnavailableOutcome(value)) return true;
  return kind === 'driver' ? isDriverSuccess(value) : isPricingSuccess(value);
}

export function parseOutcome(
  value: string,
  kind: 'driver',
): Extract<OutcomeInput, { kind: 'driver' }>;
export function parseOutcome(
  value: string,
  kind: 'pricing',
): Extract<OutcomeInput, { kind: 'pricing' }>;
export function parseOutcome(value: string, kind: OutcomeKind): OutcomeInput;
export function parseOutcome(value: string, kind: OutcomeKind): OutcomeInput {
  const parsed = parseJson(value);
  if (!isOutcome(parsed, kind)) {
    throw new Error(`Invalid ${kind} outcome record.`);
  }
  if (kind === 'driver')
    return { kind, outcome: parsed as DriverOutcome };
  return { kind, outcome: parsed as PricingOutcome };
}

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
  if (
    'driverOutcome' in value &&
    !isOutcome(value.driverOutcome, 'driver')
  ) {
    return false;
  }
  if (
    'pricingOutcome' in value &&
    !isOutcome(value.pricingOutcome, 'pricing')
  ) {
    return false;
  }
  if (
    'rejectionReason' in value &&
    !isRejectionReason(value.rejectionReason)
  ) {
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
