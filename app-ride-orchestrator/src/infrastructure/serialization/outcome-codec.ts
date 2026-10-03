import type {
  DriverOutcome,
  OutcomeInput,
  OutcomeKind,
  PricingOutcome,
} from '../../domain/outcomes.js';
import {
  isNonEmptyString,
  isRecord,
  isTimestamp,
  parseJson,
  type RecordValue,
} from './json-guards.js';

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

export function isDriverOutcome(value: unknown): value is DriverOutcome {
  if (!isRecord(value) || !isNonEmptyString(value.rideId)) return false;
  return isUnavailableOutcome(value) || isDriverSuccess(value);
}

export function isPricingOutcome(value: unknown): value is PricingOutcome {
  if (!isRecord(value) || !isNonEmptyString(value.rideId)) return false;
  return isUnavailableOutcome(value) || isPricingSuccess(value);
}

export function parseOutcome(value: string, kind: OutcomeKind): OutcomeInput {
  const parsed = parseJson(value);
  if (kind === 'driver') {
    if (!isDriverOutcome(parsed))
      throw new Error('Invalid driver outcome record.');
    return { kind, outcome: parsed };
  }
  if (!isPricingOutcome(parsed))
    throw new Error('Invalid pricing outcome record.');
  return { kind, outcome: parsed };
}
