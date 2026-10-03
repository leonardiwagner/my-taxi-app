import {
  applyOutcome,
  timeoutTrip,
} from '../src/domain/trip-state/transitions.js';

const now = new Date('2026-10-02T12:00:00.000Z');
const driver = {
  status: 'success' as const,
  rideId: 'ride-1',
  driver: { id: 'driver-1', name: 'Alex' },
  matchedAt: now.toISOString(),
};
const pricing = {
  status: 'success' as const,
  rideId: 'ride-1',
  price: { amount: 12.5, currency: 'EUR' },
  pricedAt: now.toISOString(),
};

describe('applyOutcome', () => {
  it('keeps the first result pending and confirms after the other result succeeds', () => {
    const pending = applyOutcome(
      undefined,
      { kind: 'driver', outcome: driver },
      now,
    );
    expect(pending.state.status).toBe('PENDING');
    expect(pending.state.startedAt).toBe(now.toISOString());

    const confirmed = applyOutcome(
      pending.state,
      { kind: 'pricing', outcome: pricing },
      new Date(now.getTime() + 1_000),
    );
    expect(confirmed.state.status).toBe('CONFIRMED');
    expect(confirmed.event).toMatchObject({
      type: 'ride.confirmed',
      rideId: 'ride-1',
      price: pricing.price,
    });
  });

  it('rejects immediately on unavailable and preserves the source error', () => {
    const unavailable = {
      status: 'unavailable' as const,
      rideId: 'ride-1',
      error: { code: 'PRICING_UNAVAILABLE', message: 'No quote.' },
    };
    const rejected = applyOutcome(
      undefined,
      { kind: 'pricing', outcome: unavailable },
      now,
    );
    expect(rejected.state.status).toBe('REJECTED');
    expect(rejected.event).toMatchObject({
      type: 'ride.rejected',
      reason: { source: 'pricing', code: 'PRICING_UNAVAILABLE' },
    });
  });

  it('ignores duplicate result types and results after a final state', () => {
    const pending = applyOutcome(
      undefined,
      { kind: 'driver', outcome: driver },
      now,
    ).state;
    expect(
      applyOutcome(pending, { kind: 'driver', outcome: driver }, now).changed,
    ).toBe(false);
    const confirmed = applyOutcome(
      pending,
      { kind: 'pricing', outcome: pricing },
      now,
    ).state;
    expect(
      applyOutcome(confirmed, { kind: 'driver', outcome: driver }, now).changed,
    ).toBe(false);
  });
});

describe('timeoutTrip', () => {
  it('rejects a pending ride with the timeout reason', () => {
    const pending = applyOutcome(
      undefined,
      { kind: 'driver', outcome: driver },
      now,
    ).state;
    const expired = timeoutTrip(pending, new Date(now.getTime() + 60_000));
    expect(expired.state.status).toBe('REJECTED');
    expect(expired.event).toMatchObject({
      type: 'ride.rejected',
      reason: { source: 'timeout', code: 'TIMEOUT' },
    });
  });
});
