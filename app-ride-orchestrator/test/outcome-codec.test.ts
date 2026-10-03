import { parseOutcome } from '../src/infrastructure/serialization/outcome-codec.js';

const matchedAt = '2026-10-02T12:00:00.000Z';

describe('parseOutcome', () => {
  it('accepts a driver success and keeps the kind', () => {
    const value = JSON.stringify({
      status: 'success',
      rideId: 'ride-1',
      driver: { id: 'driver-1', name: 'Alex' },
      matchedAt,
    });
    expect(parseOutcome(value, 'driver')).toMatchObject({
      kind: 'driver',
      outcome: { rideId: 'ride-1' },
    });
  });

  it('accepts an unavailable outcome for either kind', () => {
    const value = JSON.stringify({
      status: 'unavailable',
      rideId: 'ride-1',
      error: { code: 'PRICING_UNAVAILABLE', message: 'No quote.' },
    });
    expect(parseOutcome(value, 'pricing').outcome.status).toBe('unavailable');
    expect(parseOutcome(value, 'driver').outcome.status).toBe('unavailable');
  });

  it('rejects a driver success read as a pricing record', () => {
    const value = JSON.stringify({
      status: 'success',
      rideId: 'ride-1',
      driver: { id: 'driver-1', name: 'Alex' },
      matchedAt,
    });
    expect(() => parseOutcome(value, 'pricing')).toThrow(
      'Invalid pricing outcome record.',
    );
  });

  it('rejects a missing rideId, a bad timestamp and a non-finite price', () => {
    expect(() =>
      parseOutcome(
        JSON.stringify({
          status: 'success',
          driver: { id: 'd', name: 'n' },
          matchedAt,
        }),
        'driver',
      ),
    ).toThrow('Invalid driver outcome record.');
    expect(() =>
      parseOutcome(
        JSON.stringify({
          status: 'success',
          rideId: 'ride-1',
          driver: { id: 'd', name: 'n' },
          matchedAt: 'not-a-date',
        }),
        'driver',
      ),
    ).toThrow('Invalid driver outcome record.');
    expect(() =>
      parseOutcome(
        JSON.stringify({
          status: 'success',
          rideId: 'ride-1',
          price: { amount: Number.NaN, currency: 'EUR' },
          pricedAt: matchedAt,
        }),
        'pricing',
      ),
    ).toThrow('Invalid pricing outcome record.');
  });

  it('rejects malformed JSON, arrays and an unavailable record without an error', () => {
    expect(() => parseOutcome('{', 'driver')).toThrow();
    expect(() => parseOutcome('[]', 'driver')).toThrow(
      'Invalid driver outcome record.',
    );
    expect(() =>
      parseOutcome(
        JSON.stringify({ status: 'unavailable', rideId: 'ride-1' }),
        'driver',
      ),
    ).toThrow('Invalid driver outcome record.');
  });
});
