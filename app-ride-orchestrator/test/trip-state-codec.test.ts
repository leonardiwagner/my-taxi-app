import { parseTripState } from '../src/infrastructure/serialization/trip-state-codec.js';

const at = '2026-10-02T12:00:00.000Z';
const driver = {
  status: 'success',
  rideId: 'ride-1',
  driver: { id: 'driver-1', name: 'Alex' },
  matchedAt: at,
};
const pricing = {
  status: 'success',
  rideId: 'ride-1',
  price: { amount: 12.5, currency: 'EUR' },
  pricedAt: at,
};

const parse = (state: Record<string, unknown>) =>
  parseTripState(JSON.stringify(state));

describe('parseTripState', () => {
  it('accepts a pending snapshot carrying one outcome', () => {
    expect(
      parse({
        rideId: 'ride-1',
        status: 'PENDING',
        startedAt: at,
        updatedAt: at,
        driverOutcome: driver,
      }),
    ).toMatchObject({ rideId: 'ride-1', status: 'PENDING' });
  });

  it('accepts confirmed and rejected snapshots', () => {
    expect(
      parse({
        rideId: 'ride-1',
        status: 'CONFIRMED',
        startedAt: at,
        updatedAt: at,
        finalAt: at,
        driverOutcome: driver,
        pricingOutcome: pricing,
      }).status,
    ).toBe('CONFIRMED');
    expect(
      parse({
        rideId: 'ride-1',
        status: 'REJECTED',
        startedAt: at,
        updatedAt: at,
        finalAt: at,
        rejectionReason: {
          source: 'timeout',
          code: 'TIMEOUT',
          message: 'Ride matching timed out.',
        },
      }).status,
    ).toBe('REJECTED');
  });

  it('rejects a pending snapshot that carries final fields', () => {
    expect(() =>
      parse({
        rideId: 'ride-1',
        status: 'PENDING',
        startedAt: at,
        updatedAt: at,
        finalAt: at,
      }),
    ).toThrow('Trip state record is invalid.');
  });

  it('rejects a confirmed snapshot missing a successful outcome', () => {
    expect(() =>
      parse({
        rideId: 'ride-1',
        status: 'CONFIRMED',
        startedAt: at,
        updatedAt: at,
        finalAt: at,
        driverOutcome: driver,
      }),
    ).toThrow('Trip state record is invalid.');
  });

  it('rejects a rejected snapshot without a rejection reason', () => {
    expect(() =>
      parse({
        rideId: 'ride-1',
        status: 'REJECTED',
        startedAt: at,
        updatedAt: at,
        finalAt: at,
      }),
    ).toThrow('Trip state record is invalid.');
  });

  it('rejects an embedded outcome whose rideId disagrees with the trip', () => {
    expect(() =>
      parse({
        rideId: 'ride-2',
        status: 'PENDING',
        startedAt: at,
        updatedAt: at,
        driverOutcome: driver,
      }),
    ).toThrow('Trip state record is invalid.');
  });

  it('rejects an unknown status and a bad timestamp', () => {
    expect(() =>
      parse({
        rideId: 'ride-1',
        status: 'CANCELLED',
        startedAt: at,
        updatedAt: at,
      }),
    ).toThrow('Trip state record is invalid.');
    expect(() =>
      parse({
        rideId: 'ride-1',
        status: 'PENDING',
        startedAt: 'nope',
        updatedAt: at,
      }),
    ).toThrow('Trip state record is invalid.');
  });
});
