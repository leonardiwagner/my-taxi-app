import { expirePendingTrips } from '../src/application/expire-pending-trips.js';
import { tombstoneSettledTrips } from '../src/application/tombstone-settled-trips.js';
import { TripStore } from '../src/application/trip-store.js';
import type { TripState } from '../src/domain/trip.js';
import { FakeTripWriter } from './fake-trip-writer.js';

const started = new Date('2026-10-02T12:00:00.000Z');

const pendingTrip = (rideId: string, startedAt = started): TripState => ({
  rideId,
  status: 'PENDING',
  startedAt: startedAt.toISOString(),
  updatedAt: startedAt.toISOString(),
});

const settledTrip = (rideId: string, finalAt = started): TripState => ({
  rideId,
  status: 'REJECTED',
  startedAt: started.toISOString(),
  updatedAt: finalAt.toISOString(),
  finalAt: finalAt.toISOString(),
  rejectionReason: {
    source: 'timeout',
    code: 'TIMEOUT',
    message: 'Ride matching timed out.',
  },
});

describe('expirePendingTrips', () => {
  it('rejects only the pending trips past the deadline', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();
    store.put(pendingTrip('old'));
    store.put(pendingTrip('fresh', new Date(started.getTime() + 59_000)));

    await expirePendingTrips(
      store,
      { timeoutMs: 60_000 },
      writer,
      () => new Date(started.getTime() + 60_000),
    );

    expect(writer.transitions).toHaveLength(1);
    expect(writer.transitions[0]?.event).toMatchObject({
      type: 'ride.rejected',
      rideId: 'old',
      reason: { source: 'timeout', code: 'TIMEOUT' },
    });
    expect(store.get('old')?.status).toBe('REJECTED');
    expect(store.get('fresh')?.status).toBe('PENDING');
  });

  it('leaves settled trips alone', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();
    store.put(settledTrip('done'));

    await expirePendingTrips(
      store,
      { timeoutMs: 0 },
      writer,
      () => new Date(started.getTime() + 600_000),
    );

    expect(writer.transitions).toHaveLength(0);
  });
});

describe('tombstoneSettledTrips', () => {
  it('tombstones and forgets only the settled trips past retention', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();
    store.put(settledTrip('expired'));
    store.put(settledTrip('recent', new Date(started.getTime() + 9_000)));
    store.put(pendingTrip('waiting'));

    await tombstoneSettledTrips(
      store,
      { finalRetentionMs: 10_000 },
      writer,
      () => new Date(started.getTime() + 10_000),
    );

    expect(writer.tombstones).toEqual(['expired']);
    expect(store.get('expired')).toBeUndefined();
    expect(store.get('recent')?.status).toBe('REJECTED');
    expect(store.get('waiting')?.status).toBe('PENDING');
  });

  it('never tombstones a pending trip, whatever the retention', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();
    store.put(pendingTrip('waiting'));

    await tombstoneSettledTrips(
      store,
      { finalRetentionMs: 0 },
      writer,
      () => new Date(started.getTime() + 600_000),
    );

    expect(writer.tombstones).toHaveLength(0);
    expect(store.size).toBe(1);
  });
});
