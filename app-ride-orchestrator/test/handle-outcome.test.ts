import { handleOutcome } from '../src/application/handle-outcome.js';
import { TripStore } from '../src/application/trip-store.js';
import type { OutcomeInput } from '../src/domain/outcomes.js';
import type { InboundRecord } from '../src/application/ports.js';
import { cursor, FakeTripWriter, silentLogger } from './fake-trip-writer.js';

const now = new Date('2026-10-02T12:00:00.000Z');
const clock = () => now;
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

const accepted = (input: OutcomeInput): InboundRecord => ({
  ok: true,
  cursor: cursor(),
  input,
});

describe('handleOutcome', () => {
  it('writes the transition and stores it only once the write succeeded', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();

    await handleOutcome(
      accepted({ kind: 'driver', outcome: driver }),
      store,
      writer,
      clock,
      silentLogger,
    );

    expect(writer.commits).toHaveLength(1);
    expect(writer.commits[0]?.transition?.changed).toBe(true);
    expect(store.get('ride-1')?.status).toBe('PENDING');
  });

  it('confirms the trip once both outcomes arrive', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();

    await handleOutcome(
      accepted({ kind: 'driver', outcome: driver }),
      store,
      writer,
      clock,
      silentLogger,
    );
    await handleOutcome(
      accepted({ kind: 'pricing', outcome: pricing }),
      store,
      writer,
      clock,
      silentLogger,
    );

    expect(store.get('ride-1')?.status).toBe('CONFIRMED');
    expect(writer.commits[1]?.transition?.event).toMatchObject({
      type: 'ride.confirmed',
    });
  });

  it('commits a duplicate outcome without changing stored state', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();
    const record = accepted({ kind: 'driver', outcome: driver });

    await handleOutcome(record, store, writer, clock, silentLogger);
    const stored = store.get('ride-1');
    await handleOutcome(record, store, writer, clock, silentLogger);

    expect(writer.commits).toHaveLength(2);
    expect(writer.commits[1]?.transition?.changed).toBe(false);
    expect(store.get('ride-1')).toBe(stored);
  });

  it('commits the offset and logs when a record cannot be decoded', async () => {
    const store = new TripStore();
    const writer = new FakeTripWriter();
    const errors: unknown[] = [];

    await handleOutcome(
      {
        ok: false,
        cursor: cursor('12'),
        reason: 'Skipping invalid result record',
        detail: { offset: '11' },
      },
      store,
      writer,
      clock,
      { ...silentLogger, error: (...args: unknown[]) => errors.push(args) },
    );

    expect(writer.commits).toEqual([
      { cursor: cursor('12'), transition: undefined },
    ]);
    expect(writer.transitions).toHaveLength(0);
    expect(store.size).toBe(0);
    expect(errors).toHaveLength(1);
  });
});
