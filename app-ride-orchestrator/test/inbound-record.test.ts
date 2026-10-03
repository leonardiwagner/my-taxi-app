import type { EachMessagePayload } from 'kafkajs';
import { decodeOutcomeRecord } from '../src/infrastructure/kafka/inbound-record.js';
import type { TopicMap } from '../src/infrastructure/kafka/topics.js';

const topics: TopicMap = {
  driver: 'driver-matching-results',
  pricing: 'pricing-results',
  state: 'trip.state',
  confirmed: 'ride.confirmed',
  rejected: 'ride.rejected',
};

const driverRecord = {
  status: 'success',
  rideId: 'ride-1',
  driver: { id: 'driver-1', name: 'Alex' },
  matchedAt: '2026-10-02T12:00:00.000Z',
};

const payload = (
  topic: string,
  key: string | null,
  value: string | null,
  offset = '11',
): EachMessagePayload =>
  ({
    topic,
    partition: 3,
    message: {
      offset,
      key: key === null ? null : Buffer.from(key),
      value: value === null ? null : Buffer.from(value),
    },
  }) as unknown as EachMessagePayload;

describe('decodeOutcomeRecord', () => {
  it('routes each input topic to its outcome kind and advances the offset', () => {
    const decoded = decodeOutcomeRecord(
      payload(topics.driver, 'ride-1', JSON.stringify(driverRecord)),
      topics,
    );

    expect(decoded.ok).toBe(true);
    expect(decoded.cursor).toEqual({
      topic: topics.driver,
      partition: 3,
      nextOffset: '12',
    });
    if (decoded.ok) expect(decoded.input.kind).toBe('driver');
  });

  it('reports an invalid record instead of throwing', () => {
    const decoded = decodeOutcomeRecord(
      payload(topics.pricing, 'ride-1', '{'),
      topics,
    );

    expect(decoded.ok).toBe(false);
    if (!decoded.ok) {
      expect(decoded.reason).toBe('Skipping invalid result record');
      expect(decoded.detail).toMatchObject({
        topic: topics.pricing,
        partition: 3,
        offset: '11',
      });
    }
    expect(decoded.cursor.nextOffset).toBe('12');
  });

  it('reports a key that disagrees with the rideId', () => {
    const decoded = decodeOutcomeRecord(
      payload(topics.driver, 'ride-other', JSON.stringify(driverRecord)),
      topics,
    );

    expect(decoded.ok).toBe(false);
    if (!decoded.ok) {
      expect(decoded.reason).toBe(
        'Skipping result whose key does not match rideId',
      );
      expect(decoded.detail).toMatchObject({
        rideIdKey: 'ride-other',
        rideId: 'ride-1',
      });
    }
  });

  it('reports a missing key', () => {
    const decoded = decodeOutcomeRecord(
      payload(topics.driver, null, JSON.stringify(driverRecord)),
      topics,
    );
    expect(decoded.ok).toBe(false);
  });

  it('throws for a topic it was never meant to consume', () => {
    expect(() =>
      decodeOutcomeRecord(payload('something-else', 'ride-1', '{}'), topics),
    ).toThrow('Unexpected result topic: something-else');
  });
});
