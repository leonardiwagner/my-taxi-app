import type { Producer, Transaction } from 'kafkajs';
import { TransactionalTripWriter } from '../src/infrastructure/kafka/transactional-trip-writer.js';

const topics = {
  state: 'trip.state',
  confirmed: 'ride.confirmed',
  rejected: 'ride.rejected',
} as never;

function fakeProducer(options: { sendError?: Error; commitError?: Error }) {
  const calls = { commit: 0, abort: 0 };
  const transaction = {
    send: async () => {
      if (options.sendError) throw options.sendError;
    },
    sendOffsets: async () => undefined,
    commit: async () => {
      calls.commit += 1;
      if (options.commitError) throw options.commitError;
    },
    abort: async () => {
      calls.abort += 1;
      if (calls.commit > 0)
        throw new Error('Cannot call "abort" in state "COMMITTING"');
    },
  };
  const producer = {
    transaction: async () => transaction as unknown as Transaction,
  } as unknown as Producer;
  return { producer, calls };
}

describe('TransactionalTripWriter', () => {
  it('surfaces the commit error instead of masking it with abort', async () => {
    const commitError = new Error('endTxn failed');
    const { producer, calls } = fakeProducer({ commitError });
    const writer = new TransactionalTripWriter(producer, topics, 'group');

    await expect(writer.publishTombstone('ride-1')).rejects.toBe(commitError);
    expect(calls.abort).toBe(0);
  });

  it('aborts when work fails before the commit starts', async () => {
    const sendError = new Error('send failed');
    const { producer, calls } = fakeProducer({ sendError });
    const writer = new TransactionalTripWriter(producer, topics, 'group');

    await expect(writer.publishTombstone('ride-1')).rejects.toBe(sendError);
    expect(calls).toEqual({ commit: 0, abort: 1 });
  });
});
