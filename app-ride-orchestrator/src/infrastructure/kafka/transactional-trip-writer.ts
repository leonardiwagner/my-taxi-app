import type { Producer, Transaction } from 'kafkajs';
import type { RideEvent } from '../../domain/ride-events.js';
import type { Transition } from '../../domain/transitions.js';
import type { TripState } from '../../domain/trip.js';
import type { InputCursor, TripWriter } from '../../application/ports.js';
import type { TopicMap } from './topics.js';
import { injectTraceHeaders } from '../../trace-context.js';

export class TransactionalTripWriter implements TripWriter {
  constructor(
    private readonly producer: Producer,
    private readonly topics: TopicMap,
    private readonly consumerGroup: string,
  ) {}

  connect(): Promise<void> {
    return this.producer.connect();
  }

  disconnect(): Promise<void> {
    return this.producer.disconnect();
  }

  async commitOutcome(
    cursor: InputCursor,
    transition?: Transition,
  ): Promise<void> {
    await this.inTransaction(async (transaction) => {
      if (transition?.changed)
        await this.writeTransition(
          transaction,
          transition.state,
          transition.event,
        );
      await transaction.sendOffsets({
        consumerGroupId: this.consumerGroup,
        topics: [
          {
            topic: cursor.topic,
            partitions: [
              { partition: cursor.partition, offset: cursor.nextOffset },
            ],
          },
        ],
      });
    });
  }

  async publishTransition(
    state: TripState,
    event: RideEvent | undefined,
  ): Promise<void> {
    await this.inTransaction((transaction) =>
      this.writeTransition(transaction, state, event),
    );
  }

  async publishTombstone(rideId: string): Promise<void> {
    await this.inTransaction(async (transaction) => {
      await transaction.send({
        topic: this.topics.state,
        messages: [{ key: rideId, value: null, headers: injectTraceHeaders() }],
      });
    });
  }

  private async inTransaction(
    work: (transaction: Transaction) => Promise<unknown>,
  ): Promise<void> {
    const transaction = await this.producer.transaction();
    let committing = false;
    try {
      await work(transaction);
      committing = true;
      await transaction.commit();
    } catch (error) {
      // kafkajs only allows abort from TRANSACTING; once commit has started, a failure
      // leaves the producer in COMMITTING and abort would mask the original error.
      if (!committing) await transaction.abort();
      throw error;
    }
  }

  private async writeTransition(
    transaction: Transaction,
    state: TripState,
    event: RideEvent | undefined,
  ): Promise<void> {
    await transaction.send({
      topic: this.topics.state,
      messages: [
        {
          key: state.rideId,
          value: JSON.stringify(state),
          headers: injectTraceHeaders(),
        },
      ],
    });
    if (event) {
      const topic =
        event.type === 'ride.confirmed'
          ? this.topics.confirmed
          : this.topics.rejected;
      await transaction.send({
        topic,
        messages: [
          {
            key: state.rideId,
            value: JSON.stringify(event),
            headers: injectTraceHeaders(),
          },
        ],
      });
    }
  }
}
