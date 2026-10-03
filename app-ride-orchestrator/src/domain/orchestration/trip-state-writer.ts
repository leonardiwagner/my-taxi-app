import type { EachMessagePayload, Producer, Transaction } from 'kafkajs';
import type {
  RideEvent,
  TripState,
  Transition,
} from '../trip-state/model.js';
import type { OrchestratorOptions } from './orchestrator-options.js';

export class TripStateWriter {
  constructor(
    private readonly producer: Producer,
    private readonly options: OrchestratorOptions,
  ) {}

  connect(): Promise<void> {
    return this.producer.connect();
  }

  disconnect(): Promise<void> {
    return this.producer.disconnect();
  }

  async commitOutcome(
    payload: EachMessagePayload,
    transition: Transition,
  ): Promise<void> {
    const transaction = await this.producer.transaction();
    try {
      if (transition.changed)
        await this.writeTransition(
          transaction,
          transition.state,
          transition.event,
        );
      await transaction.sendOffsets({
        consumerGroupId: this.options.consumerGroup,
        topics: [
          {
            topic: payload.topic,
            partitions: [
              {
                partition: payload.partition,
                offset: (BigInt(payload.message.offset) + 1n).toString(),
              },
            ],
          },
        ],
      });
      await transaction.commit();
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }

  async commitInputOffset(payload: EachMessagePayload): Promise<void> {
    const transaction = await this.producer.transaction();
    try {
      await transaction.sendOffsets({
        consumerGroupId: this.options.consumerGroup,
        topics: [
          {
            topic: payload.topic,
            partitions: [
              {
                partition: payload.partition,
                offset: (BigInt(payload.message.offset) + 1n).toString(),
              },
            ],
          },
        ],
      });
      await transaction.commit();
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }

  async publishTransition(
    state: TripState,
    event: RideEvent | undefined,
  ): Promise<void> {
    const transaction = await this.producer.transaction();
    try {
      await this.writeTransition(transaction, state, event);
      await transaction.commit();
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }

  async publishTombstone(rideId: string): Promise<void> {
    const transaction = await this.producer.transaction();
    try {
      await transaction.send({
        topic: this.options.stateTopic,
        messages: [{ key: rideId, value: null }],
      });
      await transaction.commit();
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }

  private async writeTransition(
    transaction: Transaction,
    state: TripState,
    event: RideEvent | undefined,
  ): Promise<void> {
    await transaction.send({
      topic: this.options.stateTopic,
      messages: [{ key: state.rideId, value: JSON.stringify(state) }],
    });
    if (event) {
      const topic =
        event.type === 'ride.confirmed'
          ? this.options.confirmedTopic
          : this.options.rejectedTopic;
      await transaction.send({
        topic,
        messages: [{ key: state.rideId, value: JSON.stringify(event) }],
      });
    }
  }
}
