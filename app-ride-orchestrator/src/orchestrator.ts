import type {
  Consumer,
  EachMessagePayload,
  Kafka,
  Producer,
  Transaction,
} from 'kafkajs';
import {
  applyOutcome,
  parseOutcome,
  parseTripState,
  timeoutTrip,
  type OutcomeKind,
  type RideEvent,
  type TripOutcome,
  type TripState,
} from './trip-state.js';

export interface OrchestratorOptions {
  driverTopic: string;
  pricingTopic: string;
  stateTopic: string;
  confirmedTopic: string;
  rejectedTopic: string;
  consumerGroup: string;
  timeoutMs: number;
  finalRetentionMs: number;
  now?: () => Date;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

const pause = (milliseconds: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

export class RideOrchestrator {
  private readonly trips = new Map<string, TripState>();
  private serial: Promise<void> = Promise.resolve();
  private timeoutTimer?: NodeJS.Timeout;

  constructor(
    private readonly kafka: Kafka,
    private readonly consumer: Consumer,
    private readonly producer: Producer,
    private readonly options: OrchestratorOptions,
  ) {}

  async start(): Promise<void> {
    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: [this.options.driverTopic, this.options.pricingTopic],
      fromBeginning: false,
    });
    let releaseStateReplay!: () => void;
    let stateReplayComplete = false;
    let stateReplayError: unknown;
    const stateReady = new Promise<void>((resolve) => {
      releaseStateReplay = resolve;
    });
    await this.consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      eachMessage: async (payload) => {
        while (!stateReplayComplete) {
          await Promise.race([stateReady, pause(5_000)]);
          if (!stateReplayComplete) await payload.heartbeat();
        }
        if (stateReplayError) throw stateReplayError;
        await this.enqueue(() => this.handleResult(payload));
      },
    });
    try {
      await this.replayState();
    } catch (error) {
      stateReplayError = error;
      stateReplayComplete = true;
      releaseStateReplay();
      await this.consumer.stop();
      throw error;
    }
    stateReplayComplete = true;
    releaseStateReplay();
    this.timeoutTimer = setInterval(() => {
      void this.enqueue(() => this.expireAndCleanTrips()).catch((error) =>
        this.logger.error('Trip maintenance failed', { error }),
      );
    }, 1_000);
    this.timeoutTimer.unref();
  }

  async stop(): Promise<void> {
    if (this.timeoutTimer) clearInterval(this.timeoutTimer);
    await this.consumer.stop();
    await this.enqueue(async () => undefined);
    await Promise.all([this.consumer.disconnect(), this.producer.disconnect()]);
  }

  get states(): ReadonlyMap<string, TripState> {
    return this.trips;
  }

  private get logger(): Pick<Console, 'info' | 'warn' | 'error'> {
    return this.options.logger ?? console;
  }

  private enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.serial.then(operation);
    this.serial = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async replayState(): Promise<void> {
    const admin = this.kafka.admin();
    const replay = this.kafka.consumer({
      groupId: `${this.options.consumerGroup}-state-replay-${process.pid}-${Date.now()}`,
      allowAutoTopicCreation: false,
    });
    await Promise.all([admin.connect(), replay.connect()]);
    try {
      const ends = await admin.fetchTopicOffsets(this.options.stateTopic);
      const targets = new Map(
        ends.map(({ partition, offset }) => [partition, BigInt(offset)]),
      );
      const reached = new Set(
        ends
          .filter(({ offset }) => BigInt(offset) === 0n)
          .map(({ partition }) => partition),
      );
      let resolveReplay!: () => void;
      const complete = new Promise<void>((resolve) => {
        resolveReplay = resolve;
      });
      const markComplete = (partition: number, nextOffset: bigint) => {
        const target = targets.get(partition);
        if (target !== undefined && nextOffset >= target)
          reached.add(partition);
        if (reached.size === targets.size) resolveReplay();
      };
      if (reached.size === targets.size) return;

      await replay.subscribe({
        topic: this.options.stateTopic,
        fromBeginning: true,
      });
      await replay.run({
        autoCommit: false,
        partitionsConsumedConcurrently: 1,
        eachMessage: async ({ partition, message }) => {
          const rideId = message.key?.toString();
          if (rideId) {
            const value = message.value?.toString();
            if (value === undefined) this.trips.delete(rideId);
            else this.trips.set(rideId, parseTripState(value));
          }
          markComplete(partition, BigInt(message.offset) + 1n);
        },
      });
      await Promise.race([
        complete,
        pause(60_000).then(() => {
          throw new Error(`Timed out replaying ${this.options.stateTopic}.`);
        }),
      ]);
      await replay.stop();
      this.logger.info('Restored trip state', {
        rides: this.trips.size,
        topic: this.options.stateTopic,
      });
    } finally {
      await Promise.allSettled([replay.disconnect(), admin.disconnect()]);
    }
  }

  private async handleResult(payload: EachMessagePayload): Promise<void> {
    const kind = this.kindForTopic(payload.topic);
    const rideIdKey = payload.message.key?.toString();
    let outcome: TripOutcome;
    try {
      outcome = parseOutcome(payload.message.value?.toString() ?? '', kind);
    } catch (error) {
      this.logger.error('Skipping invalid result record', {
        topic: payload.topic,
        partition: payload.partition,
        offset: payload.message.offset,
        error,
      });
      await this.commitInputOffset(payload);
      return;
    }
    if (!rideIdKey || rideIdKey !== outcome.rideId) {
      this.logger.error('Skipping result whose key does not match rideId', {
        topic: payload.topic,
        partition: payload.partition,
        offset: payload.message.offset,
        rideIdKey,
        rideId: outcome.rideId,
      });
      await this.commitInputOffset(payload);
      return;
    }

    const transition = applyOutcome(
      this.trips.get(outcome.rideId),
      kind,
      outcome,
      this.now(),
    );
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
      if (transition.changed) this.trips.set(outcome.rideId, transition.state);
      if (transition.event)
        this.logger.info('Trip reached final state', {
          rideId: outcome.rideId,
          status: transition.state.status,
        });
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }

  private async commitInputOffset(payload: EachMessagePayload): Promise<void> {
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

  private async expireAndCleanTrips(): Promise<void> {
    const now = this.now();
    for (const [rideId, state] of this.trips) {
      if (
        state.status === 'PENDING' &&
        now.getTime() - Date.parse(state.startedAt) >= this.options.timeoutMs
      ) {
        const transition = timeoutTrip(state, now);
        await this.publishTransition(transition.state, transition.event);
        this.trips.set(rideId, transition.state);
      } else if (
        state.status !== 'PENDING' &&
        state.finalAt &&
        now.getTime() - Date.parse(state.finalAt) >=
          this.options.finalRetentionMs
      ) {
        await this.publishTombstone(rideId);
        this.trips.delete(rideId);
      }
    }
  }

  private async publishTransition(
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

  private async publishTombstone(rideId: string): Promise<void> {
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

  private kindForTopic(topic: string): OutcomeKind {
    if (topic === this.options.driverTopic) return 'driver';
    if (topic === this.options.pricingTopic) return 'pricing';
    throw new Error(`Unexpected result topic: ${topic}`);
  }

  private now(): Date {
    return (this.options.now ?? (() => new Date()))();
  }
}
