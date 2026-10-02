import type { Consumer, Kafka, Producer } from 'kafkajs';
import type { TripState } from '../trip-state.js';
import { handleResult } from './result-handler.js';
import type { OrchestratorOptions } from './orchestrator-options.js';
import { SerialQueue } from './serial-queue.js';
import { replayTripStates } from './state-replay.js';
import { expireAndCleanTrips } from './trip-maintenance.js';
import { TripStateWriter } from './trip-state-writer.js';

const pause = (milliseconds: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

export class RideOrchestrator {
  private readonly trips = new Map<string, TripState>();
  private readonly serial = new SerialQueue();
  private readonly writer: TripStateWriter;
  private timeoutTimer?: NodeJS.Timeout;

  constructor(
    private readonly kafka: Kafka,
    private readonly consumer: Consumer,
    producer: Producer,
    private readonly options: OrchestratorOptions,
  ) {
    this.writer = new TripStateWriter(producer, options);
  }

  async start(): Promise<void> {
    await this.writer.connect();
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
        await this.serial.enqueue(() =>
          handleResult(
            payload,
            this.trips,
            this.options,
            this.writer,
            () => this.now(),
            this.logger,
          ),
        );
      },
    });
    try {
      await replayTripStates(this.kafka, this.options, this.trips, this.logger);
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
      void this.serial
        .enqueue(() =>
          expireAndCleanTrips(this.trips, this.options, this.writer, () =>
            this.now(),
          ),
        )
        .catch((error) =>
          this.logger.error('Trip maintenance failed', { error }),
        );
    }, 1_000);
    this.timeoutTimer.unref();
  }

  async stop(): Promise<void> {
    if (this.timeoutTimer) clearInterval(this.timeoutTimer);
    await this.consumer.stop();
    await this.serial.enqueue(async () => undefined);
    await Promise.all([this.consumer.disconnect(), this.writer.disconnect()]);
  }

  get states(): ReadonlyMap<string, TripState> {
    return this.trips;
  }

  private get logger(): Pick<Console, 'info' | 'warn' | 'error'> {
    return this.options.logger ?? console;
  }

  private now(): Date {
    return (this.options.now ?? (() => new Date()))();
  }
}
