import type { TripState } from '../domain/trip.js';
import { SerialQueue } from '../shared/serial-queue.js';
import { createTraceAwareLogger } from '../trace-context.js';
import { expirePendingTrips } from './expire-pending-trips.js';
import { handleOutcome } from './handle-outcome.js';
import type {
  Clock,
  Logger,
  OutcomeSource,
  StateRestore,
  TimingPolicy,
  TripWriter,
} from './ports.js';
import { tombstoneSettledTrips } from './tombstone-settled-trips.js';
import { TripStore } from './trip-store.js';

export interface RideOrchestratorDependencies {
  source: OutcomeSource;
  writer: TripWriter;
  restoreState: StateRestore;
  timing: TimingPolicy;
  now?: Clock;
  logger?: Logger;
}

export class RideOrchestrator {
  private readonly store = new TripStore();
  private readonly serial = new SerialQueue();
  private readonly source: OutcomeSource;
  private readonly writer: TripWriter;
  private readonly restoreState: StateRestore;
  private readonly timing: TimingPolicy;
  private readonly now: Clock;
  private readonly logger: Logger;
  private sweepTimer?: NodeJS.Timeout;

  constructor(dependencies: RideOrchestratorDependencies) {
    this.source = dependencies.source;
    this.writer = dependencies.writer;
    this.restoreState = dependencies.restoreState;
    this.timing = dependencies.timing;
    this.now = dependencies.now ?? (() => new Date());
    this.logger = createTraceAwareLogger(dependencies.logger ?? console);
  }

  async start(): Promise<void> {
    await this.writer.connect();
    await this.source.connect();
    await this.source.run((record) =>
      this.serial.enqueue(() =>
        handleOutcome(record, this.store, this.writer, this.now, this.logger),
      ),
    );

    try {
      await this.restoreState(this.store);
    } catch (error) {
      this.source.abort(error);
      await this.source.stop();
      throw error;
    }
    this.source.release();

    this.sweepTimer = setInterval(() => {
      void this.serial
        .enqueue(() => this.sweep())
        .catch((error) =>
          this.logger.error('Trip maintenance failed', { error }),
        );
    }, this.timing.sweepIntervalMs);
    this.sweepTimer.unref();
  }

  async stop(): Promise<void> {
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    await this.source.stop();
    await this.serial.enqueue(async () => undefined);
    await Promise.all([this.source.disconnect(), this.writer.disconnect()]);
  }

  get states(): ReadonlyMap<string, TripState> {
    return this.store.view;
  }

  private async sweep(): Promise<void> {
    await expirePendingTrips(this.store, this.timing, this.writer, this.now);
    await tombstoneSettledTrips(this.store, this.timing, this.writer, this.now);
  }
}
