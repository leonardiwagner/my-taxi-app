import type { RideEvent } from '../src/domain/ride-events.js';
import type { Transition } from '../src/domain/transitions.js';
import type { TripState } from '../src/domain/trip.js';
import type { InputCursor, TripWriter } from '../src/application/ports.js';

export interface RecordedCommit {
  cursor: InputCursor;
  transition?: Transition;
}

export interface RecordedTransition {
  state: TripState;
  event?: RideEvent;
}

export class FakeTripWriter implements TripWriter {
  readonly commits: RecordedCommit[] = [];
  readonly transitions: RecordedTransition[] = [];
  readonly tombstones: string[] = [];
  connected = false;

  async connect(): Promise<void> {
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }

  async commitOutcome(
    cursor: InputCursor,
    transition?: Transition,
  ): Promise<void> {
    this.commits.push({ cursor, transition });
  }

  async publishTransition(state: TripState, event?: RideEvent): Promise<void> {
    this.transitions.push({ state, event });
  }

  async publishTombstone(rideId: string): Promise<void> {
    this.tombstones.push(rideId);
  }
}

export const cursor = (offset = '7'): InputCursor => ({
  topic: 'driver-matching-results',
  partition: 0,
  nextOffset: offset,
});

export const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};
