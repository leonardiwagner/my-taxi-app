import type { OutcomeInput } from '../domain/outcomes.js';
import type { RideEvent } from '../domain/ride-events.js';
import type { Transition } from '../domain/transitions.js';
import type { TripState } from '../domain/trip.js';
import type { TripStore } from './trip-store.js';

export type Clock = () => Date;

export type Logger = Pick<Console, 'info' | 'warn' | 'error'>;

export interface TimingPolicy {
  timeoutMs: number;
  finalRetentionMs: number;
  sweepIntervalMs: number;
  replayTimeoutMs: number;
  heartbeatIntervalMs: number;
}

export interface InputCursor {
  topic: string;
  partition: number;
  nextOffset: string;
}

export type InboundRecord =
  | { ok: true; cursor: InputCursor; input: OutcomeInput }
  | {
      ok: false;
      cursor: InputCursor;
      reason: string;
      detail: Record<string, unknown>;
    };

export interface TripWriter {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  commitOutcome(cursor: InputCursor, transition?: Transition): Promise<void>;
  publishTransition(state: TripState, event?: RideEvent): Promise<void>;
  publishTombstone(rideId: string): Promise<void>;
}

export interface OutcomeSource {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  run(consume: (record: InboundRecord) => Promise<void>): Promise<void>;
  stop(): Promise<void>;
  release(): void;
  abort(error: unknown): void;
}

export type StateRestore = (store: TripStore) => Promise<void>;
