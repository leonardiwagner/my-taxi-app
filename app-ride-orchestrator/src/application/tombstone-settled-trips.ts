import { isRetentionElapsed } from '../domain/transitions.js';
import type { Clock, TimingPolicy, TripWriter } from './ports.js';
import type { TripStore } from './trip-store.js';

export async function tombstoneSettledTrips(
  store: TripStore,
  timing: Pick<TimingPolicy, 'finalRetentionMs'>,
  writer: TripWriter,
  now: Clock,
): Promise<void> {
  const currentTime = now();
  for (const state of store.settled()) {
    if (!isRetentionElapsed(state, currentTime, timing.finalRetentionMs))
      continue;
    await writer.publishTombstone(state.rideId);
    store.forget(state.rideId);
  }
}
