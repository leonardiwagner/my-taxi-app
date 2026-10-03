import { isTimedOut, timeoutTrip } from '../domain/transitions.js';
import type { Clock, TimingPolicy, TripWriter } from './ports.js';
import type { TripStore } from './trip-store.js';

export async function expirePendingTrips(
  store: TripStore,
  timing: Pick<TimingPolicy, 'timeoutMs'>,
  writer: TripWriter,
  now: Clock,
): Promise<void> {
  const currentTime = now();
  for (const state of store.pending()) {
    if (!isTimedOut(state, currentTime, timing.timeoutMs)) continue;
    const transition = timeoutTrip(state, currentTime);
    await writer.publishTransition(transition.state, transition.event);
    store.put(transition.state);
  }
}
