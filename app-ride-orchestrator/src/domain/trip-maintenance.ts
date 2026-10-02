import { timeoutTrip, type TripState } from '../trip-state.js';
import type { OrchestratorOptions } from './orchestrator-options.js';
import { TripStateWriter } from './trip-state-writer.js';

export async function expireAndCleanTrips(
  trips: Map<string, TripState>,
  options: OrchestratorOptions,
  writer: TripStateWriter,
  now: () => Date,
): Promise<void> {
  const currentTime = now();
  for (const [rideId, state] of trips) {
    if (
      state.status === 'PENDING' &&
      currentTime.getTime() - Date.parse(state.startedAt) >= options.timeoutMs
    ) {
      const transition = timeoutTrip(state, currentTime);
      await writer.publishTransition(transition.state, transition.event);
      trips.set(rideId, transition.state);
    } else if (
      state.status !== 'PENDING' &&
      state.finalAt &&
      currentTime.getTime() - Date.parse(state.finalAt) >=
        options.finalRetentionMs
    ) {
      await writer.publishTombstone(rideId);
      trips.delete(rideId);
    }
  }
}
