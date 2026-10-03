import type { TripState } from '../domain/trip.js';

export class TripStore {
  private readonly trips = new Map<string, TripState>();

  get(rideId: string): TripState | undefined {
    return this.trips.get(rideId);
  }

  put(state: TripState): void {
    this.trips.set(state.rideId, state);
  }

  forget(rideId: string): void {
    this.trips.delete(rideId);
  }

  restore(rideId: string, state: TripState | undefined): void {
    if (state === undefined) this.trips.delete(rideId);
    else this.trips.set(rideId, state);
  }

  pending(): TripState[] {
    return [...this.trips.values()].filter(
      (state) => state.status === 'PENDING',
    );
  }

  settled(): TripState[] {
    return [...this.trips.values()].filter(
      (state) => state.status !== 'PENDING',
    );
  }

  get size(): number {
    return this.trips.size;
  }

  get view(): ReadonlyMap<string, TripState> {
    return this.trips;
  }
}
