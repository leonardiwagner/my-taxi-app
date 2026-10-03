import { loadConfig } from '../src/config.js';

describe('loadConfig', () => {
  it('applies the documented defaults', () => {
    const config = loadConfig({});

    expect(config.brokers).toEqual(['localhost:9092']);
    expect(config.consumerGroup).toBe('app-ride-orchestrator');
    expect(config.topics).toEqual({
      driver: 'driver-matching-results',
      pricing: 'pricing-results',
      state: 'trip.state',
      confirmed: 'ride.confirmed',
      rejected: 'ride.rejected',
    });
    expect(config.timing.timeoutMs).toBe(60_000);
    expect(config.timing.finalRetentionMs).toBe(86_400_000);
  });

  it('trims and splits the broker list', () => {
    expect(loadConfig({ KAFKA_BROKERS: 'a:9092, b:9092 ,' }).brokers).toEqual([
      'a:9092',
      'b:9092',
    ]);
  });

  it('reads the overridable topics and retention', () => {
    const config = loadConfig({
      KAFKA_TRIP_STATE_TOPIC: 'trips',
      KAFKA_RIDE_CONFIRMED_TOPIC: 'ok',
      TRIP_FINAL_RETENTION_MS: '5000',
    });

    expect(config.topics.state).toBe('trips');
    expect(config.topics.confirmed).toBe('ok');
    expect(config.timing.finalRetentionMs).toBe(5_000);
  });

  it('rejects an empty broker list and a negative or unparsable retention', () => {
    expect(() => loadConfig({ KAFKA_BROKERS: ' , ' })).toThrow(
      'Kafka brokers and a non-negative final retention period are required.',
    );
    expect(() => loadConfig({ TRIP_FINAL_RETENTION_MS: '-1' })).toThrow();
    expect(() => loadConfig({ TRIP_FINAL_RETENTION_MS: 'soon' })).toThrow();
  });
});
