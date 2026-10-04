import { jest } from '@jest/globals';
import { NodeSDK } from '@opentelemetry/sdk-node';
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { startRideGenerator } from '../src/ride-generator.js';
import { createRideRequestPublisher } from '../src/ride-request-publisher.js';

const exporter = new InMemorySpanExporter();
const sdk = new NodeSDK({
  serviceName: 'app-ride',
  spanProcessors: [new SimpleSpanProcessor(exporter)],
  instrumentations: [],
});

describe('ride request tracing', () => {
  beforeAll(() => sdk.start());
  afterAll(async () => sdk.shutdown());

  it('publishes W3C trace headers and records ride attributes', async () => {
    const send = jest.fn().mockResolvedValue([{ partition: 0 }]);
    const info = jest.spyOn(console, 'info').mockImplementation(() => undefined);
    const publisher = createRideRequestPublisher({ send } as never, 'rides');
    const ride = {
      id: 'ride-123',
      customer: { id: 'customer-123' },
      startingPoint: 'Station',
      destination: 'Airport',
      requestedAt: new Date().toISOString(),
    };

    await publisher.publish(ride);
    for (
      let attempts = 0;
      attempts < 100 && exporter.getFinishedSpans().length === 0;
      attempts += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    const message = send.mock.calls[0]?.[0].messages[0];
    const traceparent = message.headers.traceparent.toString();
    expect(traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    const span = exporter.getFinishedSpans()[0];
    expect(span?.attributes).toMatchObject({
      'service.name': 'app-ride',
      'messaging.destination.name': 'rides',
      'processing.step': 'generate-ride-request',
      rideId: 'ride-123',
    });
    const publishedLog = info.mock.calls.find(
      ([message]) => message === 'Published ride request to Kafka',
    );
    expect(publishedLog?.[1]).toMatchObject({
      trace_id: span?.spanContext().traceId,
      rideId: 'ride-123',
    });
    info.mockRestore();
  });

  it('adds trace and span IDs to the generated ride log', async () => {
    exporter.reset();
    const info = jest.spyOn(console, 'info').mockImplementation(() => undefined);
    let markPublished!: () => void;
    const published = new Promise<void>((resolve) => {
      markPublished = resolve;
    });
    const ride = {
      id: 'ride-456',
      customer: { id: 'customer-123' },
      startingPoint: 'Station',
      destination: 'Airport',
      requestedAt: new Date().toISOString(),
    };
    const generator = startRideGenerator(
      { publish: async () => markPublished() },
      { intervalMs: 10, createRideRequest: () => ride },
    );

    await published;
    await generator.stop();

    const span = exporter.getFinishedSpans()[0];
    const generatedLog = info.mock.calls.find(
      ([message]) => message === 'Generated ride request',
    );
    expect(generatedLog?.[1]).toMatchObject({
      trace_id: span?.spanContext().traceId,
      rideId: 'ride-456',
    });
    info.mockRestore();
  });
});
