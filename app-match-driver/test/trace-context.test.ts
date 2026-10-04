import type { EachMessagePayload } from 'kafkajs';
import { NodeSDK } from '@opentelemetry/sdk-node';
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import {
  createTraceAwareLogger,
  injectTraceHeaders,
  setRideId,
  withConsumerSpan,
} from '../src/trace-context.js';

const exporter = new InMemorySpanExporter();
const sdk = new NodeSDK({
  serviceName: 'app-match-driver',
  spanProcessors: [new SimpleSpanProcessor(exporter)],
  instrumentations: [],
});

function payload(
  headers: EachMessagePayload['message']['headers'] = {},
): EachMessagePayload {
  return {
    topic: 'rides',
    partition: 0,
    message: {
      key: Buffer.from('ride-123'),
      value: Buffer.from('{}'),
      headers,
      offset: '0',
      timestamp: '0',
      attributes: 0,
    },
    heartbeat: async () => undefined,
    pause: () => () => undefined,
  } as EachMessagePayload;
}

describe('driver matching trace context', () => {
  beforeAll(() => sdk.start());
  afterAll(async () => sdk.shutdown());

  it('extracts Kafka trace context and propagates it with ride attributes', async () => {
    const incomingTraceId = '11111111111111111111111111111111';
    const incomingParentId = '2222222222222222';
    const outgoing: Record<string, string> = {};
    const logged: Array<{ message: string; fields?: Record<string, unknown> }> = [];
    const logger = createTraceAwareLogger({
      info: (message, fields) => logged.push({ message, fields }),
      warn: (message, fields) => logged.push({ message, fields }),
      error: (message, fields) => logged.push({ message, fields }),
    });

    await withConsumerSpan(
      payload({ traceparent: `00-${incomingTraceId}-${incomingParentId}-01` }),
      'app-match-driver',
      'match-driver',
      async (span) => {
        setRideId(span, 'ride-123');
        Object.assign(outgoing, injectTraceHeaders());
        logger.info('Driver matched to ride', { rideId: 'ride-123' });
      },
    );
    for (
      let attempts = 0;
      attempts < 100 && exporter.getFinishedSpans().length === 0;
      attempts += 1
    ) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    const span = exporter.getFinishedSpans()[0];
    expect(span?.spanContext().traceId).toBe(incomingTraceId);
    expect(span?.parentSpanContext?.spanId).toBe(incomingParentId);
    expect(span?.attributes).toMatchObject({
      'service.name': 'app-match-driver',
      'messaging.destination.name': 'rides',
      'processing.step': 'match-driver',
      rideId: 'ride-123',
    });
    expect(outgoing.traceparent).toMatch(
      new RegExp(
        `^00-${incomingTraceId}-(?!${incomingParentId})[0-9a-f]{16}-01$`,
      ),
    );
    expect(logged[0]?.fields).toMatchObject({
      trace_id: incomingTraceId,
      rideId: 'ride-123',
    });
  });
});
