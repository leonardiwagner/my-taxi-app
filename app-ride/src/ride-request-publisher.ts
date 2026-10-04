import type { Producer } from 'kafkajs';
import { trace } from '@opentelemetry/api';
import {
  createTraceAwareLogger,
  injectTraceHeaders,
} from './trace-context.js';
import type { RideRequest } from './ride-request.js';

const logger = createTraceAwareLogger(console);

export interface RideRequestPublisher {
  publish(rideRequest: RideRequest): Promise<void>;
}

export function createRideRequestPublisher(
  producer: Producer,
  topic: string,
): RideRequestPublisher {
  return {
    async publish(rideRequest): Promise<void> {
      await trace.getTracer('app-ride').startActiveSpan(
        'publish ride request',
        {
          attributes: {
            'service.name': 'app-ride',
            'messaging.system': 'kafka',
            'messaging.destination.name': topic,
            'messaging.operation.type': 'publish',
            'processing.step': 'generate-ride-request',
            rideId: rideRequest.id,
          },
        },
        async (span) => {
          try {
            const result = await producer.send({
              topic,
              messages: [
                {
                  key: rideRequest.id,
                  value: JSON.stringify(rideRequest),
                  headers: injectTraceHeaders(),
                },
              ],
            });
            logger.info('Published ride request to Kafka', {
              rideId: rideRequest.id,
              topic,
              partitions: result.map(({ partition }) => partition),
            });
          } catch (error) {
            span.recordException(
              error instanceof Error ? error : new Error(String(error)),
            );
            span.setStatus({ code: 2 });
            throw error;
          } finally {
            span.end();
          }
        },
      );
    },
  };
}
