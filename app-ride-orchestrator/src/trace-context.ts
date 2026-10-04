import {
  context,
  isSpanContextValid,
  propagation,
  trace,
  type Span,
} from '@opentelemetry/api';
import type { EachMessagePayload, IHeaders } from 'kafkajs';

type StructuredLogger = Pick<Console, 'info' | 'warn' | 'error'>;

export function createTraceAwareLogger(logger: StructuredLogger): StructuredLogger {
  const logContext = (
    fields?: Record<string, unknown>,
  ): Record<string, unknown> | undefined => {
    const spanContext = trace.getActiveSpan()?.spanContext();
    if (!spanContext || !isSpanContextValid(spanContext)) return fields;

    return {
      ...fields,
      trace_id: spanContext.traceId,
    };
  };

  const log = (
    method: keyof StructuredLogger,
    message: string,
    fields?: Record<string, unknown>,
  ): void => {
    const context = logContext(fields);
    if (context) logger[method](message, context);
    else logger[method](message);
  };

  return {
    info: (message, fields) => log('info', message, fields),
    warn: (message, fields) => log('warn', message, fields),
    error: (message, fields) => log('error', message, fields),
  };
}

export function injectTraceHeaders(): IHeaders {
  const headers: IHeaders = {};
  propagation.inject(context.active(), headers, {
    set(carrier, key, value) {
      carrier[key] = value;
    },
  });
  return headers;
}

export function withConsumerSpan<T>(
  payload: EachMessagePayload,
  service: string,
  step: string,
  work: (span: Span) => Promise<T>,
): Promise<T> {
  const carrier = Object.fromEntries(
    Object.entries(payload.message.headers ?? {}).map(([key, value]) => [
      key,
      Array.isArray(value) ? value[0]?.toString() : value?.toString(),
    ]),
  );
  const parent = propagation.extract(context.active(), carrier);
  const rideId = payload.message.key?.toString() ?? 'unknown';

  return trace.getTracer(service).startActiveSpan(
    `${service} ${step}`,
    {
      attributes: {
        'service.name': service,
        'messaging.system': 'kafka',
        'messaging.destination.name': payload.topic,
        'messaging.operation.type': 'process',
        'processing.step': step,
        rideId,
      },
    },
    parent,
    async (span) => {
      try {
        return await work(span);
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
}

export function setRideId(span: Span, rideId: string): void {
  span.setAttribute('rideId', rideId);
}
