import type { FastifyInstance } from 'fastify';
import {
  collectDefaultMetrics,
  Counter,
  Histogram,
  Registry,
} from '@prometheus-io/client';

const registry = new Registry();
registry.setDefaultLabels({ service: 'app-ride' });
collectDefaultMetrics({ register: registry });

const requestCount = new Counter({
  name: 'http_server_requests_total',
  help: 'Total number of completed HTTP requests',
  labelNames: ['method', 'route', 'status_code'] as const,
  registers: [registry],
});

const requestDuration = new Histogram({
  name: 'http_server_request_duration_seconds',
  help: 'Duration of completed HTTP requests in seconds',
  labelNames: ['method', 'route', 'status_code'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export function registerMetrics(app: FastifyInstance): void {
  app.addHook('onResponse', async (request, reply) => {
    const labels = {
      method: request.method,
      route: request.routeOptions.url ?? 'unmatched',
      status_code: String(reply.statusCode),
    };

    requestCount.inc(labels);
    requestDuration.observe(labels, reply.elapsedTime / 1000);
  });

  app.get('/metrics', async (_request, reply) => {
    reply.header('content-type', registry.contentType);
    return registry.metrics();
  });
}
