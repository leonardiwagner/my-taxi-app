import Fastify, { type FastifyInstance } from 'fastify';
import { registerMetrics } from './observability/metrics.js';
import { healthRoutes } from './routes/health.js';

export function buildApp(): FastifyInstance {
  const app = Fastify({ logger: true });

  registerMetrics(app);
  app.register(healthRoutes);

  return app;
}
