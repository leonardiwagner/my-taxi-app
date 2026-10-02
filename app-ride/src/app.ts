import Fastify, { type FastifyInstance } from 'fastify';
import { registerMetrics } from './observability/metrics.js';
import { healthRoutes } from './routes/health.js';
import { rideRoutes } from './routes/rides.js';

export function buildApp(): FastifyInstance {
  const app = Fastify({
    logger: true,
    ajv: {
      customOptions: {
        coerceTypes: false,
      },
    },
  });

  registerMetrics(app);
  app.register(healthRoutes);
  app.register(rideRoutes);

  return app;
}
