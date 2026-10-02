import { buildApp } from './app.js';
import { shutdownTelemetry } from './instrumentation.js';

const app = buildApp();
const port = Number(process.env.PORT ?? 3000);
const host = process.env.HOST ?? '0.0.0.0';

try {
  await app.listen({ port, host });
} catch (error) {
  app.log.error(error);
  process.exitCode = 1;
}

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, async () => {
    await app.close();
    await shutdownTelemetry();
    process.exit(0);
  });
}
