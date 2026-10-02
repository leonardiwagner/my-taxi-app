# app-ride

Fastify HTTP API scaffold for the ride service.

## Run locally

```sh
npm install
npm run dev
```

The API listens on `0.0.0.0:3000` by default. Set `HOST` or `PORT` to override
those values.

## Endpoints

- `GET /health` reports that the process is responding.
- `GET /ready` reports that the service is ready to receive traffic.
- `GET /metrics` exposes Prometheus process and HTTP request metrics.

## Observability

Fastify's Pino logger writes structured logs. OpenTelemetry auto-instrumentation
collects traces and exports them over OTLP/HTTP to `http://localhost:4318` by
default. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to use another collector endpoint.

Prometheus can scrape `/metrics`. The HTTP metrics use route templates to keep
label cardinality bounded.

## Checks

```sh
npm test
npm run build
npm run lint
npm run format:check
npm run typecheck
```
