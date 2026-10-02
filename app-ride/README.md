# app-ride

Generates a ride request every second and publishes it to Kafka.

## Run locally

```sh
npm install
npm run dev
```

The generator connects to `localhost:9092` by default and writes to the `rides`
topic. Configure these values with `KAFKA_BROKERS` (a comma-separated list) and
`KAFKA_RIDE_TOPIC`.

Each message is JSON with a unique ride and customer identifier, a start and
destination from a small list of city routes, and an ISO-8601 request timestamp.

OpenTelemetry auto-instrumentation exports traces over OTLP/HTTP to
`http://localhost:4318` by default. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to use
another collector endpoint.

## Checks

```sh
npm run build
npm run lint
npm run format:check
npm run typecheck
```
