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

OpenTelemetry exports traces over OTLP/HTTP to Tempo at
`http://localhost:4318` by default. Kafka headers carry the W3C trace context
for the generated ride. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to another OTLP/HTTP
base URL (without `/v1/traces`) to use a different collector.

## Run with Docker Compose

From the repository root:

```sh
docker compose up -d --build app-ride
docker compose logs -f app-ride
```

This also starts Kafka and runs `kafka-init`, which creates the topics before
the service starts. The image is built from this directory's `Dockerfile`.
Compose sets `KAFKA_BROKERS=kafka:29092` and
`OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318`; every other variable in the
configuration table uses its default unless you add it to this service's
`environment` in `docker-compose.yml`.

## Checks

```sh
npm run build
npm run lint
npm run format:check
npm run typecheck
```
