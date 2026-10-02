# app-pricing

Consumes ride requests from Kafka, waits 1–3 seconds, and publishes a mock pricing outcome. Successful outcomes are generated 95% of the time; expected unavailable outcomes are published normally and do not enter the retry or dead-letter path.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_CONSUMER_GROUP` | `app-pricing` | Consumer group ID |
| `KAFKA_RIDE_TOPIC` | `rides` | Input topic |
| `KAFKA_PRICING_RESULTS_TOPIC` | `pricing-results` | Pricing outcome topic |
| `KAFKA_PRICING_DEAD_LETTER_TOPIC` | `pricing-dead-letter` | Topic for rides that fail processing or result publication |
| `PRICING_MAX_RETRIES` | `3` | Retries after the initial attempt |
| `PRICING_RETRY_BACKOFF_MS` | `250` | Base linear retry delay in milliseconds |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Optional OpenTelemetry collector base URL |

Create the input, results, and dead-letter topics before starting the service. For each input ride, the result is keyed by ride ID and has one of these shapes:

```json
{"status":"success","rideId":"ride-123","price":{"amount":42.5,"currency":"EUR"},"pricedAt":"2026-10-02T12:00:00.000Z"}
```

```json
{"status":"unavailable","rideId":"ride-123","error":{"code":"PRICING_UNAVAILABLE","message":"Pricing is temporarily unavailable."},"pricedAt":"2026-10-02T12:00:00.000Z"}
```

If processing or publishing a result exhausts its retries, the original key and value plus failure details are published to the dead-letter topic. The input offset is committed only after publishing the result or dead-letter record succeeds. This provides at-least-once delivery, so consumers should tolerate duplicate results after a crash between publish and offset commit.

## Run

```sh
npm install
npm run build
npm start
```

`npm run dev` starts the service with TypeScript watch mode. `SIGINT` and `SIGTERM` stop consumption, wait for any active message handler, and disconnect Kafka cleanly.
