# app-match-driver

Consumes ride requests from Kafka, waits a randomly selected 5–10 seconds, and publishes a mock driver matching outcome. A driver is selected from a small mock set for 95% of rides. The other 5% receive a normal unavailable result and do not enter the retry or dead-letter path.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_CONSUMER_GROUP` | `app-match-driver` | Consumer group ID |
| `KAFKA_RIDE_TOPIC` | `rides` | Input topic |
| `KAFKA_MATCHING_RESULTS_TOPIC` | `driver-matching-results` | Matching outcome topic |
| `KAFKA_MATCHING_DEAD_LETTER_TOPIC` | `driver-matching-dead-letter` | Topic for rides that fail processing or result publication |
| `MATCHING_MAX_RETRIES` | `3` | Retries after the initial attempt |
| `MATCHING_RETRY_BACKOFF_MS` | `250` | Base linear retry delay in milliseconds |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP collector base URL; `/v1/traces` is appended |

Create the input, results, and dead-letter topics before starting the service. Outcome messages are keyed by ride ID and use one of these shapes:

The service extracts W3C trace context from Kafka headers, records a
`match-driver` span with the `rideId` and input topic, and injects the context
into result and dead-letter records.

```json
{"status":"success","rideId":"ride-123","driver":{"id":"driver-001","name":"Alex Morgan"},"matchedAt":"2026-10-02T12:00:00.000Z"}
```

```json
{"status":"unavailable","rideId":"ride-123","error":{"code":"DRIVER_UNAVAILABLE","message":"No drivers are currently available."},"matchedAt":"2026-10-02T12:00:00.000Z"}
```

If processing or publishing a result exhausts its retries, the original key and value plus failure details are published to the dead-letter topic. The input offset is committed only after publishing the result or dead-letter record succeeds. This provides at-least-once delivery, so consumers should tolerate duplicate outcomes after a crash between publish and offset commit.

## Run

```sh
npm install
npm run build
npm start
```

`npm run dev` starts the service with TypeScript watch mode. `SIGINT` and `SIGTERM` stop consumption, wait for any active message handler, and disconnect Kafka cleanly.
