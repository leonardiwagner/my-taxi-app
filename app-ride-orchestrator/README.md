# app-ride-orchestrator

Combines driver matching and pricing outcomes into a durable trip state and emits a final ride confirmation or rejection. The service correlates exclusively by Kafka key and `rideId`. Run one service instance because trip state is held in memory and coordinated through one Kafka consumer group.

## Topics

Create all topics before starting the service. Configure the `trip.state` topic with `cleanup.policy=compact`; use the same partition count for all topics and key every record by `rideId`. The service writes state and final outputs in Kafka transactions and commits each consumed result offset in the same transaction.

| Topic                     | Purpose                                    |
| ------------------------- | ------------------------------------------ |
| `driver-matching-results` | Driver matching outcome input              |
| `pricing-results`         | Pricing outcome input                      |
| `trip.state`              | Compacted trip state, including tombstones |
| `ride.confirmed`          | Final confirmed ride events                |
| `ride.rejected`           | Final rejected ride events                 |

Driver input success: `{"status":"success","rideId":"ride-123","driver":{"id":"driver-001","name":"Alex Morgan"},"matchedAt":"2026-10-02T12:00:00.000Z"}`. Driver unavailable inputs and pricing unavailable inputs include `{"status":"unavailable","rideId":"ride-123","error":{"code":"...","message":"..."}}`. Pricing success inputs include `{"status":"success","rideId":"ride-123","price":{"amount":42.5,"currency":"EUR"},"pricedAt":"2026-10-02T12:00:00.000Z"}`.

The service restores `trip.state` from the beginning before it subscribes to result topics. A pending trip keeps its first `startedAt`; duplicate result types and outcomes after a final state are ignored. An unavailable result rejects immediately with its source reason. Two successful outcomes confirm the ride. Pending rides time out after 60 seconds and emit a rejection with reason `TIMEOUT`.

Confirmed output shape:

```json
{
  "type": "ride.confirmed",
  "rideId": "ride-123",
  "confirmedAt": "2026-10-02T12:00:00.000Z",
  "driver": { "id": "driver-001", "name": "Alex Morgan" },
  "price": { "amount": 42.5, "currency": "EUR" }
}
```

Rejected output shape:

```json
{
  "type": "ride.rejected",
  "rideId": "ride-123",
  "rejectedAt": "2026-10-02T12:00:00.000Z",
  "reason": {
    "source": "pricing",
    "code": "PRICING_UNAVAILABLE",
    "message": "Pricing is temporarily unavailable."
  }
}
```

Final states are removed from memory and tombstoned from the compacted state topic after the configured retention period.

## Configuration

| Variable                      | Default                   | Purpose                                                |
| ----------------------------- | ------------------------- | ------------------------------------------------------ |
| `KAFKA_BROKERS`               | `localhost:9092`          | Comma-separated Kafka broker addresses                 |
| `KAFKA_CONSUMER_GROUP`        | `app-ride-orchestrator`   | Consumer group and transactional producer identity     |
| `KAFKA_DRIVER_RESULTS_TOPIC`  | `driver-matching-results` | Driver matching input topic                            |
| `KAFKA_PRICING_RESULTS_TOPIC` | `pricing-results`         | Pricing input topic                                    |
| `KAFKA_TRIP_STATE_TOPIC`      | `trip.state`              | Compacted state topic                                  |
| `KAFKA_RIDE_CONFIRMED_TOPIC`  | `ride.confirmed`          | Confirmed ride output topic                            |
| `KAFKA_RIDE_REJECTED_TOPIC`   | `ride.rejected`           | Rejected ride output topic                             |
| `TRIP_FINAL_RETENTION_MS`     | `86400000`                | Time to keep final states before tombstoning           |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318`   | OTLP/HTTP collector base URL; `/v1/traces` is appended |

## Run

```sh
npm install
npm run build
npm start
```

`npm run dev` starts TypeScript watch mode. `SIGINT` and `SIGTERM` stop consumption, wait for active work, disconnect Kafka, and shut down telemetry.

The orchestrator extracts W3C trace context from result-topic headers and records
`join-ride-outcome` spans with the `rideId`, input topic, and processing step.
State and final-event records include the active trace context in their Kafka
headers.
