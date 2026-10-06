# my-taxi-app

An event-driven taxi-ride pipeline built from four independent Node.js /
TypeScript microservices that communicate only through Apache Kafka. A ride
request is generated, priced and matched to a driver in parallel, then combined
into a durable trip state that ends as a confirmed or rejected ride.

## Architecture

```
                         ┌──────────────────┐
                         │     app-ride     │  generates 1 ride/second
                         └────────┬─────────┘
                                  │ rides
                 ┌────────────────┴────────────────┐
                 ▼                                 ▼
        ┌─────────────────┐              ┌────────────────────┐
        │   app-pricing   │              │  app-match-driver  │
        └────────┬────────┘              └──────────┬─────────┘
                 │ pricing-results                  │ driver-matching-results
                 │ (+ pricing-dead-letter)          │ (+ driver-matching-dead-letter)
                 └────────────────┬─────────────────┘
                                  ▼
                     ┌────────────────────────┐
                     │ app-ride-orchestrator  │  in-memory trip state,
                     │  single instance only  │  restored from trip.state
                     └────────────┬───────────┘
                                  │
            trip.state (compacted) │ ride.confirmed / ride.rejected
```

Each service is a self-contained npm package. There is no root `package.json`,
no workspace and no shared library: install and run commands from inside the
service directory.

| Service                                            | Role                                                                                     |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| [app-ride](app-ride/)                              | Publishes a synthetic ride request to `rides` every second.                              |
| [app-pricing](app-pricing/)                        | Prices a ride after 1–3 s; 5% come back `unavailable`.                                   |
| [app-match-driver](app-match-driver/)              | Matches a driver after 5–10 s; 5% come back `unavailable`.                               |
| [app-ride-orchestrator](app-ride-orchestrator/)    | Joins both outcomes into a trip, emits the final confirmation or rejection, times out at 60 s. |

Rules that hold across the pipeline: every record is keyed by `rideId`; topics
are never auto-created; consumers commit offsets manually. The two workers are
at-least-once (offset committed after the result is published), so duplicates
are possible. The orchestrator is exactly-once for its own state and outputs:
state write, final event and input-offset commit all happen in one Kafka
transaction.

Per-service message shapes, environment variables and failure semantics live in
each service's README — start with
[app-ride-orchestrator/README.md](app-ride-orchestrator/README.md) for the trip
state machine.

## Prerequisites

- Docker with Compose v2 (required for both options)
- Node.js >= 22 and `npm` 10+ (only when running services on your machine)

## Quick start

There are two ways to run the pipeline. Both start Kafka, Kafka UI, Tempo and
Grafana from [docker-compose.yml](docker-compose.yml); they differ only in where
the four services run.

### Option A: everything in Docker Compose

```sh
docker compose up -d --build
```

This builds one image per service, creates the topics with `kafka-init`, and
starts `app-ride`, `app-pricing`, `app-match-driver` and
`app-ride-orchestrator` once Kafka is healthy. Kafka UI is on
<http://localhost:8080>, Grafana on <http://localhost:3000> (local login `admin` /
`admin`). Tempo stores traces in the `tempo-data` Compose volume and retains them
for 24 hours.

```sh
docker compose logs -f app-ride app-pricing app-match-driver app-ride-orchestrator
docker compose up -d --build app-pricing   # rebuild and restart one service
docker compose down                        # stop; add -v to delete the Tempo and Grafana volumes
```

Keep `app-ride-orchestrator` at one replica (the default). Do not run a service
on your machine at the same time as its Compose container: both join the same
consumer group, so they split the work.

### Option B: services on your machine

Start the infrastructure in Docker:

```sh
docker compose up -d kafka kafka-init kafka-ui tempo grafana
```

Kafka is then on `localhost:9092`. Then run the services in four terminals, one
per service directory:

```sh
cd app-ride && npm install && npm run dev
cd app-pricing && npm install && npm run dev
cd app-match-driver && npm install && npm run dev
cd app-ride-orchestrator && npm install && npm run dev
```

`npm run dev` runs TypeScript directly with `tsx watch`. For a production-style
run use `npm run build && npm start`. Start the orchestrator last (or restart it
freely — it replays `trip.state` before consuming results), and run only **one**
orchestrator instance: its trip state is in memory and coordinated through a
single consumer group.

### Where the services connect

| Setting                       | On your machine (default) | In Docker Compose        |
| ----------------------------- | ------------------------- | ------------------------ |
| `KAFKA_BROKERS`               | `localhost:9092`          | `kafka:29092`            |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318`   | `http://tempo:4318`      |

Kafka advertises two listeners: `localhost:9092` for clients on the host and
`kafka:29092` for clients on the Compose network. The Compose file sets the
second column for each service; change the `environment` of a service there to
point it elsewhere.

### Topics

`kafka-init` creates every topic on startup in both options, because auto
creation is disabled in every client. Its commands are idempotent, so it is safe
to run `docker compose up -d kafka-init` again. Use the same partition count for
all topics — records are keyed by `rideId`, and the orchestrator relies on
per-key ordering.

`trip.state` **must** be compacted: it is the orchestrator's durable state and
is replayed from the beginning on every startup. Verify with:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic trip.state
```

### Find a ride trace

Services export traces to Tempo: `http://localhost:4318` from your machine, and
`http://tempo:4318` inside Compose (see the table above). Each Kafka publish
injects W3C `traceparent` headers and each consumer extracts them, so the spans
for a ride share its trace ID. The `rideId` span attribute is also indexed for
TraceQL search.

Ride processing logs include `trace_id` alongside `rideId`. Copy it from a log
into Grafana's Tempo trace ID search to open the trace.

Open the provisioned **Ride pipeline / Ride trace lookup** dashboard in Grafana
and enter a trace ID in the **Trace ID** field. The trace view lists service
spans with their processing steps and durations.

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to another OTLP/HTTP base URL (without the
`/v1/traces` suffix) to export elsewhere.

### Watch it work

Tail the final events, or browse the topics in Kafka UI:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ride.confirmed --from-beginning --property print.key=true
```

A healthy pipeline confirms most rides within ~10 s of the request, rejects
about 10% (either outcome unavailable), and rejects anything still pending after
60 s with reason `TIMEOUT`.

## Topics

| Topic                         | Produced by           | Consumed by           | Notes                                    |
| ----------------------------- | --------------------- | --------------------- | ---------------------------------------- |
| `rides`                       | app-ride              | app-pricing, app-match-driver | One ride request per second      |
| `pricing-results`             | app-pricing           | app-ride-orchestrator | `success` or `unavailable`                |
| `pricing-dead-letter`         | app-pricing           | —                     | Processing/publish failures after retries |
| `driver-matching-results`     | app-match-driver      | app-ride-orchestrator | `success` or `unavailable`                |
| `driver-matching-dead-letter` | app-match-driver      | —                     | Processing/publish failures after retries |
| `trip.state`                  | app-ride-orchestrator | app-ride-orchestrator | Compacted, tombstoned after retention     |
| `ride.confirmed`              | app-ride-orchestrator | —                     | Final confirmation event                  |
| `ride.rejected`               | app-ride-orchestrator | —                     | Final rejection event with reason         |

Defaults can be overridden per service with `KAFKA_*` environment variables; see
the service READMEs for the full list.

## Service layout

The orchestrator shows the structure to follow for anything non-trivial:

```
app-ride-orchestrator/
├── src/
│   ├── domain/           pure types and transitions (no I/O)
│   ├── application/      use cases over the interfaces in ports.ts
│   ├── infrastructure/   kafkajs adapters and JSON codecs
│   ├── shared/           small primitives (pause, SerialQueue)
│   ├── config.ts         environment parsing and validation
│   ├── instrumentation.ts OpenTelemetry bootstrap
│   └── server.ts         wiring, startup and signal handling
└── test/                 Jest (ESM) unit tests with fakes
```

Dependencies point inward only: domain <- application <- infrastructure <-
server. The smaller services use a flat `src/` with the same separation between
outcome logic, processor and server.

## Development

All commands run inside a service directory:

| Command                | Purpose                                             |
| ---------------------- | --------------------------------------------------- |
| `npm run dev`          | Run from TypeScript sources with watch mode         |
| `npm run build`        | Compile to `dist/`                                  |
| `npm start`            | Run the compiled output                             |
| `npm test`             | Jest (ESM, `--runInBand`)                           |
| `npm run test:watch`   | Jest in watch mode                                  |
| `npm run typecheck`    | `tsc --noEmit`                                      |
| `npm run lint`         | ESLint (flat config, typescript-eslint + Prettier)  |
| `npm run format`       | Prettier write                                      |
| `npm run format:check` | Prettier check                                      |
| `npm run clean`        | Remove `dist/` and `coverage/`                      |

Each service has a multi-stage `Dockerfile` (build with dev dependencies, run
with production dependencies only). `docker compose build <service>` builds one
image.

Conventions: TypeScript ESM with `.js` suffixes on relative imports, `strict`
plus `noUncheckedIndexedAccess`, Prettier (single quotes, trailing commas), and
collaborators injected through an options object with defaults so tests can
replace clocks, randomness and loggers.

Test coverage today: `app-ride-orchestrator` has 9 suites / 46 tests covering
the transitions, codecs, sweeps, startup gate and replay. The other three
services have no tests yet; their Jest setup is already in place, so a new
`test/*.test.ts` file runs without further configuration.

Commit subjects follow `#<issue> <scope> <summary>`, e.g.
`#6 app-ride-orchestrator fix replay bug`. Work happens on `development`; `main`
is the PR target.

AI agents working in this repository should read [AGENTS.md](AGENTS.md).

## Troubleshooting

- **`This server does not host this topic-partition` / startup failure** — a
  topic is missing. Auto creation is off; run `docker compose up -d kafka-init`
  (or check `docker compose logs kafka-init`).
- **Compose services exit at startup** — they wait for Kafka and `kafka-init`
  to finish. Check `docker compose ps -a` and `docker compose logs <service>`;
  `kafka-init` is expected to show `Exited (0)`.
- **A container cannot reach Kafka** — inside Compose use `kafka:29092`, not
  `localhost:9092`; `localhost` is the container itself.
- **Orchestrator never confirms anything** — check that `trip.state` is
  compacted and that only one orchestrator instance is running.
- **Nothing arrives on the results topics** — the workers consume with
  `fromBeginning: false`, so they only see rides published after they started.
- **OTLP export errors in the logs** — no collector is listening. Set
  `OTEL_SDK_DISABLED=true`, or point `OTEL_EXPORTER_OTLP_ENDPOINT` at one.
- **Stale behavior after `npm start`** — `dist/` is build output and is not
  tracked; rebuild with `npm run build` (or `npm run clean && npm run build`).

## License

MIT — see [LICENSE](LICENSE).
