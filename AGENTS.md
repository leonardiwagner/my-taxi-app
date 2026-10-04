# AGENTS.md

Guidance for AI agents working in this repository. Read this before planning or
editing. `README.md` has the developer-facing overview; each service has its own
`README.md` with its contract, topics, and environment variables.

## What this repository is

An event-driven taxi-ride pipeline built as four independent Node.js/TypeScript
microservices that communicate only through Kafka. There is no shared library,
no root `package.json`, and no workspace tooling: each service directory is a
self-contained npm package with its own `node_modules`, `tsconfig`, lint, and
test setup.

```
app-ride              publishes ride requests            -> rides
app-pricing           rides -> pricing-results           (+ pricing-dead-letter)
app-match-driver      rides -> driver-matching-results   (+ driver-matching-dead-letter)
app-ride-orchestrator pricing-results + driver-matching-results
                      -> trip.state, ride.confirmed, ride.rejected
```

`docker-compose.yml` provides only Kafka (KRaft, single broker) and Kafka UI.
The services are run from the host with npm scripts.

## Working rules

- **Work inside one service directory.** Every npm command (`install`, `build`,
  `test`, `lint`, `typecheck`) must be run from the service directory, never
  from the repository root. A change scoped to one service must not touch
  another service's files.
- **Duplication across services is intentional.** `instrumentation.ts`, the
  retry helper, `serializeError`, and `parseRideRequest` are deliberately copied
  per service. Do not extract them into a shared package or root workspace
  unless the task explicitly asks for that.
- **Prefer the smallest coherent change.** No speculative refactors, no new
  dependencies, no generated files, no reformatting of untouched code.

## Kafka conventions

- Every record is keyed by `rideId`. The orchestrator correlates only by key and
  `rideId`, and rejects a record whose key disagrees with the payload.


## Code style

- Name things after their responsibility (`expire-pending-trips.ts`,
  `transactional-trip-writer.ts`); files are kebab-case, types PascalCase,
  functions and variables camelCase.
- Let the code explain routine behavior. Comments are rare in this repository
  and should only record non-obvious intent, a constraint, or a trade-off.
- Logging is structured: a message string plus one context object,
  `logger.info('Trip reached final state', { rideId, status })`. Use the injected
  logger in application code and `console` only at the process edge.


## Verification before reporting work done

From the changed service's directory, run the narrowest useful checks first and
then widen:

```sh
npm test
npm run build
```