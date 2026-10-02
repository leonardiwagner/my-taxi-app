import type { EachMessagePayload } from 'kafkajs';
import {
  applyOutcome,
  parseOutcome,
  type OutcomeKind,
  type TripOutcome,
  type TripState,
} from '../trip-state.js';
import type { OrchestratorOptions } from './orchestrator-options.js';
import { TripStateWriter } from './trip-state-writer.js';

export async function handleResult(
  payload: EachMessagePayload,
  trips: Map<string, TripState>,
  options: OrchestratorOptions,
  writer: TripStateWriter,
  now: () => Date,
  logger: Pick<Console, 'info' | 'warn' | 'error'>,
): Promise<void> {
  const kind = kindForTopic(payload.topic, options);
  const rideIdKey = payload.message.key?.toString();
  let outcome: TripOutcome;
  try {
    outcome = parseOutcome(payload.message.value?.toString() ?? '', kind);
  } catch (error) {
    logger.error('Skipping invalid result record', {
      topic: payload.topic,
      partition: payload.partition,
      offset: payload.message.offset,
      error,
    });
    await writer.commitInputOffset(payload);
    return;
  }
  if (!rideIdKey || rideIdKey !== outcome.rideId) {
    logger.error('Skipping result whose key does not match rideId', {
      topic: payload.topic,
      partition: payload.partition,
      offset: payload.message.offset,
      rideIdKey,
      rideId: outcome.rideId,
    });
    await writer.commitInputOffset(payload);
    return;
  }

  const transition = applyOutcome(
    trips.get(outcome.rideId),
    kind,
    outcome,
    now(),
  );
  await writer.commitOutcome(payload, transition);
  if (transition.changed) trips.set(outcome.rideId, transition.state);
  if (transition.event)
    logger.info('Trip reached final state', {
      rideId: outcome.rideId,
      status: transition.state.status,
    });
}

function kindForTopic(
  topic: string,
  options: OrchestratorOptions,
): OutcomeKind {
  if (topic === options.driverTopic) return 'driver';
  if (topic === options.pricingTopic) return 'pricing';
  throw new Error(`Unexpected result topic: ${topic}`);
}
