import type { EachMessagePayload } from 'kafkajs';
import type { OutcomeInput, OutcomeKind } from '../../domain/outcomes.js';
import type { InboundRecord } from '../../application/ports.js';
import { parseOutcome } from '../serialization/outcome-codec.js';
import { cursorFor } from './offsets.js';
import type { TopicMap } from './topics.js';

function kindForTopic(topic: string, topics: TopicMap): OutcomeKind {
  if (topic === topics.driver) return 'driver';
  if (topic === topics.pricing) return 'pricing';
  throw new Error(`Unexpected result topic: ${topic}`);
}

export function decodeOutcomeRecord(
  payload: EachMessagePayload,
  topics: TopicMap,
): InboundRecord {
  const cursor = cursorFor(payload);
  const kind = kindForTopic(payload.topic, topics);
  const location = {
    topic: payload.topic,
    partition: payload.partition,
    offset: payload.message.offset,
  };
  const rideIdKey = payload.message.key?.toString();

  let input: OutcomeInput;
  try {
    input = parseOutcome(payload.message.value?.toString() ?? '', kind);
  } catch (error) {
    return {
      ok: false,
      cursor,
      reason: 'Skipping invalid result record',
      detail: { ...location, error },
    };
  }

  if (!rideIdKey || rideIdKey !== input.outcome.rideId) {
    return {
      ok: false,
      cursor,
      reason: 'Skipping result whose key does not match rideId',
      detail: { ...location, rideIdKey, rideId: input.outcome.rideId },
    };
  }

  return { ok: true, cursor, input };
}
