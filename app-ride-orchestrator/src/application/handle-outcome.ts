import { applyOutcome } from '../domain/transitions.js';
import type { Clock, InboundRecord, Logger, TripWriter } from './ports.js';
import type { TripStore } from './trip-store.js';

export async function handleOutcome(
  record: InboundRecord,
  store: TripStore,
  writer: TripWriter,
  now: Clock,
  logger: Logger,
): Promise<void> {
  if (!record.ok) {
    logger.error(record.reason, record.detail);
    await writer.commitOutcome(record.cursor);
    return;
  }

  const { rideId } = record.input.outcome;
  const transition = applyOutcome(store.get(rideId), record.input, now());
  await writer.commitOutcome(record.cursor, transition);
  if (transition.changed) store.put(transition.state);
  /*logger.info('Processed ride outcome', {
    rideId,
    kind: record.input.kind,
    outcomeStatus: record.input.outcome.status,
    topic: record.cursor.topic,
    partition: record.cursor.partition,
    offset: (BigInt(record.cursor.nextOffset) - 1n).toString(),
    changed: transition.changed,
    tripStatus: transition.state.status,
  });*/
  if (transition.event)
    logger.info('Trip reached final state', {
      rideId,
      status: transition.state.status,
    });
}
