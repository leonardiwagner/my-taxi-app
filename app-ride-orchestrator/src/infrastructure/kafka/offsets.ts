import type { EachMessagePayload } from 'kafkajs';
import type { InputCursor } from '../../application/ports.js';

export function nextOffset(offset: string): string {
  return (BigInt(offset) + 1n).toString();
}

export function cursorFor(payload: EachMessagePayload): InputCursor {
  return {
    topic: payload.topic,
    partition: payload.partition,
    nextOffset: nextOffset(payload.message.offset),
  };
}
