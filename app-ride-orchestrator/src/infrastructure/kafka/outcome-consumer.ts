import type { Consumer } from 'kafkajs';
import type { InboundRecord, OutcomeSource } from '../../application/ports.js';
import { decodeOutcomeRecord } from './inbound-record.js';
import { StartupGate } from './startup-gate.js';
import type { TopicMap } from './topics.js';

export class KafkaOutcomeSource implements OutcomeSource {
  private readonly gate: StartupGate;

  constructor(
    private readonly consumer: Consumer,
    private readonly topics: TopicMap,
    heartbeatIntervalMs: number,
  ) {
    this.gate = new StartupGate(heartbeatIntervalMs);
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: [this.topics.driver, this.topics.pricing],
      fromBeginning: false,
    });
  }

  async run(consume: (record: InboundRecord) => Promise<void>): Promise<void> {
    await this.consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 1,
      eachMessage: async (payload) => {
        await this.gate.wait(() => payload.heartbeat());
        await consume(decodeOutcomeRecord(payload, this.topics));
      },
    });
  }

  release(): void {
    this.gate.release();
  }

  abort(error: unknown): void {
    this.gate.abort(error);
  }

  stop(): Promise<void> {
    return this.consumer.stop();
  }

  disconnect(): Promise<void> {
    return this.consumer.disconnect();
  }
}
