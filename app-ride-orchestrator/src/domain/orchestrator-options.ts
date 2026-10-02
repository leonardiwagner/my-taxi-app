export interface OrchestratorOptions {
  driverTopic: string;
  pricingTopic: string;
  stateTopic: string;
  confirmedTopic: string;
  rejectedTopic: string;
  consumerGroup: string;
  timeoutMs: number;
  finalRetentionMs: number;
  now?: () => Date;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}
