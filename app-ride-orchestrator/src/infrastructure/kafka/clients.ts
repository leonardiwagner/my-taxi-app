import { Kafka, logLevel, type Consumer, type Producer } from 'kafkajs';

export interface KafkaClients {
  kafka: Kafka;
  consumer: Consumer;
  producer: Producer;
}

export function createKafkaClients(options: {
  brokers: string[];
  consumerGroup: string;
}): KafkaClients {
  const kafka = new Kafka({
    clientId: 'app-ride-orchestrator',
    brokers: options.brokers,
    logLevel: logLevel.NOTHING,
  });

  return {
    kafka,
    consumer: kafka.consumer({
      groupId: options.consumerGroup,
      allowAutoTopicCreation: false,
    }),
    producer: kafka.producer({
      transactionalId: `${options.consumerGroup}-transactional`,
      allowAutoTopicCreation: false,
    }),
  };
}
