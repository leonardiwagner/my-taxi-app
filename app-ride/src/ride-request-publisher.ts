import type { Producer } from 'kafkajs';
import type { RideRequest } from './ride-request.js';

export interface RideRequestPublisher {
  publish(rideRequest: RideRequest): Promise<void>;
}

export function createRideRequestPublisher(
  producer: Producer,
  topic: string,
): RideRequestPublisher {
  return {
    async publish(rideRequest): Promise<void> {
      const result = await producer.send({
        topic,
        messages: [
          {
            key: rideRequest.id,
            value: JSON.stringify(rideRequest),
          },
        ],
      });
      console.info('Published ride request to Kafka', {
        rideId: rideRequest.id,
        topic,
        partitions: result.map(({ partition }) => partition),
      });
    },
  };
}
