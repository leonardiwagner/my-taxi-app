import type { FastifyPluginAsync } from 'fastify';

interface RideRequest {
  customer: {
    id: string;
  };
  startingPoint: string;
  destination: string;
}

const rideRequestSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['customer', 'startingPoint', 'destination'],
  properties: {
    customer: {
      type: 'object',
      additionalProperties: false,
      required: ['id'],
      properties: {
        id: { type: 'string' },
      },
    },
    startingPoint: { type: 'string' },
    destination: { type: 'string' },
  },
} as const;

export const rideRoutes: FastifyPluginAsync = async (app) => {
  app.post<{ Body: RideRequest }>(
    '/rides',
    { schema: { body: rideRequestSchema } },
    async (_request, reply) => {
      return reply.code(202).send({ status: 'requested' });
    },
  );
};
