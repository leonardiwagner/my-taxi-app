import { randomUUID } from 'node:crypto';

export interface RideRequest {
  id: string;
  customer: {
    id: string;
  };
  startingPoint: string;
  destination: string;
  requestedAt: string;
}

const routes = [
  ['Central Station', 'Airport'],
  ['Museum Quarter', 'Canal District'],
  ['University', 'Business Park'],
  ['City Hall', 'Harbor'],
] as const;

export function generateRideRequest(): RideRequest {
  const [startingPoint, destination] = routes[Math.floor(Math.random() * routes.length)]!;

  return {
    id: randomUUID(),
    customer: { id: `customer-${randomUUID()}` },
    startingPoint,
    destination,
    requestedAt: new Date().toISOString(),
  };
}
