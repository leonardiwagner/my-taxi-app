import { buildApp } from '../src/app.js';

describe('API foundation', () => {
  const app = buildApp();

  afterAll(async () => {
    await app.close();
  });

  it('reports health', async () => {
    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ status: 'ok' });
  });

  it('exposes Prometheus metrics', async () => {
    await app.inject({ method: 'GET', url: '/health' });
    const response = await app.inject({ method: 'GET', url: '/metrics' });

    expect(response.statusCode).toBe(200);
    expect(response.body).toContain('http_server_requests_total');
    expect(response.body).toContain('process_resident_memory_bytes');
  });
});

describe('ride requests', () => {
  const app = buildApp();

  afterAll(async () => {
    await app.close();
  });

  it('accepts a ride request', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/rides',
      payload: {
        customer: { id: 'customer-123' },
        startingPoint: 'Central Station',
        destination: 'Airport',
      },
    });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ status: 'requested' });
  });

  it('rejects a request without a destination', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/rides',
      payload: {
        customer: { id: 'customer-123' },
        startingPoint: 'Central Station',
      },
    });

    expect(response.statusCode).toBe(400);
  });

  it('rejects a non-string customer id', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/rides',
      payload: {
        customer: { id: 123 },
        startingPoint: 'Central Station',
        destination: 'Airport',
      },
    });

    expect(response.statusCode).toBe(400);
  });
});
