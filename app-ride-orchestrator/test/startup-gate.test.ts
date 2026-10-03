import { StartupGate } from '../src/infrastructure/kafka/startup-gate.js';

describe('StartupGate', () => {
  it('lets a record through immediately once released', async () => {
    const gate = new StartupGate(5);
    gate.release();
    const heartbeats: number[] = [];

    await gate.wait(async () => {
      heartbeats.push(1);
    });

    expect(heartbeats).toHaveLength(0);
  });

  it('heartbeats while held and returns after release', async () => {
    const gate = new StartupGate(5);
    let heartbeats = 0;
    const held = gate.wait(async () => {
      heartbeats += 1;
    });

    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(heartbeats).toBeGreaterThan(0);

    gate.release();
    await expect(held).resolves.toBeUndefined();
  });

  it('fails held records with the replay error', async () => {
    const gate = new StartupGate(5);
    const failure = new Error('Timed out replaying trip.state.');
    const held = gate.wait(async () => undefined);

    gate.abort(failure);

    await expect(held).rejects.toBe(failure);
  });

  it('keeps failing records that arrive after the abort', async () => {
    const gate = new StartupGate(5);
    const failure = new Error('replay failed');
    gate.abort(failure);

    await expect(gate.wait(async () => undefined)).rejects.toBe(failure);
  });
});
