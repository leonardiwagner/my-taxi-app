import { createReplayProgress } from '../src/infrastructure/kafka/replay-progress.js';

describe('createReplayProgress', () => {
  it('is already complete when every partition is empty', () => {
    const progress = createReplayProgress([
      { partition: 0, offset: '0' },
      { partition: 1, offset: '0' },
    ]);
    expect(progress.isComplete()).toBe(true);
  });

  it('is incomplete while any partition still has records', () => {
    const progress = createReplayProgress([
      { partition: 0, offset: '0' },
      { partition: 1, offset: '3' },
    ]);
    expect(progress.isComplete()).toBe(false);
  });

  it('completes once every partition reaches its end offset', async () => {
    const progress = createReplayProgress([
      { partition: 0, offset: '2' },
      { partition: 1, offset: '1' },
    ]);
    let settled = false;
    void progress.complete.then(() => {
      settled = true;
    });

    progress.markPartitionReached(0, 1n);
    await Promise.resolve();
    expect(settled).toBe(false);

    progress.markPartitionReached(0, 2n);
    progress.markPartitionReached(1, 1n);
    await progress.complete;
    expect(settled).toBe(true);
    expect(progress.isComplete()).toBe(true);
  });

  it('ignores partitions it was not given', () => {
    const progress = createReplayProgress([{ partition: 0, offset: '2' }]);
    progress.markPartitionReached(9, 99n);
    expect(progress.isComplete()).toBe(false);
  });
});
