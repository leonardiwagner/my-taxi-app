export interface ReplayProgress {
  complete: Promise<void>;
  isComplete: () => boolean;
  markPartitionReached: (partition: number, nextOffset: bigint) => void;
}

export function createReplayProgress(
  offsets: Array<{ partition: number; offset: string }>,
): ReplayProgress {
  const targets = new Map(
    offsets.map(({ partition, offset }) => [partition, BigInt(offset)]),
  );
  const reached = new Set(
    offsets
      .filter(({ offset }) => BigInt(offset) === 0n)
      .map(({ partition }) => partition),
  );
  let resolveReplay!: () => void;
  const complete = new Promise<void>((resolve) => {
    resolveReplay = resolve;
  });

  return {
    complete,
    isComplete: () => reached.size === targets.size,
    markPartitionReached(partition: number, nextOffset: bigint) {
      const target = targets.get(partition);
      if (target !== undefined && nextOffset >= target) reached.add(partition);
      if (reached.size === targets.size) resolveReplay();
    },
  };
}
