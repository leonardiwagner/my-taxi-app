import { pause } from '../../shared/pause.js';

export class StartupGate {
  private released = false;
  private failure?: unknown;
  private resolveReleased!: () => void;
  private readonly releaseSignal: Promise<void>;

  constructor(private readonly heartbeatIntervalMs: number) {
    this.releaseSignal = new Promise<void>((resolve) => {
      this.resolveReleased = resolve;
    });
  }

  release(): void {
    this.released = true;
    this.resolveReleased();
  }

  abort(error: unknown): void {
    this.failure = error;
    this.release();
  }

  async wait(heartbeat: () => Promise<void>): Promise<void> {
    while (!this.released) {
      await Promise.race([this.releaseSignal, pause(this.heartbeatIntervalMs)]);
      if (!this.released) await heartbeat();
    }
    if (this.failure) throw this.failure;
  }
}
