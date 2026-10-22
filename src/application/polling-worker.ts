import { type Logger, noopLogger } from './observability';

/**
 * Laço de fundo reutilizável. `tick()` faz uma rodada e devolve `true` se há mais trabalho imediato
 * (não dorme); `false` para aguardar `idleIntervalMs`. Falhas de rodada são logadas e o laço segue:
 * cada rodada é transacional, então nada fica pela metade. `stop()` espera a rodada em andamento.
 */
export class PollingWorker {
  private running = false;
  private loop?: Promise<void>;
  private wake?: () => void;

  constructor(
    private readonly name: string,
    private readonly tick: () => Promise<boolean>,
    private readonly idleIntervalMs: number,
    private readonly logger: Logger = noopLogger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  async stop(): Promise<void> {
    this.running = false;
    this.wake?.();
    await this.loop;
  }

  private async run(): Promise<void> {
    while (this.running) {
      let more = false;
      try {
        more = await this.tick();
      } catch (e) {
        this.logger.error(`${this.name} round failed`, { error: e instanceof Error ? e.message : String(e) });
      }
      if (!more && this.running) await this.sleep();
    }
  }

  private sleep(): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, this.idleIntervalMs);
      this.wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }
}
