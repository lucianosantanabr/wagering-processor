import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import type { Logger, MetricLabels, Metrics } from '@application/observability';
import { METRIC_CATALOG, type MetricDefinition } from './metric-catalog';

type Instrument = Counter<string> | Histogram<string> | Gauge<string>;

/**
 * Implementa a porta `Metrics` sobre prom-client. As métricas são criadas a partir do catálogo;
 * um nome desconhecido é ignorado (e avisado uma única vez) em vez de derrubar o caminho financeiro.
 */
export class PrometheusMetrics implements Metrics {
  readonly registry: Registry;
  private readonly instruments = new Map<string, Instrument>();
  private readonly warned = new Set<string>();

  constructor(
    private readonly logger?: Logger,
    registry: Registry = new Registry(),
    catalog: Readonly<Record<string, MetricDefinition>> = METRIC_CATALOG,
  ) {
    this.registry = registry;
    for (const [name, def] of Object.entries(catalog)) {
      const base = { name, help: def.help, labelNames: [...def.labels], registers: [registry] };
      this.instruments.set(
        name,
        def.type === 'counter'
          ? new Counter(base)
          : def.type === 'gauge'
            ? new Gauge(base)
            : new Histogram({ ...base, buckets: [...(def.buckets ?? [])] }),
      );
    }
  }

  /** Métricas de processo (CPU, memória, event loop). Opcional: nem todo runtime expõe tudo. */
  collectProcessMetrics(): void {
    try {
      collectDefaultMetrics({ register: this.registry });
    } catch (e) {
      this.logger?.warn('default process metrics unavailable', { error: (e as Error).message });
    }
  }

  increment(name: string, labels: MetricLabels = {}, value = 1): void {
    const m = this.get(name, Counter);
    m?.inc(this.normalize(name, labels), value);
  }

  observe(name: string, value: number, labels: MetricLabels = {}): void {
    const m = this.get(name, Histogram);
    m?.observe(this.normalize(name, labels), value);
  }

  set(name: string, value: number, labels: MetricLabels = {}): void {
    const m = this.get(name, Gauge);
    m?.set(this.normalize(name, labels), value);
  }

  contentType(): string {
    return this.registry.contentType;
  }

  render(): Promise<string> {
    return this.registry.metrics();
  }

  private get<T extends Instrument>(name: string, kind: new (...args: never[]) => T): T | undefined {
    const m = this.instruments.get(name);
    if (m instanceof kind) return m;
    if (!this.warned.has(name)) {
      this.warned.add(name);
      this.logger?.warn('metric ignored: unknown name or wrong type', { metric: name });
    }
    return undefined;
  }

  /** Só os rótulos declarados; ausentes viram "" (prom-client exige o conjunto completo). */
  private normalize(name: string, labels: MetricLabels): Record<string, string> {
    const def = METRIC_CATALOG[name];
    return Object.fromEntries((def?.labels ?? []).map((l) => [l, labels[l] ?? '']));
  }
}
