/** Portas de observabilidade: o núcleo não conhece pino nem prom-client. */
export interface Logger {
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
}

export type MetricLabels = Record<string, string>;

export interface Metrics {
  /** Contador monotônico. */
  increment(name: string, labels?: MetricLabels, value?: number): void;
  /** Distribuição (histograma): latências, tamanhos. */
  observe(name: string, value: number, labels?: MetricLabels): void;
  /** Valor instantâneo (gauge): lag, profundidade de fila. */
  set(name: string, value: number, labels?: MetricLabels): void;
}

export const noopLogger: Logger = { info() {}, warn() {}, error() {} };
export const noopMetrics: Metrics = { increment() {}, observe() {}, set() {} };
