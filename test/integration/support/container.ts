import { randomBytes } from 'node:crypto';

/**
 * Sobe containers via CLI (`podman` ou `docker`; override com CONTAINER_ENGINE).
 * Preferimos o CLI ao Testcontainers: funciona igual em Podman/Docker e no Bun.
 */
let engineCache: string | undefined;

async function exec(cmd: string[], opts: { allowFail?: boolean } = {}): Promise<string> {
  const proc = Bun.spawn(cmd, { stdout: 'pipe', stderr: 'pipe' });
  const [out, err, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (code !== 0 && !opts.allowFail) {
    throw new Error(`${cmd.join(' ')} failed (${code}): ${err || out}`);
  }
  return out.trim();
}

export async function engine(): Promise<string> {
  if (engineCache) return engineCache;
  const candidates = process.env.CONTAINER_ENGINE ? [process.env.CONTAINER_ENGINE] : ['podman', 'docker'];
  for (const c of candidates) {
    try {
      await exec([c, 'info', '--format', '{{.Host.OS}}'], { allowFail: false });
      return (engineCache = c);
    } catch {
      /* tenta o próximo */
    }
  }
  throw new Error('no working container engine (podman/docker) found');
}

export interface RunningContainer {
  id: string;
  hostPort: number;
}

export async function runContainer(opts: {
  image: string;
  port: number;
  env?: Record<string, string>;
  readyCommand?: string[];
  /** Tentativas (a cada 500 ms) do readyCommand antes de desistir. */
  readyAttempts?: number;
}): Promise<RunningContainer> {
  const bin = await engine();
  const name = `wagering-test-${randomBytes(4).toString('hex')}`;
  const envArgs = Object.entries(opts.env ?? {}).flatMap(([k, v]) => ['-e', `${k}=${v}`]);
  const id = await exec([bin, 'run', '-d', '--name', name, '-p', `127.0.0.1::${opts.port}`, ...envArgs, opts.image]);
  try {
    const mapping = await exec([bin, 'port', id, `${opts.port}/tcp`]);
    const hostPort = Number(mapping.split('\n')[0]?.split(':').pop());
    if (!hostPort) throw new Error(`could not resolve host port from "${mapping}"`);
    if (opts.readyCommand) {
      const attempts = opts.readyAttempts ?? 60;
      let ready = false;
      for (let i = 0; i < attempts && !ready; i++) {
        const proc = Bun.spawn([bin, 'exec', id, ...opts.readyCommand], { stdout: 'ignore', stderr: 'ignore' });
        ready = (await proc.exited) === 0;
        if (!ready) await new Promise((r) => setTimeout(r, 500));
      }
      if (!ready) throw new Error(`container ${opts.image} did not become ready`);
    }
    return { id, hostPort };
  } catch (e) {
    await removeContainer(id);
    throw e;
  }
}

export async function removeContainer(id: string): Promise<void> {
  const bin = await engine();
  await exec([bin, 'rm', '-f', '-v', id], { allowFail: true });
}
