// Preload dos testes. Os de integração sobem containers pelo CLI (podman/docker), ver
// test/integration/support/container.ts, e alguns esperam backoff real: timeout generoso por teste.
import { setDefaultTimeout } from 'bun:test';

setDefaultTimeout(120_000);
