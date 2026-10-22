import { v7 as uuidv7 } from 'uuid';
import type { Clock, IdGenerator } from '@application/ports';

export class SystemClock implements Clock {
  now(): Date {
    return new Date();
  }
}

/** UUID v7: ordenável no tempo (bom para índices B-tree). */
export class UuidV7Generator implements IdGenerator {
  next(): string {
    return uuidv7();
  }
}
