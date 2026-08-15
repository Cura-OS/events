import { describe, expect, test } from 'bun:test';
import { z } from 'zod';
import {
  DurableInboundConsumer,
  type Consumer,
  type ConsumerRecord,
  type OffsetStore,
} from './node';

const schema = z.object({ id: z.string() });
const record = (
  offset: string,
  partition = 0,
  value: string | Uint8Array = '{"id":"a"}',
): ConsumerRecord => ({
  topic: 'avs.events',
  partition,
  offset,
  key: null,
  value: typeof value === 'string' ? Buffer.from(value) : value,
  headers: {},
});

class FakeConsumer implements Consumer {
  each?: (record: ConsumerRecord) => Promise<void>;
  commits: Array<{ topic: string; partition: number; offset: string }> = [];
  seeks: Array<{ topic: string; partition: number; offset: string }> = [];
  stopped = false;
  disconnected = false;
  assignments = [{ partition: 0, low: '0', high: '2', position: '2' }];
  assignmentError?: Error;
  commitError?: Error;
  resumed = false;
  async connect() {}
  async subscribe() {}
  duringRun?: ConsumerRecord;
  duringRunResult?: Promise<void>;
  async run(config: {
    autoCommit: false;
    pauseOnAssignment: true;
    eachMessage(record: ConsumerRecord): Promise<void>;
  }) {
    expect(config).toMatchObject({ autoCommit: false, pauseOnAssignment: true });
    this.each = config.eachMessage;
    if (this.duringRun) {
      this.duringRunResult = config.eachMessage(this.duringRun);
      void this.duringRunResult.catch(() => {});
    }
  }
  async commitOffsets(offsets: Array<{ topic: string; partition: number; offset: string }>) {
    if (this.commitError) throw this.commitError;
    this.commits.push(...offsets);
  }
  async assignedPartitions() {
    if (this.assignmentError) throw this.assignmentError;
    return this.assignments;
  }
  seek(position: { topic: string; partition: number; offset: string }) {
    this.seeks.push(position);
  }
  resume() { this.resumed = true; }
  stopError?: Error;
  disconnectError?: Error;
  async stop() { this.stopped = true; if (this.stopError) throw this.stopError; }
  async disconnect() { this.disconnected = true; if (this.disconnectError) throw this.disconnectError; }
  emit(value: ConsumerRecord) { return this.each!(value); }
}

class MemoryOffsets implements OffsetStore {
  values = new Map<string, string>();
  failSave = false;
  async load(topic: string, partition: number) { return this.values.get(`${topic}:${partition}`); }
  async save(topic: string, partition: number, nextOffset: string) {
    if (this.failSave) throw new Error('checkpoint unavailable');
    this.values.set(`${topic}:${partition}`, nextOffset);
  }
  async loadTopic(topic: string) {
    return [...this.values].flatMap(([key, offset]) => {
      const [storedTopic, partition] = key.split(':');
      return storedTopic === topic ? [{ partition: Number(partition), offset }] : [];
    });
  }
}

const setup = (over: Partial<ConstructorParameters<typeof DurableInboundConsumer>[0]> = {}) => {
  const consumer = new FakeConsumer();
  const offsets = new MemoryOffsets();
  const effects: string[] = [];
  const dead: ConsumerRecord[] = [];
  const runtime = new DurableInboundConsumer({
    consumer,
    topic: 'avs.events',
    schema,
    offsets,
    handler: { async handle(value) { effects.push((value as { id: string }).id); return 'ack'; } },
    deadLetters: { async write(item) { dead.push(item.record); } },
    sleep: async () => {},
    ...over,
  });
  return { consumer, offsets, effects, dead, runtime };
};

describe('DurableInboundConsumer', () => {
  test('crash before checkpoint leaves broker offset uncommitted', async () => {
    const x = setup();
    await x.runtime.start();
    x.offsets.failSave = true;
    await expect(x.consumer.emit(record('0'))).rejects.toThrow('checkpoint unavailable');
    expect(x.consumer.commits).toEqual([]);
  });

  test('restart resumes durable checkpoint after broker commit failure', async () => {
    const offsets = new MemoryOffsets();
    const first = setup({ offsets });
    await first.runtime.start();
    first.consumer.commitError = new Error('broker unavailable');
    await expect(first.consumer.emit(record('0'))).rejects.toThrow('broker unavailable');
    expect(offsets.values.get('avs.events:0')).toBe('1');

    const second = setup({ offsets });
    await second.runtime.start();
    expect(second.consumer.seeks).toContainEqual({ topic: 'avs.events', partition: 0, offset: '1' });
    await second.consumer.emit(record('1'));
    expect(first.effects).toEqual(['a']);
    expect(second.effects).toEqual(['a']);
  });

  test('loads durable checkpoints and seeks before intake', async () => {
    const x = setup();
    x.offsets.values.set('avs.events:3', '9');
    x.consumer.assignments = [{ partition: 3, low: '0', high: '10', position: '10' }];
    await x.runtime.start();
    expect(x.consumer.seeks).toEqual([{ topic: 'avs.events', partition: 3, offset: '9' }]);
  });

  test('isolates partition ordering', async () => {
    let release!: () => void;
    const first = new Promise<void>((resolve) => { release = resolve; });
    const seen: string[] = [];
    const x = setup({ handler: { async handle(value, item) { seen.push(`${item.partition}:${(value as { id: string }).id}`); if (item.partition === 0) await first; return 'ack'; } } });
    x.consumer.assignments = [
      { partition: 0, low: '0', high: '2', position: '2' },
      { partition: 1, low: '0', high: '1', position: '1' },
    ];
    await x.runtime.start();
    const firstBlocked = x.consumer.emit(record('0', 0));
    const secondBlocked = x.consumer.emit(record('1', 0, '{"id":"c"}'));
    await x.consumer.emit(record('0', 1, '{"id":"b"}'));
    expect(seen).toEqual(['0:a', '1:b']);
    release();
    await Promise.all([firstBlocked, secondBlocked]);
    expect(seen).toEqual(['0:a', '1:b', '0:c']);
  });

  test('dead-letters poison before advancing', async () => {
    const x = setup();
    await x.runtime.start();
    await x.consumer.emit(record('0', 0, 'not-json'));
    await x.consumer.emit(record('1', 0, new Uint8Array([0xff])));
    expect(x.dead).toHaveLength(2);
    expect(x.consumer.commits.at(-1)?.offset).toBe('2');
  });

  test('bounds retries then dead-letters and advances', async () => {
    let attempts = 0;
    const x = setup({ maxRetries: 2, handler: { async handle() { attempts += 1; return 'retry'; } } });
    await x.runtime.start();
    await x.consumer.emit(record('0'));
    expect(attempts).toBe(3);
    expect(x.dead).toHaveLength(1);
    expect(x.consumer.commits[0]?.offset).toBe('1');
  });

  test('uses Node default sleep without injection', async () => {
    let attempts = 0;
    const x = setup({ sleep: undefined, maxRetries: 1, baseBackoffMs: 1,
      handler: { async handle() { attempts += 1; return 'retry'; } } });
    await x.runtime.start();
    await x.consumer.emit(record('0'));
    expect(attempts).toBe(2);
  });

  test.each([
    [{ maxRetries: Number.NaN }, 'maxRetries'],
    [{ maxRetries: Number.POSITIVE_INFINITY }, 'maxRetries'],
    [{ maxRetries: -1 }, 'maxRetries'],
    [{ maxRetries: 1.5 }, 'maxRetries'],
    [{ maxRetries: Number.MAX_VALUE }, 'maxRetries'],
    [{ baseBackoffMs: Number.NaN }, 'baseBackoffMs'],
    [{ baseBackoffMs: Number.POSITIVE_INFINITY }, 'baseBackoffMs'],
    [{ baseBackoffMs: 0 }, 'baseBackoffMs'],
    [{ baseBackoffMs: -1 }, 'baseBackoffMs'],
    [{ baseBackoffMs: Number.MAX_VALUE }, 'baseBackoffMs'],
    [{ maxRetries: 31, baseBackoffMs: 2 }, 'maximum retry delay'],
  ])('rejects invalid retry configuration %p', (options, field) => {
    expect(() => setup(options)).toThrow(field);
  });

  test('accepts the Node maximum timer delay', () => {
    expect(() => setup({ maxRetries: 1, baseBackoffMs: 2_147_483_647 })).not.toThrow();
  });

  test('allows zero retries', async () => {
    let attempts = 0;
    const x = setup({ maxRetries: 0, handler: { async handle() { attempts += 1; return 'retry'; } } });
    await x.runtime.start();
    await x.consumer.emit(record('0'));
    expect(attempts).toBe(1);
  });

  test('rejects delivery until assignment initialization seeks and resumes', async () => {
    const x = setup();
    x.consumer.duringRun = record('99');
    await x.runtime.start();
    await expect(x.consumer.duringRunResult!).rejects.toThrow('not initialized');
    expect(x.effects).toEqual([]);
    expect(x.consumer.commits).toEqual([]);
    expect(x.consumer.seeks[0]).toEqual({ topic: 'avs.events', partition: 0, offset: '0' });
    expect(x.consumer.resumed).toBe(true);
  });

  test('cleans up failed startup and rejects later deliveries with original error', async () => {
    const x = setup();
    x.consumer.assignmentError = new Error('assignment failed');
    await expect(x.runtime.start()).rejects.toThrow('assignment failed');
    expect(x.consumer.stopped).toBe(true);
    expect(x.consumer.disconnected).toBe(true);
    await expect(x.consumer.emit(record('0'))).rejects.toThrow('not initialized');
    expect(x.effects).toEqual([]);
  });

  test('signals startup catch-up at captured high-water marks', async () => {
    const x = setup();
    await x.runtime.start();
    await x.consumer.emit(record('0', 0));
    await x.consumer.emit(record('1', 0));
    await x.runtime.caughtUp();
  });

  test('seeks fresh nonempty partitions to low and waits for backlog', async () => {
    const x = setup();
    x.consumer.assignments = [{ partition: 0, low: '4', high: '6', position: '6' }];
    await x.runtime.start();
    expect(x.consumer.seeks).toEqual([{ topic: 'avs.events', partition: 0, offset: '4' }]);
    let caught = false;
    void x.runtime.caughtUp().then(() => { caught = true; });
    await Promise.resolve();
    expect(caught).toBe(false);
    await x.consumer.emit(record('4'));
    await x.consumer.emit(record('5'));
    await x.runtime.caughtUp();
  });

  test('is caught up for fresh empty and already-current partitions', async () => {
    const empty = setup();
    empty.consumer.assignments = [{ partition: 0, low: '0', high: '0', position: '0' }];
    await empty.runtime.start();
    await empty.runtime.caughtUp();

    const current = setup();
    current.offsets.values.set('avs.events:0', '2');
    await current.runtime.start();
    await current.runtime.caughtUp();
  });

  test('disconnects after stop failure and preserves the stop error', async () => {
    const x = setup();
    await x.runtime.start();
    x.consumer.stopError = new Error('stop failed');
    x.consumer.disconnectError = new Error('disconnect failed');
    await expect(x.runtime.shutdown()).rejects.toThrow('stop failed');
    expect(x.consumer.disconnected).toBe(true);
  });

  test('disconnects after drain failure and preserves the drain error', async () => {
    const x = setup();
    await x.runtime.start();
    x.offsets.failSave = true;
    const delivery = x.consumer.emit(record('0'));
    await expect(x.runtime.shutdown()).rejects.toThrow('checkpoint unavailable');
    await expect(delivery).rejects.toThrow('checkpoint unavailable');
    expect(x.consumer.disconnected).toBe(true);
  });

  test('shutdown stops intake, drains work, then disconnects', async () => {
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const x = setup({ handler: { async handle() { await pending; return 'ack'; } } });
    await x.runtime.start();
    const delivery = x.consumer.emit(record('0'));
    const shutdown = x.runtime.shutdown();
    expect(x.consumer.stopped).toBe(true);
    expect(x.consumer.disconnected).toBe(false);
    release();
    await delivery;
    await shutdown;
    expect(x.consumer.commits.at(-1)?.offset).toBe('1');
    expect(x.consumer.disconnected).toBe(true);
  });
});
