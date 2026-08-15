import { describe, expect, test } from 'bun:test';
import { z } from 'zod';
import {
  DurableInboundConsumer,
  type Consumer,
  type ConsumerRecord,
  type OffsetStore,
} from '../src';

const schema = z.object({ id: z.string() });
const record = (offset: string, partition = 0, value = '{"id":"a"}'): ConsumerRecord => ({
  topic: 'avs.events',
  partition,
  offset,
  highWatermark: '2',
  key: null,
  value: Buffer.from(value),
  headers: {},
});

class FakeConsumer implements Consumer {
  each?: (record: ConsumerRecord) => Promise<void>;
  commits: Array<{ topic: string; partition: number; offset: string }> = [];
  seeks: Array<{ topic: string; partition: number; offset: string }> = [];
  stopped = false;
  disconnected = false;
  watermarks = [{ partition: 0, offset: '2' }];
  async connect() {}
  async subscribe() {}
  async run(config: { autoCommit: false; eachMessage(record: ConsumerRecord): Promise<void> }) {
    expect(config.autoCommit).toBe(false);
    this.each = config.eachMessage;
  }
  async commitOffsets(offsets: Array<{ topic: string; partition: number; offset: string }>) {
    this.commits.push(...offsets);
  }
  async highWaterMarks() { return this.watermarks; }
  seek(position: { topic: string; partition: number; offset: string }) {
    this.seeks.push(position);
  }
  async stop() { this.stopped = true; }
  async disconnect() { this.disconnected = true; }
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

  test('crash after effect before commit replays on restart', async () => {
    const x = setup();
    await x.runtime.start();
    x.offsets.failSave = true;
    await expect(x.consumer.emit(record('0'))).rejects.toThrow();
    x.offsets.failSave = false;
    await x.consumer.emit(record('0'));
    expect(x.effects).toEqual(['a', 'a']);
    expect(x.consumer.commits.at(-1)?.offset).toBe('1');
  });

  test('loads durable checkpoints and seeks before intake', async () => {
    const x = setup();
    x.offsets.values.set('avs.events:3', '9');
    await x.runtime.start();
    expect(x.consumer.seeks).toEqual([{ topic: 'avs.events', partition: 3, offset: '9' }]);
  });

  test('isolates partition ordering', async () => {
    let release!: () => void;
    const first = new Promise<void>((resolve) => { release = resolve; });
    const seen: string[] = [];
    const x = setup({ handler: { async handle(value, item) { seen.push(`${item.partition}:${(value as { id: string }).id}`); if (item.partition === 0) await first; return 'ack'; } } });
    x.consumer.watermarks = [{ partition: 0, offset: '1' }, { partition: 1, offset: '1' }];
    await x.runtime.start();
    const blocked = x.consumer.emit(record('0', 0));
    await x.consumer.emit(record('0', 1, '{"id":"b"}'));
    release();
    await blocked;
    expect(seen).toEqual(['0:a', '1:b']);
  });

  test('dead-letters poison before advancing', async () => {
    const x = setup();
    await x.runtime.start();
    await x.consumer.emit(record('0', 0, 'not-json'));
    expect(x.dead).toHaveLength(1);
    expect(x.consumer.commits[0]?.offset).toBe('1');
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

  test('signals startup catch-up at captured high-water marks', async () => {
    const x = setup();
    await x.runtime.start();
    await x.consumer.emit(record('0', 0));
    await x.consumer.emit(record('1', 0));
    await x.runtime.caughtUp();
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
