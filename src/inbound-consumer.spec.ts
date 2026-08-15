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
  eachAssignment?: (assignments: typeof this.assignments) => Promise<void>;
  commits: Array<{ topic: string; partition: number; offset: string }> = [];
  seeks: Array<{ topic: string; partition: number; offset: string }> = [];
  stopped = false;
  disconnected = false;
  assignments = [{ partition: 0, low: '0', high: '2', position: '2' }];
  assignmentError?: Error;
  commitError?: Error;
  resumed = false;
  resumes: Array<{ topic: string; partitions: number[] }> = [];
  connects = 0;
  subscribes = 0;
  runs = 0;
  runEntered?: () => void;
  emitInitialAssignment = true;
  stops = 0;
  disconnects = 0;
  connectGate?: Promise<void>;
  subscribeGate?: Promise<void>;
  subscribeEntered?: () => void;
  awaitAssignmentCallbacks = true;
  async connect() { this.connects += 1; await this.connectGate; }
  async subscribe() { this.subscribes += 1; this.subscribeEntered?.(); await this.subscribeGate; }
  duringRun?: ConsumerRecord;
  duringRunResult?: Promise<void>;
  async run(config: {
    autoCommit: false;
    pauseOnAssignment: true;
    eachMessage(record: ConsumerRecord): Promise<void>;
    eachAssignment(assignments: typeof this.assignments): Promise<void>;
  }) {
    this.runs += 1;
    this.runEntered?.();
    expect(config).toMatchObject({ autoCommit: false, pauseOnAssignment: true });
    this.each = config.eachMessage;
    this.eachAssignment = config.eachAssignment;
    if (this.duringRun) {
      this.duringRunResult = config.eachMessage(this.duringRun);
      void this.duringRunResult.catch(() => {});
    }
    await this.assignmentFailure();
    if (this.emitInitialAssignment) {
      const assignment = config.eachAssignment(this.assignments);
      if (this.awaitAssignmentCallbacks) await assignment;
      else void assignment.catch(() => {});
    }
  }
  async commitOffsets(offsets: Array<{ topic: string; partition: number; offset: string }>) {
    if (this.commitError) throw this.commitError;
    this.commits.push(...offsets);
  }
  async rebalance(assignments: typeof this.assignments) {
    this.assignments = assignments;
    await this.eachAssignment!(assignments);
  }
  seek(position: { topic: string; partition: number; offset: string }) {
    this.seeks.push(position);
  }
  resume(topic: string, partitions: readonly number[]) {
    this.resumed = true;
    this.resumes.push({ topic, partitions: [...partitions] });
  }
  async assignmentFailure() {
    if (this.assignmentError) throw this.assignmentError;
  }
  stopError?: Error;
  disconnectError?: Error;
  async stop() { this.stopped = true; this.stops += 1; if (this.stopError) throw this.stopError; }
  async disconnect() { this.disconnected = true; this.disconnects += 1; if (this.disconnectError) throw this.disconnectError; }
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
  loadTopicGate?: Promise<void>;
  async loadTopic(topic: string) {
    await this.loadTopicGate;
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

  test('concurrent starts share one broker lifecycle', async () => {
    const x = setup();
    const first = x.runtime.start();
    const second = x.runtime.start();
    expect(first).toBe(second);
    await first;
    expect(x.consumer.connects).toBe(1);
    expect(x.consumer.runs).toBe(1);
  });

  test('shutdown is idempotent', async () => {
    const x = setup();
    await x.runtime.start();
    const first = x.runtime.shutdown();
    const second = x.runtime.shutdown();
    expect(first).toBe(second);
    await first;
    expect(x.consumer.stops).toBe(1);
    expect(x.consumer.disconnects).toBe(1);
  });

  test('shutdown settles despite a stuck connect', async () => {
    let release!: () => void;
    const x = setup();
    x.consumer.connectGate = new Promise<void>((resolve) => { release = resolve; });
    const start = x.runtime.start();
    await Promise.resolve();
    await x.runtime.shutdown();
    release();
    await expect(start).rejects.toThrow('shut down');
    expect(x.consumer.subscribes).toBe(0);
    expect(x.consumer.runs).toBe(0);
  });

  test('shutdown during subscribe prevents later run', async () => {
    let release!: () => void;
    let entered!: () => void;
    const x = setup();
    x.consumer.subscribeGate = new Promise<void>((resolve) => { release = resolve; });
    const subscribed = new Promise<void>((resolve) => { entered = resolve; });
    x.consumer.subscribeEntered = entered;
    const start = x.runtime.start();
    await subscribed;
    const shutdown = x.runtime.shutdown();
    release();
    await shutdown;
    await expect(start).rejects.toThrow('shut down');
    expect(x.consumer.runs).toBe(0);
  });

  test('shutdown settles startup waiting for its initial assignment once', async () => {
    let entered!: () => void;
    const x = setup();
    x.consumer.emitInitialAssignment = false;
    const running = new Promise<void>((resolve) => { entered = resolve; });
    x.consumer.runEntered = entered;
    const start = x.runtime.start();
    await running;
    await x.runtime.shutdown();
    await expect(start).rejects.toThrow('shut down before initial assignment');
    expect(x.consumer.stops).toBe(1);
    expect(x.consumer.disconnects).toBe(1);
  });

  test('loads durable checkpoints and seeks before intake', async () => {
    const x = setup();
    x.offsets.values.set('avs.events:3', '9');
    x.consumer.assignments = [{ partition: 3, low: '0', high: '10', position: '10' }];
    await x.runtime.start();
    expect(x.consumer.seeks).toEqual([{ topic: 'avs.events', partition: 3, offset: '9' }]);
  });

  test('old delivery cannot resolve catch-up after a rebalance', async () => {
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const x = setup({ handler: { async handle() { await pending; return 'ack'; } } });
    await x.runtime.start();
    const delivery = x.consumer.emit(record('1'));
    const rebalance = x.consumer.rebalance([{ partition: 1, low: '0', high: '2', position: '2' }]);
    const catchUp = x.runtime.caughtUp();
    let caught = false;
    void catchUp.then(() => { caught = true; });
    release();
    await Promise.all([delivery, rebalance]);
    await Promise.resolve();
    expect(caught).toBe(false);
  });

  test('only the latest overlapping rebalance seeks and resumes', async () => {
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const x = setup({ handler: { async handle() { await pending; return 'ack'; } } });
    await x.runtime.start();
    const delivery = x.consumer.emit(record('0'));
    x.consumer.resumes = [];
    const first = x.consumer.rebalance([{ partition: 2, low: '5', high: '9', position: '9' }]);
    const second = x.consumer.rebalance([{ partition: 3, low: '6', high: '10', position: '10' }]);
    await Promise.resolve();
    expect(x.consumer.resumes).toEqual([]);
    release();
    await Promise.all([delivery, first, second]);
    expect(x.consumer.seeks).not.toContainEqual({ topic: 'avs.events', partition: 2, offset: '5' });
    expect(x.consumer.resumes).toEqual([{ topic: 'avs.events', partitions: [3] }]);
  });

  test.each([
    ['0', false],
    ['10', false],
    ['-1', true],
    ['11', true],
    ['bad', true],
  ])('validates checkpoint %s against broker bounds', async (checkpoint, invalid) => {
    const x = setup();
    x.offsets.values.set('avs.events:0', checkpoint);
    x.consumer.assignments = [{ partition: 0, low: '0', high: '10', position: '10' }];
    const start = x.runtime.start();
    if (invalid) await expect(start).rejects.toThrow('checkpoint');
    else await expect(start).resolves.toBeUndefined();
  });

  test.each([
    [{ partition: 0, low: '5', high: '4', position: '5' }],
    [{ partition: 0, low: '0', high: '4', position: '5' }],
    [{ partition: 0, low: '-1', high: '4', position: '0' }],
    [{ partition: 0, low: '0', high: '-1', position: '0' }],
    [{ partition: 0, low: '0', high: '4', position: '-1' }],
  ])('rejects invalid broker bounds %p', async (assignment) => {
    const x = setup();
    x.consumer.assignments = [assignment];
    await expect(x.runtime.start()).rejects.toThrow('broker bounds');
  });

  test('waits for a rejected initial assignment before resolving startup', async () => {
    const x = setup();
    x.consumer.awaitAssignmentCallbacks = false;
    x.consumer.assignments = [{ partition: -1, low: '0', high: '0', position: '0' }];
    await expect(x.runtime.start()).rejects.toThrow('partition');
    expect(x.consumer.stopped).toBe(true);
    expect(x.consumer.disconnected).toBe(true);
  });

  test('startup waits for the latest superseding initial assignment', async () => {
    let releaseFirst!: () => void;
    let releaseSecond!: () => void;
    let enteredFirst!: () => void;
    let enteredSecond!: () => void;
    const first = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const second = new Promise<void>((resolve) => { releaseSecond = resolve; });
    const firstEntered = new Promise<void>((resolve) => { enteredFirst = resolve; });
    const secondEntered = new Promise<void>((resolve) => { enteredSecond = resolve; });
    const x = setup();
    x.consumer.awaitAssignmentCallbacks = false;
    let calls = 0;
    x.offsets.loadTopic = async () => {
      calls += 1;
      if (calls === 1) { enteredFirst(); await first; }
      else { enteredSecond(); await second; }
      return [];
    };
    const start = x.runtime.start();
    await firstEntered;
    const rebalance = x.consumer.rebalance([{ partition: 1, low: '0', high: '1', position: '1' }]);
    releaseFirst();
    await secondEntered;
    let settled = false;
    void start.then(() => { settled = true; });
    await Promise.resolve();
    expect(settled).toBe(false);
    releaseSecond();
    await Promise.all([start, rebalance]);
  });

  test('validates every assignment before seeking or resuming', async () => {
    const x = setup();
    x.consumer.assignments = [
      { partition: 1, low: '0', high: '1', position: '1' },
      { partition: Number.NaN, low: '0', high: '1', position: '1' },
    ];
    await expect(x.runtime.start()).rejects.toThrow('partition');
    expect(x.consumer.seeks).toEqual([]);
    expect(x.consumer.resumes).toEqual([]);
  });

  test.each([
    { ...record('0'), topic: 'other.events' },
    { ...record('0'), partition: -1 },
    { ...record('0'), partition: Number.POSITIVE_INFINITY },
  ])('rejects records outside the subscribed topic and partitions', async (item) => {
    const x = setup();
    await x.runtime.start();
    await expect(x.consumer.emit(item)).rejects.toThrow('topic or partition');
    expect(x.effects).toEqual([]);
    expect(x.dead).toEqual([]);
    expect(x.consumer.commits).toEqual([]);
  });

  test('rejects delivery from a partition outside the current assignment', async () => {
    const x = setup();
    await x.runtime.start();
    await expect(x.consumer.emit(record('0', 1))).rejects.toThrow('not assigned');
    expect(x.effects).toEqual([]);
    expect(x.dead).toEqual([]);
    expect(x.consumer.commits).toEqual([]);
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

  test.each(['-1', 'not-an-offset'])('rejects invalid offset %s before parsing or effects', async (offset) => {
    const x = setup();
    await x.runtime.start();
    await expect(x.consumer.emit(record(offset, 0, 'not-json'))).rejects.toThrow('offset');
    expect(x.dead).toEqual([]);
    expect(x.effects).toEqual([]);
    expect(x.consumer.commits).toEqual([]);
  });

  test('accepts asynchronously validated records', async () => {
    const x = setup({ schema: z.object({ id: z.string().refine(async () => true) }) });
    await x.runtime.start();
    await x.consumer.emit(record('0'));
    expect(x.effects).toEqual(['a']);
    expect(x.dead).toEqual([]);
    expect(x.consumer.commits[0]?.offset).toBe('1');
  });

  test('dead-letters poison before advancing', async () => {
    const x = setup();
    await x.runtime.start();
    await x.consumer.emit(record('0', 0, 'not-json'));
    await x.consumer.emit(record('1', 0, new Uint8Array([0xff])));
    expect(x.dead).toHaveLength(2);
    expect(x.consumer.commits.at(-1)?.offset).toBe('2');
  });

  test.each(['checkpoint unavailable', 'dlq unavailable'])('rejects catch-up when %s rejects', async (failure) => {
    const x = setup({ deadLetters: { async write() { throw new Error('dlq unavailable'); } } });
    await x.runtime.start();
    if (failure === 'checkpoint unavailable') x.offsets.failSave = true;
    const catchUp = x.runtime.caughtUp();
    const delivery = failure === 'checkpoint unavailable'
      ? x.consumer.emit(record('0'))
      : x.consumer.emit(record('0', 0, 'not-json'));
    await expect(delivery).rejects.toThrow(failure);
    await expect(catchUp).rejects.toThrow(failure);
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
    x.consumer.assignments = [];
    await expect(x.runtime.start()).rejects.toThrow('assignment failed');
    expect(x.consumer.stopped).toBe(true);
    expect(x.consumer.disconnected).toBe(true);
    await expect(x.consumer.emit(record('0'))).rejects.toThrow('not initialized');
    expect(x.effects).toEqual([]);
  });

  test('failed startup does not repeat terminal cleanup on shutdown', async () => {
    const x = setup();
    x.consumer.assignmentError = new Error('assignment failed');
    await expect(x.runtime.start()).rejects.toThrow('assignment failed');
    await x.runtime.shutdown();
    expect(x.consumer.stops).toBe(1);
    expect(x.consumer.disconnects).toBe(1);
  });

  test('aggregates startup cleanup failures behind the primary error', async () => {
    const x = setup();
    x.consumer.assignmentError = new Error('assignment failed');
    x.consumer.assignments = [];
    x.consumer.stopError = new Error('stop failed');
    x.consumer.disconnectError = new Error('disconnect failed');
    try {
      await x.runtime.start();
      throw new Error('expected startup failure');
    } catch (error) {
      expect(error).toBeInstanceOf(AggregateError);
      expect((error as AggregateError).cause).toBe(x.consumer.assignmentError);
      expect((error as AggregateError).errors.map((item) => (item as Error).message)).toEqual([
        'assignment failed', 'stop failed', 'disconnect failed',
      ]);
    }
  });

  test('rejects catch-up on startup failure and shutdown before catch-up', async () => {
    const failed = setup();
    failed.consumer.assignmentError = new Error('assignment failed');
    failed.consumer.assignments = [];
    const startupCatchUp = failed.runtime.caughtUp();
    await expect(failed.runtime.start()).rejects.toThrow('assignment failed');
    await expect(startupCatchUp).rejects.toThrow('assignment failed');

    const stopped = setup();
    await stopped.runtime.start();
    const shutdownCatchUp = stopped.runtime.caughtUp();
    await stopped.runtime.shutdown();
    await expect(shutdownCatchUp).rejects.toThrow('shut down before catch-up');
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

  test('shutdown fences a blocked rebalance before disconnecting', async () => {
    let release!: () => void;
    const x = setup();
    await x.runtime.start();
    x.consumer.resumes = [];
    x.offsets.loadTopicGate = new Promise<void>((resolve) => { release = resolve; });
    const rebalance = x.consumer.rebalance([{ partition: 2, low: '0', high: '0', position: '0' }]);
    await Promise.resolve();
    const shutdown = x.runtime.shutdown();
    expect(x.consumer.disconnected).toBe(false);
    release();
    await Promise.all([rebalance, shutdown]);
    expect(x.consumer.resumes).toEqual([]);
    expect(x.consumer.disconnected).toBe(true);
  });

  test('shutdown prevents reconnecting and post-disconnect assignment callbacks', async () => {
    const x = setup();
    await x.runtime.start();
    x.consumer.seeks = [];
    x.consumer.resumes = [];
    await x.runtime.shutdown();
    await expect(x.runtime.start()).rejects.toThrow('shut down');
    await x.consumer.rebalance([{ partition: 2, low: '0', high: '0', position: '0' }]);
    expect(x.consumer.connects).toBe(1);
    expect(x.consumer.seeks).toEqual([]);
    expect(x.consumer.resumes).toEqual([]);
  });

  test('stale rebalance failure remains observable without rejecting newer catch-up', async () => {
    let rejectFirst!: (error: Error) => void;
    let enteredFirst!: () => void;
    const firstEntered = new Promise<void>((resolve) => { enteredFirst = resolve; });
    const x = setup();
    await x.runtime.start();
    const loadTopic = x.offsets.loadTopic.bind(x.offsets);
    let calls = 0;
    x.offsets.loadTopic = async (topic) => {
      calls += 1;
      if (calls === 1) {
        enteredFirst();
        await new Promise<void>((_, reject) => { rejectFirst = reject; });
      }
      return loadTopic(topic);
    };
    const first = x.consumer.rebalance([{ partition: 2, low: '0', high: '0', position: '0' }]);
    await firstEntered;
    const second = x.consumer.rebalance([{ partition: 3, low: '0', high: '0', position: '0' }]);
    const currentCatchUp = x.runtime.caughtUp();
    rejectFirst(new Error('stale assignment failed'));
    await expect(first).rejects.toThrow('stale assignment failed');
    await second;
    await expect(currentCatchUp).resolves.toBeUndefined();
    await expect(x.runtime.shutdown()).rejects.toThrow('stale assignment failed');
  });

  test('aggregates shutdown stop, drain, and disconnect failures', async () => {
    const x = setup();
    await x.runtime.start();
    x.consumer.stopError = new Error('stop failed');
    x.consumer.disconnectError = new Error('disconnect failed');
    x.offsets.failSave = true;
    const delivery = x.consumer.emit(record('0'));
    try {
      await x.runtime.shutdown();
      throw new Error('expected shutdown failure');
    } catch (error) {
      expect(error).toBeInstanceOf(AggregateError);
      expect((error as AggregateError).errors.map((item) => (item as Error).message)).toEqual([
        'stop failed', 'checkpoint unavailable', 'disconnect failed',
      ]);
    }
    await expect(delivery).rejects.toThrow('checkpoint unavailable');
    expect(x.consumer.disconnected).toBe(true);
  });

  test('includes rebalance failure in shutdown errors', async () => {
    const x = setup();
    await x.runtime.start();
    const rebalance = x.consumer.rebalance([{ partition: -1, low: '0', high: '0', position: '0' }]);
    await expect(rebalance).rejects.toThrow('partition');
    await expect(x.runtime.shutdown()).rejects.toThrow('partition');
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
    await Promise.resolve();
    await Promise.resolve();
    expect(x.consumer.stopped).toBe(true);
    expect(x.consumer.disconnected).toBe(false);
    release();
    await delivery;
    await shutdown;
    expect(x.consumer.commits.at(-1)?.offset).toBe('1');
    expect(x.consumer.disconnected).toBe(true);
  });
});
