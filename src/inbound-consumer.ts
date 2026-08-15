import { setTimeout as sleep } from 'node:timers/promises';
import type { z } from 'zod';

/** Broker delivery passed to a durable inbound handler. */
export interface ConsumerRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
  readonly headers: Readonly<Record<string, Uint8Array | string | undefined>>;
}

/** Broker position represented as the next offset to consume. */
export interface TopicPartitionOffset {
  readonly topic: string;
  readonly partition: number;
  /** Next offset to consume. */
  readonly offset: string;
}

/** Structural broker port. A KafkaJS adapter can satisfy this without becoming a dependency. */
export interface Consumer {
  connect(): Promise<void>;
  subscribe(config: { readonly topics: readonly string[]; readonly fromBeginning: boolean }): Promise<void>;
  run(config: {
    readonly autoCommit: false;
    /** Keep newly assigned partitions paused until explicit resume. */
    readonly pauseOnAssignment: true;
    readonly eachMessage: (record: ConsumerRecord) => Promise<void>;
  }): Promise<void>;
  commitOffsets(offsets: readonly TopicPartitionOffset[]): Promise<void>;
  /** Wait for paused assignment, then return broker bounds and current positions. */
  assignedPartitions(topic: string): Promise<readonly {
    partition: number;
    low: string;
    high: string;
    position: string;
  }[]>;
  seek(position: TopicPartitionOffset): void;
  resume(topic: string, partitions: readonly number[]): void;
  stop(): Promise<void>;
  disconnect(): Promise<void>;
}

/** Handler result controlling acknowledgement, retry, or durable rejection. */
export type InboundDisposition = 'ack' | 'retry' | 'dead-letter';

/** Durable domain-effect port invoked for a validated delivery. */
export interface InboundHandler<T> {
  /** Complete the durable domain effect before returning ack. */
  handle(value: T, record: ConsumerRecord): Promise<InboundDisposition>;
}

/** Durable next-offset checkpoint store. */
export interface OffsetStore {
  load(topic: string, partition: number): Promise<string | undefined>;
  loadTopic(topic: string): Promise<readonly { partition: number; offset: string }[]>;
  /** Durably checkpoint the next offset only after the handler effect completes. */
  save(topic: string, partition: number, nextOffset: string): Promise<void>;
}

/** Durable sink for malformed or rejected deliveries. */
export interface DeadLetterSink {
  /** Durably persist the poison record before resolving. */
  write(entry: {
    readonly record: ConsumerRecord;
    readonly reason: 'malformed' | 'handler-rejected' | 'retries-exhausted';
    readonly error?: unknown;
  }): Promise<void>;
}

/** Dependencies and bounded retry policy for a durable consumer. */
export interface DurableInboundConsumerOptions<T> {
  readonly consumer: Consumer;
  readonly topic: string;
  readonly schema: z.ZodType<T>;
  readonly handler: InboundHandler<T>;
  readonly offsets: OffsetStore;
  readonly deadLetters: DeadLetterSink;
  readonly maxRetries?: number;
  readonly baseBackoffMs?: number;
  readonly sleep?: (milliseconds: number) => Promise<void>;
}

const MAX_TIMER_DELAY_MS = 2_147_483_647;
const nextOffset = (offset: string): string => (BigInt(offset) + 1n).toString();
const decode = new TextDecoder('utf-8', { fatal: true });
const nonnegativeInteger = (name: string, value: number): number => {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a safe nonnegative integer`);
  }
  return value;
};
const positiveInteger = (name: string, value: number): number => {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a safe positive integer`);
  }
  return value;
};
const partitionKey = (record: Pick<ConsumerRecord, 'topic' | 'partition'>) =>
  `${record.topic}:${record.partition}`;

/**
 * Broker-neutral, at-least-once inbound orchestration.
 *
 * Ordering is effect, durable checkpoint, then broker commit. Poison records are
 * durably dead-lettered before the same checkpoint and commit sequence.
 */
export class DurableInboundConsumer<T = unknown> {
  private readonly consumer: Consumer;
  private readonly topic: string;
  private readonly schema: z.ZodType<T>;
  private readonly handler: InboundHandler<T>;
  private readonly offsets: OffsetStore;
  private readonly deadLetters: DeadLetterSink;
  private readonly maxRetries: number;
  private readonly baseBackoffMs: number;
  private readonly sleep: (milliseconds: number) => Promise<void>;
  private readonly partitions = new Map<string, Promise<void>>();
  private readonly bootHighWatermarks = new Map<string, bigint>();
  private readonly caughtPartitions = new Set<string>();
  private accepting = false;
  private caughtUpResolve!: () => void;
  private readonly caughtUpPromise = new Promise<void>((resolve) => {
    this.caughtUpResolve = resolve;
  });

  constructor(options: DurableInboundConsumerOptions<T>) {
    this.consumer = options.consumer;
    this.topic = options.topic;
    this.schema = options.schema;
    this.handler = options.handler;
    this.offsets = options.offsets;
    this.deadLetters = options.deadLetters;
    this.maxRetries = nonnegativeInteger('maxRetries', options.maxRetries ?? 5);
    this.baseBackoffMs = positiveInteger('baseBackoffMs', options.baseBackoffMs ?? 100);
    const maximumDelay = this.baseBackoffMs * 2 ** Math.max(0, this.maxRetries - 1);
    if (maximumDelay > MAX_TIMER_DELAY_MS) {
      throw new RangeError('maximum retry delay exceeds the Node timer range');
    }
    this.sleep = options.sleep ?? sleep;
  }

  /** Connect, initialize paused assignments, seek durable starts, then resume intake. */
  async start(): Promise<void> {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topics: [this.topic], fromBeginning: false });
      await this.consumer.run({
        autoCommit: false,
        pauseOnAssignment: true,
        eachMessage: (record) => this.enqueue(record),
      });
      const assignments = await this.consumer.assignedPartitions(this.topic);
      const checkpoints = new Map(
        (await this.offsets.loadTopic(this.topic)).map(({ partition, offset }) => [partition, offset]),
      );
      for (const assignment of assignments) {
        const start = checkpoints.get(assignment.partition) ?? assignment.low;
        this.consumer.seek({ topic: this.topic, partition: assignment.partition, offset: start });
        const key = `${this.topic}:${assignment.partition}`;
        this.bootHighWatermarks.set(key, BigInt(assignment.high));
        if (BigInt(start) >= BigInt(assignment.high)) this.caughtPartitions.add(key);
      }
      if (this.bootHighWatermarks.size === this.caughtPartitions.size) this.caughtUpResolve();
      this.accepting = true;
      this.consumer.resume(this.topic, assignments.map(({ partition }) => partition));
    } catch (error) {
      this.accepting = false;
      try { await this.consumer.stop(); } catch {}
      await Promise.allSettled(this.partitions.values());
      try { await this.consumer.disconnect(); } catch {}
      throw error;
    }
  }

  /** Resolve once every boot-time partition high watermark is checkpointed. */
  caughtUp(): Promise<void> {
    return this.caughtUpPromise;
  }

  /** Stop intake, settle partition jobs, disconnect, then propagate the primary failure. */
  async shutdown(): Promise<void> {
    this.accepting = false;
    let primary: unknown;
    try {
      try {
        await this.consumer.stop();
      } catch (error) {
        primary = error;
      }
      const results = await Promise.allSettled(this.partitions.values());
      primary ??= results.find((result) => result.status === 'rejected')?.reason;
    } finally {
      try {
        await this.consumer.disconnect();
      } catch (error) {
        primary ??= error;
      }
    }
    if (primary !== undefined) throw primary;
  }

  private enqueue(record: ConsumerRecord): Promise<void> {
    if (!this.accepting) return Promise.reject(new Error('inbound consumer is not initialized'));
    const key = partitionKey(record);
    const pending = (this.partitions.get(key) ?? Promise.resolve()).then(() =>
      this.process(record),
    );
    this.partitions.set(key, pending);
    void pending
      .finally(() => {
        if (this.partitions.get(key) === pending) this.partitions.delete(key);
      })
      .catch(() => {});
    return pending;
  }

  private async process(record: ConsumerRecord): Promise<void> {
    let parsed: T;
    try {
      if (record.value === null) throw new Error('record value is null');
      parsed = this.schema.parse(JSON.parse(decode.decode(record.value)));
    } catch (error) {
      await this.deadLetters.write({ record, reason: 'malformed', error });
      await this.advance(record);
      return;
    }

    let lastError: unknown;
    // Retries are intentionally sequential to preserve partition order and backoff.
    /* oxlint-disable no-await-in-loop */
    for (let attempt = 0; attempt <= this.maxRetries; attempt += 1) {
      let disposition: InboundDisposition = 'retry';
      try {
        disposition = await this.handler.handle(parsed, record);
      } catch (error) {
        lastError = error;
      }
      if (disposition === 'ack') {
        await this.advance(record);
        return;
      }
      if (disposition === 'dead-letter') {
        await this.deadLetters.write({ record, reason: 'handler-rejected' });
        await this.advance(record);
        return;
      }
      if (attempt < this.maxRetries) {
        await this.sleep(this.baseBackoffMs * 2 ** attempt);
      }
    }
    /* oxlint-enable no-await-in-loop */
    await this.deadLetters.write({ record, reason: 'retries-exhausted', error: lastError });
    await this.advance(record);
  }

  private async advance(record: ConsumerRecord): Promise<void> {
    const offset = nextOffset(record.offset);
    await this.offsets.save(record.topic, record.partition, offset);
    await this.consumer.commitOffsets([{ topic: record.topic, partition: record.partition, offset }]);
    const key = partitionKey(record);
    const bootHighWatermark = this.bootHighWatermarks.get(key);
    if (bootHighWatermark !== undefined && BigInt(offset) >= bootHighWatermark) {
      this.caughtPartitions.add(key);
      if (this.caughtPartitions.size === this.bootHighWatermarks.size) this.caughtUpResolve();
    }
  }
}
