import { setTimeout as sleep } from 'node:timers/promises';
import type { z } from 'zod';

export interface ConsumerRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
  readonly headers: Readonly<Record<string, Uint8Array | string | undefined>>;
}

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

export type InboundDisposition = 'ack' | 'retry' | 'dead-letter';

export interface InboundHandler<T> {
  /** Complete the durable domain effect before returning ack. */
  handle(value: T, record: ConsumerRecord): Promise<InboundDisposition>;
}

export interface OffsetStore {
  load(topic: string, partition: number): Promise<string | undefined>;
  loadTopic(topic: string): Promise<readonly { partition: number; offset: string }[]>;
  /** Durably checkpoint the next offset only after the handler effect completes. */
  save(topic: string, partition: number, nextOffset: string): Promise<void>;
}

export interface DeadLetterSink {
  /** Durably persist the poison record before resolving. */
  write(entry: {
    readonly record: ConsumerRecord;
    readonly reason: 'malformed' | 'handler-rejected' | 'retries-exhausted';
    readonly error?: unknown;
  }): Promise<void>;
}

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

const nextOffset = (offset: string): string => (BigInt(offset) + 1n).toString();
const decode = new TextDecoder('utf-8', { fatal: true });
const nonnegativeInteger = (name: string, value: number): number => {
  if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a finite nonnegative integer`);
  }
  return value;
};
const positiveFinite = (name: string, value: number): number => {
  if (!Number.isFinite(value) || value <= 0) throw new TypeError(`${name} must be finite and positive`);
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
    this.baseBackoffMs = positiveFinite('baseBackoffMs', options.baseBackoffMs ?? 100);
    this.sleep = options.sleep ?? sleep;
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [this.topic], fromBeginning: false });
    this.accepting = true;
    await this.consumer.run({
      autoCommit: false,
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
    this.consumer.resume(this.topic, assignments.map(({ partition }) => partition));
  }

  caughtUp(): Promise<void> {
    return this.caughtUpPromise;
  }

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
    if (!this.accepting) return Promise.reject(new Error('inbound consumer is stopping'));
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
