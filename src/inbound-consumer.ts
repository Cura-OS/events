import type { z } from 'zod';

export interface ConsumerRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  /** Exclusive broker high-water offset captured with this delivery. */
  readonly highWatermark: string;
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
  /** Capture exclusive partition high-water offsets before live intake starts. */
  highWaterMarks(topic: string): Promise<readonly { partition: number; offset: string }[]>;
  seek(position: TopicPartitionOffset): void;
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
    this.maxRetries = Math.max(0, options.maxRetries ?? 5);
    this.baseBackoffMs = Math.max(1, options.baseBackoffMs ?? 100);
    this.sleep = options.sleep ?? ((milliseconds) => Bun.sleep(milliseconds));
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [this.topic], fromBeginning: false });
    const checkpoints = await this.offsets.loadTopic(this.topic);
    for (const checkpoint of checkpoints) this.consumer.seek({ topic: this.topic, ...checkpoint });
    const highWatermarks = await this.consumer.highWaterMarks(this.topic);
    const durableOffsets = await Promise.all(
      highWatermarks.map(({ partition }) => this.offsets.load(this.topic, partition)),
    );
    for (const [index, highWatermark] of highWatermarks.entries()) {
      const key = `${this.topic}:${highWatermark.partition}`;
      this.bootHighWatermarks.set(key, BigInt(highWatermark.offset));
      const checkpoint = durableOffsets[index];
      if (checkpoint !== undefined && BigInt(checkpoint) >= BigInt(highWatermark.offset)) {
        this.caughtPartitions.add(key);
      }
    }
    if (this.bootHighWatermarks.size === this.caughtPartitions.size) this.caughtUpResolve();
    this.accepting = true;
    await this.consumer.run({
      autoCommit: false,
      eachMessage: (record) => this.enqueue(record),
    });
  }

  caughtUp(): Promise<void> {
    return this.caughtUpPromise;
  }

  async shutdown(): Promise<void> {
    this.accepting = false;
    await this.consumer.stop();
    await Promise.all(this.partitions.values());
    await this.consumer.disconnect();
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
      parsed = this.schema.parse(JSON.parse(Buffer.from(record.value).toString('utf8')));
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
