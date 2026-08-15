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

/** Paused broker assignment with retained offset bounds. */
export interface ConsumerAssignment {
  readonly partition: number;
  readonly low: string;
  readonly high: string;
  readonly position: string;
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
    /** Initialize every paused assignment generation before broker delivery resumes. */
    readonly eachAssignment: (assignments: readonly ConsumerAssignment[]) => Promise<void>;
  }): Promise<void>;
  commitOffsets(offsets: readonly TopicPartitionOffset[]): Promise<void>;
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
const nextOffset = (offset: string): string => {
  if (!/^\d+$/.test(offset)) throw new RangeError('record offset must be a nonnegative integer');
  return (BigInt(offset) + 1n).toString();
};
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
  private closed = false;
  private assignmentEpoch = 0;
  private assignmentTail: Promise<void> = Promise.resolve();
  private readonly assignmentFailures: unknown[] = [];
  private catchUpSettled = false;
  private caughtUpResolve!: () => void;
  private caughtUpReject!: (error: unknown) => void;
  private caughtUpPromise!: Promise<void>;

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
    this.resetCatchUp();
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
        eachAssignment: (assignments) => this.enqueueAssignments(assignments),
      });
    } catch (error) {
      this.accepting = false;
      this.rejectCatchUp(error);
      const cleanupErrors = await this.cleanup();
      throw this.withCleanup(error, cleanupErrors, 'consumer startup and cleanup failed');
    }
  }

  /** Resolve once every boot-time partition high watermark is checkpointed. */
  caughtUp(): Promise<void> {
    return this.caughtUpPromise;
  }

  /** Stop intake, settle partition jobs, disconnect, then propagate the primary failure. */
  async shutdown(): Promise<void> {
    this.closed = true;
    this.accepting = false;
    this.assignmentEpoch += 1;
    this.rejectCatchUp(new Error('consumer shut down before catch-up'));
    const errors: unknown[] = [];
    try { await this.consumer.stop(); } catch (error) { errors.push(error); }
    await this.assignmentTail;
    errors.push(...this.assignmentFailures, ...await this.cleanup(false));
    if (errors.length > 0) throw this.withCleanup(errors[0], errors.slice(1), 'consumer shutdown failed');
  }

  private async cleanup(stop = true): Promise<unknown[]> {
    const errors: unknown[] = [];
    if (stop) {
      try { await this.consumer.stop(); } catch (error) { errors.push(error); }
    }
    const jobs = await Promise.allSettled(this.partitions.values());
    for (const job of jobs) if (job.status === 'rejected') errors.push(job.reason);
    try { await this.consumer.disconnect(); } catch (error) { errors.push(error); }
    return errors;
  }

  private withCleanup(primary: unknown, cleanupErrors: readonly unknown[], message: string): unknown {
    if (cleanupErrors.length === 0) return primary;
    // oxlint-disable-next-line preserve-caught-error -- AggregateError carries primary in errors and cause.
    return new AggregateError([primary, ...cleanupErrors], message, { cause: primary });
  }

  private resetCatchUp(): void {
    this.catchUpSettled = false;
    this.caughtUpPromise = new Promise<void>((resolve, reject) => {
      this.caughtUpResolve = () => { this.catchUpSettled = true; resolve(); };
      this.caughtUpReject = (error) => { this.catchUpSettled = true; reject(error); };
    });
    void this.caughtUpPromise.catch(() => {});
  }

  private rejectCatchUp(error: unknown): void {
    if (!this.catchUpSettled) this.caughtUpReject(error);
  }

  private enqueueAssignments(assignments: readonly ConsumerAssignment[]): Promise<void> {
    if (this.closed) return Promise.resolve();
    const epoch = ++this.assignmentEpoch;
    if (epoch > 1) {
      this.rejectCatchUp(new Error('assignment replaced before catch-up'));
      this.resetCatchUp();
    }
    const pending = this.assignmentTail.then(() => this.initializeAssignments(assignments, epoch));
    this.assignmentTail = pending.catch((error) => { this.assignmentFailures.push(error); });
    return pending;
  }

  private async initializeAssignments(
    assignments: readonly ConsumerAssignment[],
    epoch: number,
  ): Promise<void> {
    this.accepting = false;
    try {
      if (epoch > 1) {
        const jobs = await Promise.allSettled(this.partitions.values());
        const rejected = jobs.find((job) => job.status === 'rejected');
        if (rejected?.status === 'rejected') throw rejected.reason;
        if (epoch !== this.assignmentEpoch) return;
      }
      const partitions = new Set<number>();
      for (const assignment of assignments) {
        nonnegativeInteger('assignment partition', assignment.partition);
        if (partitions.has(assignment.partition)) {
          throw new RangeError(`duplicate assignment partition ${assignment.partition}`);
        }
        partitions.add(assignment.partition);
      }
      const checkpoints = new Map(
        (await this.offsets.loadTopic(this.topic)).map(({ partition, offset }) => [partition, offset]),
      );
      if (epoch !== this.assignmentEpoch || this.closed) return;
      const initialized = assignments.map((assignment) => {
        let low: bigint;
        let high: bigint;
        let position: bigint;
        let start: bigint;
        const checkpoint = checkpoints.get(assignment.partition);
        try {
          low = BigInt(assignment.low);
          high = BigInt(assignment.high);
          position = BigInt(assignment.position);
          start = BigInt(checkpoint ?? assignment.low);
        } catch (error) {
          throw new RangeError(
            `checkpoint or broker bounds are invalid for partition ${assignment.partition}`,
            { cause: error },
          );
        }
        if (
          low < 0n || high < 0n || position < 0n ||
          low > high || position < low || position > high || start < low || start > high
        ) {
          throw new RangeError(`checkpoint is outside broker bounds for partition ${assignment.partition}`);
        }
        return { partition: assignment.partition, high, start };
      });
      if (epoch !== this.assignmentEpoch || this.closed) return;
      this.bootHighWatermarks.clear();
      this.caughtPartitions.clear();
      for (const { partition, high, start } of initialized) {
        const offset = start.toString();
        this.consumer.seek({ topic: this.topic, partition, offset });
        const key = `${this.topic}:${partition}`;
        this.bootHighWatermarks.set(key, high);
        if (start === high) this.caughtPartitions.add(key);
      }
      if (this.bootHighWatermarks.size === this.caughtPartitions.size) this.caughtUpResolve();
      if (epoch !== this.assignmentEpoch || this.closed) return;
      this.accepting = true;
      this.consumer.resume(this.topic, initialized.map(({ partition }) => partition));
    } catch (error) {
      if (epoch === this.assignmentEpoch) {
        this.rejectCatchUp(error);
        throw error;
      }
    }
  }

  private enqueue(record: ConsumerRecord): Promise<void> {
    if (!this.accepting) return Promise.reject(new Error('inbound consumer is not initialized'));
    if (record.topic !== this.topic || !Number.isSafeInteger(record.partition) || record.partition < 0) {
      return Promise.reject(new RangeError('record topic or partition is invalid'));
    }
    const key = partitionKey(record);
    const pending = (this.partitions.get(key) ?? Promise.resolve()).then(() =>
      this.process(record),
    );
    this.partitions.set(key, pending);
    void pending
      .catch((error) => this.rejectCatchUp(error))
      .finally(() => {
        if (this.partitions.get(key) === pending) this.partitions.delete(key);
      });
    return pending;
  }

  private async process(record: ConsumerRecord): Promise<void> {
    const offset = nextOffset(record.offset);
    let parsed: T;
    try {
      if (record.value === null) throw new Error('record value is null');
      parsed = this.schema.parse(JSON.parse(decode.decode(record.value)));
    } catch (error) {
      await this.deadLetters.write({ record, reason: 'malformed', error });
      await this.advance(record, offset);
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
        await this.advance(record, offset);
        return;
      }
      if (disposition === 'dead-letter') {
        await this.deadLetters.write({ record, reason: 'handler-rejected' });
        await this.advance(record, offset);
        return;
      }
      if (attempt < this.maxRetries) {
        await this.sleep(this.baseBackoffMs * 2 ** attempt);
      }
    }
    /* oxlint-enable no-await-in-loop */
    await this.deadLetters.write({ record, reason: 'retries-exhausted', error: lastError });
    await this.advance(record, offset);
  }

  private async advance(record: ConsumerRecord, offset: string): Promise<void> {
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
