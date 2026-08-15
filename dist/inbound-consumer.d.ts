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
    subscribe(config: {
        readonly topics: readonly string[];
        readonly fromBeginning: boolean;
    }): Promise<void>;
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
    loadTopic(topic: string): Promise<readonly {
        partition: number;
        offset: string;
    }[]>;
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
/**
 * Broker-neutral, at-least-once inbound orchestration.
 *
 * Ordering is effect, durable checkpoint, then broker commit. Poison records are
 * durably dead-lettered before the same checkpoint and commit sequence.
 */
export declare class DurableInboundConsumer<T = unknown> {
    private readonly consumer;
    private readonly topic;
    private readonly schema;
    private readonly handler;
    private readonly offsets;
    private readonly deadLetters;
    private readonly maxRetries;
    private readonly baseBackoffMs;
    private readonly sleep;
    private readonly partitions;
    private readonly bootHighWatermarks;
    private readonly caughtPartitions;
    private accepting;
    private caughtUpResolve;
    private readonly caughtUpPromise;
    constructor(options: DurableInboundConsumerOptions<T>);
    start(): Promise<void>;
    caughtUp(): Promise<void>;
    shutdown(): Promise<void>;
    private enqueue;
    private process;
    private advance;
}
//# sourceMappingURL=inbound-consumer.d.ts.map