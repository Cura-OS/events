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
    subscribe(config: {
        readonly topics: readonly string[];
        readonly fromBeginning: boolean;
    }): Promise<void>;
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
    loadTopic(topic: string): Promise<readonly {
        partition: number;
        offset: string;
    }[]>;
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
    private readonly assignedPartitions;
    private accepting;
    private closed;
    private assignmentEpoch;
    private assignmentTail;
    private readonly assignmentFailures;
    private rejectInitialAssignment?;
    private startPromise?;
    private shutdownPromise?;
    private tornDown;
    private connected;
    private disconnected;
    private catchUpSettled;
    private caughtUpResolve;
    private caughtUpReject;
    private caughtUpPromise;
    constructor(options: DurableInboundConsumerOptions<T>);
    /** Connect, initialize paused assignments, seek durable starts, then resume intake. */
    start(): Promise<void>;
    private startInternal;
    /** Resolve once every boot-time partition high watermark is checkpointed. */
    caughtUp(): Promise<void>;
    /** Stop intake, settle partition jobs, disconnect, then propagate the primary failure. */
    shutdown(): Promise<void>;
    private shutdownInternal;
    private cleanup;
    private disconnect;
    private withCleanup;
    private resetCatchUp;
    private rejectCatchUp;
    private enqueueAssignments;
    private initializeAssignments;
    private enqueue;
    private process;
    private advance;
}
//# sourceMappingURL=inbound-consumer.d.ts.map