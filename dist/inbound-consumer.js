"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DurableInboundConsumer = void 0;
const promises_1 = require("node:timers/promises");
const nextOffset = (offset) => (BigInt(offset) + 1n).toString();
const decode = new TextDecoder('utf-8', { fatal: true });
const nonnegativeInteger = (name, value) => {
    if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
        throw new TypeError(`${name} must be a finite nonnegative integer`);
    }
    return value;
};
const positiveFinite = (name, value) => {
    if (!Number.isFinite(value) || value <= 0)
        throw new TypeError(`${name} must be finite and positive`);
    return value;
};
const partitionKey = (record) => `${record.topic}:${record.partition}`;
/**
 * Broker-neutral, at-least-once inbound orchestration.
 *
 * Ordering is effect, durable checkpoint, then broker commit. Poison records are
 * durably dead-lettered before the same checkpoint and commit sequence.
 */
class DurableInboundConsumer {
    consumer;
    topic;
    schema;
    handler;
    offsets;
    deadLetters;
    maxRetries;
    baseBackoffMs;
    sleep;
    partitions = new Map();
    bootHighWatermarks = new Map();
    caughtPartitions = new Set();
    accepting = false;
    caughtUpResolve;
    caughtUpPromise = new Promise((resolve) => {
        this.caughtUpResolve = resolve;
    });
    constructor(options) {
        this.consumer = options.consumer;
        this.topic = options.topic;
        this.schema = options.schema;
        this.handler = options.handler;
        this.offsets = options.offsets;
        this.deadLetters = options.deadLetters;
        this.maxRetries = nonnegativeInteger('maxRetries', options.maxRetries ?? 5);
        this.baseBackoffMs = positiveFinite('baseBackoffMs', options.baseBackoffMs ?? 100);
        this.sleep = options.sleep ?? promises_1.setTimeout;
    }
    async start() {
        await this.consumer.connect();
        await this.consumer.subscribe({ topics: [this.topic], fromBeginning: false });
        this.accepting = true;
        await this.consumer.run({
            autoCommit: false,
            eachMessage: (record) => this.enqueue(record),
        });
        const assignments = await this.consumer.assignedPartitions(this.topic);
        const checkpoints = new Map((await this.offsets.loadTopic(this.topic)).map(({ partition, offset }) => [partition, offset]));
        for (const assignment of assignments) {
            const start = checkpoints.get(assignment.partition) ?? assignment.low;
            this.consumer.seek({ topic: this.topic, partition: assignment.partition, offset: start });
            const key = `${this.topic}:${assignment.partition}`;
            this.bootHighWatermarks.set(key, BigInt(assignment.high));
            if (BigInt(start) >= BigInt(assignment.high))
                this.caughtPartitions.add(key);
        }
        if (this.bootHighWatermarks.size === this.caughtPartitions.size)
            this.caughtUpResolve();
        this.consumer.resume(this.topic, assignments.map(({ partition }) => partition));
    }
    caughtUp() {
        return this.caughtUpPromise;
    }
    async shutdown() {
        this.accepting = false;
        let primary;
        try {
            try {
                await this.consumer.stop();
            }
            catch (error) {
                primary = error;
            }
            const results = await Promise.allSettled(this.partitions.values());
            primary ??= results.find((result) => result.status === 'rejected')?.reason;
        }
        finally {
            try {
                await this.consumer.disconnect();
            }
            catch (error) {
                primary ??= error;
            }
        }
        if (primary !== undefined)
            throw primary;
    }
    enqueue(record) {
        if (!this.accepting)
            return Promise.reject(new Error('inbound consumer is stopping'));
        const key = partitionKey(record);
        const pending = (this.partitions.get(key) ?? Promise.resolve()).then(() => this.process(record));
        this.partitions.set(key, pending);
        void pending
            .finally(() => {
            if (this.partitions.get(key) === pending)
                this.partitions.delete(key);
        })
            .catch(() => { });
        return pending;
    }
    async process(record) {
        let parsed;
        try {
            if (record.value === null)
                throw new Error('record value is null');
            parsed = this.schema.parse(JSON.parse(decode.decode(record.value)));
        }
        catch (error) {
            await this.deadLetters.write({ record, reason: 'malformed', error });
            await this.advance(record);
            return;
        }
        let lastError;
        // Retries are intentionally sequential to preserve partition order and backoff.
        /* oxlint-disable no-await-in-loop */
        for (let attempt = 0; attempt <= this.maxRetries; attempt += 1) {
            let disposition = 'retry';
            try {
                disposition = await this.handler.handle(parsed, record);
            }
            catch (error) {
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
    async advance(record) {
        const offset = nextOffset(record.offset);
        await this.offsets.save(record.topic, record.partition, offset);
        await this.consumer.commitOffsets([{ topic: record.topic, partition: record.partition, offset }]);
        const key = partitionKey(record);
        const bootHighWatermark = this.bootHighWatermarks.get(key);
        if (bootHighWatermark !== undefined && BigInt(offset) >= bootHighWatermark) {
            this.caughtPartitions.add(key);
            if (this.caughtPartitions.size === this.bootHighWatermarks.size)
                this.caughtUpResolve();
        }
    }
}
exports.DurableInboundConsumer = DurableInboundConsumer;
