"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DurableInboundConsumer = void 0;
const nextOffset = (offset) => (BigInt(offset) + 1n).toString();
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
        this.maxRetries = Math.max(0, options.maxRetries ?? 5);
        this.baseBackoffMs = Math.max(1, options.baseBackoffMs ?? 100);
        this.sleep = options.sleep ?? ((milliseconds) => Bun.sleep(milliseconds));
    }
    async start() {
        await this.consumer.connect();
        await this.consumer.subscribe({ topics: [this.topic], fromBeginning: false });
        const checkpoints = await this.offsets.loadTopic(this.topic);
        for (const checkpoint of checkpoints)
            this.consumer.seek({ topic: this.topic, ...checkpoint });
        const highWatermarks = await this.consumer.highWaterMarks(this.topic);
        const durableOffsets = await Promise.all(highWatermarks.map(({ partition }) => this.offsets.load(this.topic, partition)));
        for (const [index, highWatermark] of highWatermarks.entries()) {
            const key = `${this.topic}:${highWatermark.partition}`;
            this.bootHighWatermarks.set(key, BigInt(highWatermark.offset));
            const checkpoint = durableOffsets[index];
            if (checkpoint !== undefined && BigInt(checkpoint) >= BigInt(highWatermark.offset)) {
                this.caughtPartitions.add(key);
            }
        }
        if (this.bootHighWatermarks.size === this.caughtPartitions.size)
            this.caughtUpResolve();
        this.accepting = true;
        await this.consumer.run({
            autoCommit: false,
            eachMessage: (record) => this.enqueue(record),
        });
    }
    caughtUp() {
        return this.caughtUpPromise;
    }
    async shutdown() {
        this.accepting = false;
        await this.consumer.stop();
        await Promise.all(this.partitions.values());
        await this.consumer.disconnect();
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
            parsed = this.schema.parse(JSON.parse(Buffer.from(record.value).toString('utf8')));
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
