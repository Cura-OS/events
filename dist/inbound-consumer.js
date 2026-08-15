"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DurableInboundConsumer = void 0;
const promises_1 = require("node:timers/promises");
const MAX_TIMER_DELAY_MS = 2_147_483_647;
const nextOffset = (offset) => {
    if (!/^\d+$/.test(offset))
        throw new RangeError('record offset must be a nonnegative integer');
    return (BigInt(offset) + 1n).toString();
};
const decode = new TextDecoder('utf-8', { fatal: true });
const nonnegativeInteger = (name, value) => {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw new TypeError(`${name} must be a safe nonnegative integer`);
    }
    return value;
};
const positiveInteger = (name, value) => {
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw new TypeError(`${name} must be a safe positive integer`);
    }
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
    assignmentEpoch = 0;
    assignmentTail = Promise.resolve();
    catchUpSettled = false;
    caughtUpResolve;
    caughtUpReject;
    caughtUpPromise;
    constructor(options) {
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
        this.sleep = options.sleep ?? promises_1.setTimeout;
        this.resetCatchUp();
    }
    /** Connect, initialize paused assignments, seek durable starts, then resume intake. */
    async start() {
        try {
            await this.consumer.connect();
            await this.consumer.subscribe({ topics: [this.topic], fromBeginning: false });
            await this.consumer.run({
                autoCommit: false,
                pauseOnAssignment: true,
                eachMessage: (record) => this.enqueue(record),
                eachAssignment: (assignments) => this.enqueueAssignments(assignments),
            });
        }
        catch (error) {
            this.accepting = false;
            this.rejectCatchUp(error);
            const cleanupErrors = await this.cleanup();
            throw this.withCleanup(error, cleanupErrors, 'consumer startup and cleanup failed');
        }
    }
    /** Resolve once every boot-time partition high watermark is checkpointed. */
    caughtUp() {
        return this.caughtUpPromise;
    }
    /** Stop intake, settle partition jobs, disconnect, then propagate the primary failure. */
    async shutdown() {
        this.accepting = false;
        this.rejectCatchUp(new Error('consumer shut down before catch-up'));
        const cleanupErrors = await this.cleanup();
        if (cleanupErrors.length > 0)
            throw this.withCleanup(cleanupErrors[0], cleanupErrors.slice(1), 'consumer shutdown failed');
    }
    async cleanup() {
        const errors = [];
        try {
            await this.consumer.stop();
        }
        catch (error) {
            errors.push(error);
        }
        const jobs = await Promise.allSettled(this.partitions.values());
        for (const job of jobs)
            if (job.status === 'rejected')
                errors.push(job.reason);
        try {
            await this.consumer.disconnect();
        }
        catch (error) {
            errors.push(error);
        }
        return errors;
    }
    withCleanup(primary, cleanupErrors, message) {
        if (cleanupErrors.length === 0)
            return primary;
        // oxlint-disable-next-line preserve-caught-error -- AggregateError carries primary in errors and cause.
        return new AggregateError([primary, ...cleanupErrors], message, { cause: primary });
    }
    resetCatchUp() {
        this.catchUpSettled = false;
        this.caughtUpPromise = new Promise((resolve, reject) => {
            this.caughtUpResolve = () => { this.catchUpSettled = true; resolve(); };
            this.caughtUpReject = (error) => { this.catchUpSettled = true; reject(error); };
        });
        void this.caughtUpPromise.catch(() => { });
    }
    rejectCatchUp(error) {
        if (!this.catchUpSettled)
            this.caughtUpReject(error);
    }
    enqueueAssignments(assignments) {
        const epoch = ++this.assignmentEpoch;
        const pending = this.assignmentTail.then(() => this.initializeAssignments(assignments, epoch));
        this.assignmentTail = pending.catch(() => { });
        return pending;
    }
    async initializeAssignments(assignments, epoch) {
        this.accepting = false;
        try {
            if (epoch > 1) {
                this.rejectCatchUp(new Error('assignment replaced before catch-up'));
                const jobs = await Promise.allSettled(this.partitions.values());
                const rejected = jobs.find((job) => job.status === 'rejected');
                if (rejected?.status === 'rejected')
                    throw rejected.reason;
                if (epoch !== this.assignmentEpoch)
                    return;
                this.resetCatchUp();
            }
            const checkpoints = new Map((await this.offsets.loadTopic(this.topic)).map(({ partition, offset }) => [partition, offset]));
            if (epoch !== this.assignmentEpoch)
                return;
            this.bootHighWatermarks.clear();
            this.caughtPartitions.clear();
            for (const assignment of assignments) {
                let low;
                let high;
                let position;
                let start;
                const checkpoint = checkpoints.get(assignment.partition);
                try {
                    low = BigInt(assignment.low);
                    high = BigInt(assignment.high);
                    position = BigInt(assignment.position);
                    start = BigInt(checkpoint ?? assignment.low);
                }
                catch (error) {
                    throw new RangeError(`checkpoint or broker bounds are invalid for partition ${assignment.partition}`, { cause: error });
                }
                if (low > high || position < low || position > high || start < low || start > high) {
                    throw new RangeError(`checkpoint is outside broker bounds for partition ${assignment.partition}`);
                }
                const offset = start.toString();
                this.consumer.seek({ topic: this.topic, partition: assignment.partition, offset });
                const key = `${this.topic}:${assignment.partition}`;
                this.bootHighWatermarks.set(key, high);
                if (start === high)
                    this.caughtPartitions.add(key);
            }
            if (this.bootHighWatermarks.size === this.caughtPartitions.size)
                this.caughtUpResolve();
            this.accepting = true;
            this.consumer.resume(this.topic, assignments.map(({ partition }) => partition));
        }
        catch (error) {
            this.rejectCatchUp(error);
            throw error;
        }
    }
    enqueue(record) {
        if (!this.accepting)
            return Promise.reject(new Error('inbound consumer is not initialized'));
        const key = partitionKey(record);
        const pending = (this.partitions.get(key) ?? Promise.resolve()).then(() => this.process(record));
        this.partitions.set(key, pending);
        void pending
            .catch((error) => this.rejectCatchUp(error))
            .finally(() => {
            if (this.partitions.get(key) === pending)
                this.partitions.delete(key);
        });
        return pending;
    }
    async process(record) {
        const offset = nextOffset(record.offset);
        let parsed;
        try {
            if (record.value === null)
                throw new Error('record value is null');
            parsed = this.schema.parse(JSON.parse(decode.decode(record.value)));
        }
        catch (error) {
            await this.deadLetters.write({ record, reason: 'malformed', error });
            await this.advance(record, offset);
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
    async advance(record, offset) {
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
