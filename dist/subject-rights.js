"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.subjectRightsEventDefinitions = exports.SubjectRightsCapabilityManifestSchema = exports.SubjectRightsErasureFailedEventSchema = exports.SubjectRightsErasureCompletedEventSchema = exports.SubjectRightsErasureRequestedEventSchema = exports.SubjectRightsExportReadyEventSchema = exports.SubjectRightsStepCompletedEventSchema = exports.SubjectRightsRequestedEventSchema = exports.SUBJECT_RIGHTS_ERASURE_FAILED_EVENT = exports.SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT = exports.SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT = exports.SUBJECT_RIGHTS_EXPORT_READY_EVENT = exports.SUBJECT_RIGHTS_STEP_COMPLETED_EVENT = exports.SUBJECT_RIGHTS_REQUESTED_EVENT = exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION = void 0;
const zod_1 = require("zod");
exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION = 1;
exports.SUBJECT_RIGHTS_REQUESTED_EVENT = 'cura.compliance.subject-rights.requested';
exports.SUBJECT_RIGHTS_STEP_COMPLETED_EVENT = 'cura.compliance.subject-rights.step-completed';
exports.SUBJECT_RIGHTS_EXPORT_READY_EVENT = 'cura.compliance.subject-rights.export-ready';
exports.SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT = 'cura.compliance.subject-rights.erasure-requested';
exports.SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT = 'cura.compliance.subject-rights.erasure-completed';
exports.SUBJECT_RIGHTS_ERASURE_FAILED_EVENT = 'cura.compliance.subject-rights.erasure-failed';
const serviceNameSchema = zod_1.z
    .string()
    .min(3)
    .max(96)
    .regex(/^[a-z][a-z0-9-]*-service$/);
const safeReferenceSchema = zod_1.z
    .string()
    .min(3)
    .max(256)
    .regex(/^[A-Za-z0-9][A-Za-z0-9._:/#-]*$/);
const subjectReferenceKindSchema = zod_1.z.enum([
    'party',
    'user',
    'patient-reference',
    'tenant-account',
    'external-subject',
]);
const itemCountKindSchema = zod_1.z.enum(['subjectReferences', 'records', 'attachments', 'bundles']);
const subjectReferenceSchema = zod_1.z
    .object({
    subjectRef: safeReferenceSchema,
    subjectType: subjectReferenceKindSchema,
})
    .strict();
const itemCountsSchema = zod_1.z
    .object({
    subjectReferences: zod_1.z.number().int().nonnegative().optional(),
    records: zod_1.z.number().int().nonnegative().optional(),
    attachments: zod_1.z.number().int().nonnegative().optional(),
    bundles: zod_1.z.number().int().nonnegative().optional(),
})
    .strict()
    .refine((counts) => Object.values(counts).some((value) => value !== undefined), {
    message: 'itemCounts must include at least one count',
});
const eventEnvelopeSchema = zod_1.z
    .object({
    eventId: zod_1.z.string().uuid(),
    tenantId: zod_1.z.string().uuid(),
    actorId: zod_1.z.string().uuid().optional(),
    occurredAt: zod_1.z.string().datetime(),
    correlationId: safeReferenceSchema.optional(),
    traceId: safeReferenceSchema.optional(),
    causationId: safeReferenceSchema.optional(),
})
    .strict();
const phiKeyPattern = /^(dateofbirth|dob|birthdate|ssn|socialsecuritynumber|firstname|lastname|fullname|patientname|race|ethnicity|gender|address|phone|email|mrn|medicalrecordnumber|fhirid|fhirmeta|deceaseddate)$/i;
const ssnValuePattern = /\b\d{3}-?\d{2}-?\d{4}\b/;
function addPhiIssues(value, ctx, path) {
    if (typeof value === 'string') {
        if (ssnValuePattern.test(value)) {
            ctx.addIssue({
                code: 'custom',
                path,
                message: 'PHI-like value is not allowed in subject-rights event payloads',
            });
        }
        return;
    }
    if (Array.isArray(value)) {
        value.forEach((entry, index) => addPhiIssues(entry, ctx, [...path, index]));
        return;
    }
    if (value && typeof value === 'object') {
        for (const [key, entry] of Object.entries(value)) {
            const normalizedKey = key.replace(/[^A-Za-z]/g, '').toLowerCase();
            if (phiKeyPattern.test(normalizedKey)) {
                ctx.addIssue({
                    code: 'custom',
                    path: [...path, key],
                    message: 'PHI-like key is not allowed in subject-rights event payloads',
                });
            }
            addPhiIssues(entry, ctx, [...path, key]);
        }
    }
}
function subjectRightsEventSchema(type, payload) {
    return eventEnvelopeSchema
        .extend({
        type: zod_1.z.literal(type),
        payload,
    })
        .strict()
        .superRefine((event, ctx) => addPhiIssues(event.payload, ctx, ['payload']));
}
exports.SubjectRightsRequestedEventSchema = subjectRightsEventSchema(exports.SUBJECT_RIGHTS_REQUESTED_EVENT, zod_1.z
    .object({
    requestId: zod_1.z.string().uuid(),
    subject: subjectReferenceSchema,
    requestKind: zod_1.z.enum(['export', 'erasure']),
    dueBy: zod_1.z.string().datetime(),
    requestedScopes: zod_1.z.array(safeReferenceSchema).min(1).max(32),
})
    .strict());
exports.SubjectRightsStepCompletedEventSchema = subjectRightsEventSchema(exports.SUBJECT_RIGHTS_STEP_COMPLETED_EVENT, zod_1.z
    .object({
    requestId: zod_1.z.string().uuid(),
    serviceName: serviceNameSchema,
    step: zod_1.z.enum([
        'capability-checked',
        'reference-map-exported',
        'export-fragment-ready',
        'erasure-applied',
        'legal-hold-checked',
    ]),
    itemCounts: itemCountsSchema,
})
    .strict());
exports.SubjectRightsExportReadyEventSchema = subjectRightsEventSchema(exports.SUBJECT_RIGHTS_EXPORT_READY_EVENT, zod_1.z
    .object({
    requestId: zod_1.z.string().uuid(),
    bundleRef: zod_1.z
        .object({
        storageRef: safeReferenceSchema,
        checksumSha256: zod_1.z
            .string()
            .length(64)
            .regex(/^[a-f0-9]+$/),
    })
        .strict(),
    itemCounts: itemCountsSchema,
    expiresAt: zod_1.z.string().datetime(),
})
    .strict());
const erasureActionSchema = zod_1.z.enum(['delete', 'anonymize']);
exports.SubjectRightsErasureRequestedEventSchema = subjectRightsEventSchema(exports.SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT, zod_1.z
    .object({
    requestId: zod_1.z.string().uuid(),
    subject: subjectReferenceSchema,
    serviceName: serviceNameSchema,
    action: erasureActionSchema,
    dueBy: zod_1.z.string().datetime(),
    legalHold: zod_1.z.boolean(),
})
    .strict());
exports.SubjectRightsErasureCompletedEventSchema = subjectRightsEventSchema(exports.SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT, zod_1.z
    .object({
    requestId: zod_1.z.string().uuid(),
    serviceName: serviceNameSchema,
    action: erasureActionSchema,
    itemCounts: itemCountsSchema,
})
    .strict());
exports.SubjectRightsErasureFailedEventSchema = subjectRightsEventSchema(exports.SUBJECT_RIGHTS_ERASURE_FAILED_EVENT, zod_1.z
    .object({
    requestId: zod_1.z.string().uuid(),
    serviceName: serviceNameSchema,
    reason: zod_1.z.enum(['legal-hold', 'retryable', 'permanent']),
    retryable: zod_1.z.boolean(),
    failureRef: safeReferenceSchema.optional(),
})
    .superRefine((payload, ctx) => {
    const mustRetry = payload.reason === 'retryable';
    if (payload.retryable !== mustRetry) {
        ctx.addIssue({
            code: 'custom',
            message: '`retryable` must be true only when reason is retryable',
            path: ['retryable'],
        });
    }
})
    .strict());
const exportCapabilitySchema = zod_1.z
    .object({
    supported: zod_1.z.boolean(),
    producesBundle: zod_1.z.boolean(),
    itemCountKinds: zod_1.z.array(itemCountKindSchema).max(4),
})
    .superRefine((capability, ctx) => {
    if (capability.supported && capability.itemCountKinds.length === 0) {
        ctx.addIssue({
            code: 'custom',
            message: 'supported export capabilities must name at least one item count kind',
            path: ['itemCountKinds'],
        });
    }
    if (!capability.supported && capability.producesBundle) {
        ctx.addIssue({
            code: 'custom',
            message: 'unsupported export capabilities cannot produce bundles',
            path: ['producesBundle'],
        });
    }
})
    .strict();
const erasureCapabilitySchema = zod_1.z
    .object({
    supported: zod_1.z.boolean(),
    actions: zod_1.z.array(erasureActionSchema).max(2),
    legalHoldResponse: zod_1.z.enum(['block', 'defer', 'report-only']),
})
    .superRefine((capability, ctx) => {
    if (capability.supported && capability.actions.length === 0) {
        ctx.addIssue({
            code: 'custom',
            message: 'supported erasure capabilities must name at least one action',
            path: ['actions'],
        });
    }
    if (!capability.supported && capability.actions.length > 0) {
        ctx.addIssue({
            code: 'custom',
            message: 'unsupported erasure capabilities cannot advertise actions',
            path: ['actions'],
        });
    }
})
    .strict();
exports.SubjectRightsCapabilityManifestSchema = zod_1.z
    .object({
    manifestVersion: zod_1.z.literal(1),
    serviceName: serviceNameSchema,
    subjectReferenceKinds: zod_1.z.array(subjectReferenceKindSchema).min(1).max(16),
    export: exportCapabilitySchema,
    erasure: erasureCapabilitySchema,
})
    .strict();
exports.subjectRightsEventDefinitions = [
    {
        name: exports.SUBJECT_RIGHTS_REQUESTED_EVENT,
        schemaVersion: exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
        schema: exports.SubjectRightsRequestedEventSchema,
    },
    {
        name: exports.SUBJECT_RIGHTS_STEP_COMPLETED_EVENT,
        schemaVersion: exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
        schema: exports.SubjectRightsStepCompletedEventSchema,
    },
    {
        name: exports.SUBJECT_RIGHTS_EXPORT_READY_EVENT,
        schemaVersion: exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
        schema: exports.SubjectRightsExportReadyEventSchema,
    },
    {
        name: exports.SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT,
        schemaVersion: exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
        schema: exports.SubjectRightsErasureRequestedEventSchema,
    },
    {
        name: exports.SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT,
        schemaVersion: exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
        schema: exports.SubjectRightsErasureCompletedEventSchema,
    },
    {
        name: exports.SUBJECT_RIGHTS_ERASURE_FAILED_EVENT,
        schemaVersion: exports.SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
        schema: exports.SubjectRightsErasureFailedEventSchema,
    },
];
