import { z } from 'zod';

export const SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION = 1;

export const SUBJECT_RIGHTS_REQUESTED_EVENT = 'cura.compliance.subject-rights.requested';
export const SUBJECT_RIGHTS_STEP_COMPLETED_EVENT = 'cura.compliance.subject-rights.step-completed';
export const SUBJECT_RIGHTS_EXPORT_READY_EVENT = 'cura.compliance.subject-rights.export-ready';
export const SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT =
  'cura.compliance.subject-rights.erasure-requested';
export const SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT =
  'cura.compliance.subject-rights.erasure-completed';
export const SUBJECT_RIGHTS_ERASURE_FAILED_EVENT = 'cura.compliance.subject-rights.erasure-failed';

const serviceNameSchema = z
  .string()
  .min(3)
  .max(96)
  .regex(/^[a-z][a-z0-9-]*-service$/);

const safeReferenceSchema = z
  .string()
  .min(3)
  .max(256)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._:/#-]*$/);

const subjectReferenceKindSchema = z.enum([
  'party',
  'user',
  'patient-reference',
  'tenant-account',
  'external-subject',
]);

const itemCountKindSchema = z.enum(['subjectReferences', 'records', 'attachments', 'bundles']);

const subjectReferenceSchema = z
  .object({
    subjectRef: safeReferenceSchema,
    subjectType: subjectReferenceKindSchema,
  })
  .strict();

const itemCountsSchema = z
  .object({
    subjectReferences: z.number().int().nonnegative().optional(),
    records: z.number().int().nonnegative().optional(),
    attachments: z.number().int().nonnegative().optional(),
    bundles: z.number().int().nonnegative().optional(),
  })
  .strict()
  .refine((counts) => Object.values(counts).some((value) => value !== undefined), {
    message: 'itemCounts must include at least one count',
  });

const eventEnvelopeSchema = z
  .object({
    eventId: z.string().uuid(),
    tenantId: z.string().uuid(),
    actorId: z.string().uuid().optional(),
    occurredAt: z.string().datetime(),
    correlationId: safeReferenceSchema.optional(),
    traceId: safeReferenceSchema.optional(),
    causationId: safeReferenceSchema.optional(),
  })
  .strict();

const phiKeyPattern =
  /^(dateofbirth|dob|birthdate|ssn|socialsecuritynumber|firstname|lastname|fullname|patientname|race|ethnicity|gender|address|phone|email|mrn|medicalrecordnumber|fhirid|fhirmeta|deceaseddate)$/i;
const ssnValuePattern = /\b\d{3}-?\d{2}-?\d{4}\b/;

function addPhiIssues(value: unknown, ctx: z.RefinementCtx, path: Array<string | number>): void {
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

function subjectRightsEventSchema<const EventName extends string, Payload extends z.ZodType>(
  type: EventName,
  payload: Payload,
) {
  return eventEnvelopeSchema
    .extend({
      type: z.literal(type),
      payload,
    })
    .strict()
    .superRefine((event, ctx) =>
      addPhiIssues((event as { payload: unknown }).payload, ctx, ['payload']),
    );
}

export const SubjectRightsRequestedEventSchema = subjectRightsEventSchema(
  SUBJECT_RIGHTS_REQUESTED_EVENT,
  z
    .object({
      requestId: z.string().uuid(),
      subject: subjectReferenceSchema,
      requestKind: z.enum(['export', 'erasure']),
      dueBy: z.string().datetime(),
      requestedScopes: z.array(safeReferenceSchema).min(1).max(32),
    })
    .strict(),
);

export const SubjectRightsStepCompletedEventSchema = subjectRightsEventSchema(
  SUBJECT_RIGHTS_STEP_COMPLETED_EVENT,
  z
    .object({
      requestId: z.string().uuid(),
      serviceName: serviceNameSchema,
      step: z.enum([
        'capability-checked',
        'reference-map-exported',
        'export-fragment-ready',
        'erasure-applied',
        'legal-hold-checked',
      ]),
      itemCounts: itemCountsSchema,
    })
    .strict(),
);

export const SubjectRightsExportReadyEventSchema = subjectRightsEventSchema(
  SUBJECT_RIGHTS_EXPORT_READY_EVENT,
  z
    .object({
      requestId: z.string().uuid(),
      bundleRef: z
        .object({
          storageRef: safeReferenceSchema,
          checksumSha256: z
            .string()
            .length(64)
            .regex(/^[a-f0-9]+$/),
        })
        .strict(),
      itemCounts: itemCountsSchema,
      expiresAt: z.string().datetime(),
    })
    .strict(),
);

const erasureActionSchema = z.enum(['delete', 'anonymize']);

export const SubjectRightsErasureRequestedEventSchema = subjectRightsEventSchema(
  SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT,
  z
    .object({
      requestId: z.string().uuid(),
      subject: subjectReferenceSchema,
      serviceName: serviceNameSchema,
      action: erasureActionSchema,
      dueBy: z.string().datetime(),
      legalHold: z.boolean(),
    })
    .strict(),
);

export const SubjectRightsErasureCompletedEventSchema = subjectRightsEventSchema(
  SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT,
  z
    .object({
      requestId: z.string().uuid(),
      serviceName: serviceNameSchema,
      action: erasureActionSchema,
      itemCounts: itemCountsSchema,
    })
    .strict(),
);

export const SubjectRightsErasureFailedEventSchema = subjectRightsEventSchema(
  SUBJECT_RIGHTS_ERASURE_FAILED_EVENT,
  z
    .object({
      requestId: z.string().uuid(),
      serviceName: serviceNameSchema,
      reason: z.enum(['legal-hold', 'retryable', 'permanent']),
      retryable: z.boolean(),
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
    .strict(),
);

const exportCapabilitySchema = z
  .object({
    supported: z.boolean(),
    producesBundle: z.boolean(),
    itemCountKinds: z.array(itemCountKindSchema).max(4),
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

const erasureCapabilitySchema = z
  .object({
    supported: z.boolean(),
    actions: z.array(erasureActionSchema).max(2),
    legalHoldResponse: z.enum(['block', 'defer', 'report-only']),
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

export const SubjectRightsCapabilityManifestSchema = z
  .object({
    manifestVersion: z.literal(1),
    serviceName: serviceNameSchema,
    subjectReferenceKinds: z.array(subjectReferenceKindSchema).min(1).max(16),
    export: exportCapabilitySchema,
    erasure: erasureCapabilitySchema,
  })
  .strict();

export const subjectRightsEventDefinitions = [
  {
    name: SUBJECT_RIGHTS_REQUESTED_EVENT,
    schemaVersion: SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
    schema: SubjectRightsRequestedEventSchema,
  },
  {
    name: SUBJECT_RIGHTS_STEP_COMPLETED_EVENT,
    schemaVersion: SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
    schema: SubjectRightsStepCompletedEventSchema,
  },
  {
    name: SUBJECT_RIGHTS_EXPORT_READY_EVENT,
    schemaVersion: SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
    schema: SubjectRightsExportReadyEventSchema,
  },
  {
    name: SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT,
    schemaVersion: SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
    schema: SubjectRightsErasureRequestedEventSchema,
  },
  {
    name: SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT,
    schemaVersion: SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
    schema: SubjectRightsErasureCompletedEventSchema,
  },
  {
    name: SUBJECT_RIGHTS_ERASURE_FAILED_EVENT,
    schemaVersion: SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
    schema: SubjectRightsErasureFailedEventSchema,
  },
] as const;

export type SubjectReference = z.infer<typeof subjectReferenceSchema>;
export type SubjectRightsRequestedEvent = z.infer<typeof SubjectRightsRequestedEventSchema>;
export type SubjectRightsStepCompletedEvent = z.infer<typeof SubjectRightsStepCompletedEventSchema>;
export type SubjectRightsExportReadyEvent = z.infer<typeof SubjectRightsExportReadyEventSchema>;
export type SubjectRightsErasureRequestedEvent = z.infer<
  typeof SubjectRightsErasureRequestedEventSchema
>;
export type SubjectRightsErasureCompletedEvent = z.infer<
  typeof SubjectRightsErasureCompletedEventSchema
>;
export type SubjectRightsErasureFailedEvent = z.infer<typeof SubjectRightsErasureFailedEventSchema>;
export type SubjectRightsCapabilityManifest = z.infer<typeof SubjectRightsCapabilityManifestSchema>;
