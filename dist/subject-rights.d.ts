import { z } from 'zod';
export declare const SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION = 1;
export declare const SUBJECT_RIGHTS_REQUESTED_EVENT = "cura.compliance.subject-rights.requested";
export declare const SUBJECT_RIGHTS_STEP_COMPLETED_EVENT = "cura.compliance.subject-rights.step-completed";
export declare const SUBJECT_RIGHTS_EXPORT_READY_EVENT = "cura.compliance.subject-rights.export-ready";
export declare const SUBJECT_RIGHTS_ERASURE_REQUESTED_EVENT = "cura.compliance.subject-rights.erasure-requested";
export declare const SUBJECT_RIGHTS_ERASURE_COMPLETED_EVENT = "cura.compliance.subject-rights.erasure-completed";
export declare const SUBJECT_RIGHTS_ERASURE_FAILED_EVENT = "cura.compliance.subject-rights.erasure-failed";
declare const subjectReferenceSchema: z.ZodObject<{
    subjectRef: z.ZodString;
    subjectType: z.ZodEnum<{
        party: "party";
        user: "user";
        "patient-reference": "patient-reference";
        "tenant-account": "tenant-account";
        "external-subject": "external-subject";
    }>;
}, z.core.$strict>;
export declare const SubjectRightsRequestedEventSchema: z.ZodObject<{
    eventId: z.ZodString;
    tenantId: z.ZodString;
    actorId: z.ZodOptional<z.ZodString>;
    occurredAt: z.ZodString;
    correlationId: z.ZodOptional<z.ZodString>;
    traceId: z.ZodOptional<z.ZodString>;
    causationId: z.ZodOptional<z.ZodString>;
    type: z.ZodLiteral<"cura.compliance.subject-rights.requested">;
    payload: z.ZodObject<{
        requestId: z.ZodString;
        subject: z.ZodObject<{
            subjectRef: z.ZodString;
            subjectType: z.ZodEnum<{
                party: "party";
                user: "user";
                "patient-reference": "patient-reference";
                "tenant-account": "tenant-account";
                "external-subject": "external-subject";
            }>;
        }, z.core.$strict>;
        requestKind: z.ZodEnum<{
            export: "export";
            erasure: "erasure";
        }>;
        dueBy: z.ZodString;
        requestedScopes: z.ZodArray<z.ZodString>;
    }, z.core.$strict>;
}, z.core.$strict>;
export declare const SubjectRightsStepCompletedEventSchema: z.ZodObject<{
    eventId: z.ZodString;
    tenantId: z.ZodString;
    actorId: z.ZodOptional<z.ZodString>;
    occurredAt: z.ZodString;
    correlationId: z.ZodOptional<z.ZodString>;
    traceId: z.ZodOptional<z.ZodString>;
    causationId: z.ZodOptional<z.ZodString>;
    type: z.ZodLiteral<"cura.compliance.subject-rights.step-completed">;
    payload: z.ZodObject<{
        requestId: z.ZodString;
        serviceName: z.ZodString;
        step: z.ZodEnum<{
            "capability-checked": "capability-checked";
            "reference-map-exported": "reference-map-exported";
            "export-fragment-ready": "export-fragment-ready";
            "erasure-applied": "erasure-applied";
            "legal-hold-checked": "legal-hold-checked";
        }>;
        itemCounts: z.ZodObject<{
            subjectReferences: z.ZodOptional<z.ZodNumber>;
            records: z.ZodOptional<z.ZodNumber>;
            attachments: z.ZodOptional<z.ZodNumber>;
            bundles: z.ZodOptional<z.ZodNumber>;
        }, z.core.$strict>;
    }, z.core.$strict>;
}, z.core.$strict>;
export declare const SubjectRightsExportReadyEventSchema: z.ZodObject<{
    eventId: z.ZodString;
    tenantId: z.ZodString;
    actorId: z.ZodOptional<z.ZodString>;
    occurredAt: z.ZodString;
    correlationId: z.ZodOptional<z.ZodString>;
    traceId: z.ZodOptional<z.ZodString>;
    causationId: z.ZodOptional<z.ZodString>;
    type: z.ZodLiteral<"cura.compliance.subject-rights.export-ready">;
    payload: z.ZodObject<{
        requestId: z.ZodString;
        bundleRef: z.ZodObject<{
            storageRef: z.ZodString;
            checksumSha256: z.ZodString;
        }, z.core.$strict>;
        itemCounts: z.ZodObject<{
            subjectReferences: z.ZodOptional<z.ZodNumber>;
            records: z.ZodOptional<z.ZodNumber>;
            attachments: z.ZodOptional<z.ZodNumber>;
            bundles: z.ZodOptional<z.ZodNumber>;
        }, z.core.$strict>;
        expiresAt: z.ZodString;
    }, z.core.$strict>;
}, z.core.$strict>;
export declare const SubjectRightsErasureRequestedEventSchema: z.ZodObject<{
    eventId: z.ZodString;
    tenantId: z.ZodString;
    actorId: z.ZodOptional<z.ZodString>;
    occurredAt: z.ZodString;
    correlationId: z.ZodOptional<z.ZodString>;
    traceId: z.ZodOptional<z.ZodString>;
    causationId: z.ZodOptional<z.ZodString>;
    type: z.ZodLiteral<"cura.compliance.subject-rights.erasure-requested">;
    payload: z.ZodObject<{
        requestId: z.ZodString;
        subject: z.ZodObject<{
            subjectRef: z.ZodString;
            subjectType: z.ZodEnum<{
                party: "party";
                user: "user";
                "patient-reference": "patient-reference";
                "tenant-account": "tenant-account";
                "external-subject": "external-subject";
            }>;
        }, z.core.$strict>;
        serviceName: z.ZodString;
        action: z.ZodEnum<{
            delete: "delete";
            anonymize: "anonymize";
        }>;
        dueBy: z.ZodString;
        legalHold: z.ZodBoolean;
    }, z.core.$strict>;
}, z.core.$strict>;
export declare const SubjectRightsErasureCompletedEventSchema: z.ZodObject<{
    eventId: z.ZodString;
    tenantId: z.ZodString;
    actorId: z.ZodOptional<z.ZodString>;
    occurredAt: z.ZodString;
    correlationId: z.ZodOptional<z.ZodString>;
    traceId: z.ZodOptional<z.ZodString>;
    causationId: z.ZodOptional<z.ZodString>;
    type: z.ZodLiteral<"cura.compliance.subject-rights.erasure-completed">;
    payload: z.ZodObject<{
        requestId: z.ZodString;
        serviceName: z.ZodString;
        action: z.ZodEnum<{
            delete: "delete";
            anonymize: "anonymize";
        }>;
        itemCounts: z.ZodObject<{
            subjectReferences: z.ZodOptional<z.ZodNumber>;
            records: z.ZodOptional<z.ZodNumber>;
            attachments: z.ZodOptional<z.ZodNumber>;
            bundles: z.ZodOptional<z.ZodNumber>;
        }, z.core.$strict>;
    }, z.core.$strict>;
}, z.core.$strict>;
export declare const SubjectRightsErasureFailedEventSchema: z.ZodObject<{
    eventId: z.ZodString;
    tenantId: z.ZodString;
    actorId: z.ZodOptional<z.ZodString>;
    occurredAt: z.ZodString;
    correlationId: z.ZodOptional<z.ZodString>;
    traceId: z.ZodOptional<z.ZodString>;
    causationId: z.ZodOptional<z.ZodString>;
    type: z.ZodLiteral<"cura.compliance.subject-rights.erasure-failed">;
    payload: z.ZodObject<{
        requestId: z.ZodString;
        serviceName: z.ZodString;
        reason: z.ZodEnum<{
            "legal-hold": "legal-hold";
            retryable: "retryable";
            permanent: "permanent";
        }>;
        retryable: z.ZodBoolean;
        failureRef: z.ZodOptional<z.ZodString>;
    }, z.core.$strict>;
}, z.core.$strict>;
export declare const SubjectRightsCapabilityManifestSchema: z.ZodObject<{
    manifestVersion: z.ZodLiteral<1>;
    serviceName: z.ZodString;
    subjectReferenceKinds: z.ZodArray<z.ZodEnum<{
        party: "party";
        user: "user";
        "patient-reference": "patient-reference";
        "tenant-account": "tenant-account";
        "external-subject": "external-subject";
    }>>;
    export: z.ZodObject<{
        supported: z.ZodBoolean;
        producesBundle: z.ZodBoolean;
        itemCountKinds: z.ZodArray<z.ZodEnum<{
            subjectReferences: "subjectReferences";
            records: "records";
            attachments: "attachments";
            bundles: "bundles";
        }>>;
    }, z.core.$strict>;
    erasure: z.ZodObject<{
        supported: z.ZodBoolean;
        actions: z.ZodArray<z.ZodEnum<{
            delete: "delete";
            anonymize: "anonymize";
        }>>;
        legalHoldResponse: z.ZodEnum<{
            block: "block";
            defer: "defer";
            "report-only": "report-only";
        }>;
    }, z.core.$strict>;
}, z.core.$strict>;
export declare const subjectRightsEventDefinitions: readonly [{
    readonly name: "cura.compliance.subject-rights.requested";
    readonly schemaVersion: 1;
    readonly schema: z.ZodObject<{
        eventId: z.ZodString;
        tenantId: z.ZodString;
        actorId: z.ZodOptional<z.ZodString>;
        occurredAt: z.ZodString;
        correlationId: z.ZodOptional<z.ZodString>;
        traceId: z.ZodOptional<z.ZodString>;
        causationId: z.ZodOptional<z.ZodString>;
        type: z.ZodLiteral<"cura.compliance.subject-rights.requested">;
        payload: z.ZodObject<{
            requestId: z.ZodString;
            subject: z.ZodObject<{
                subjectRef: z.ZodString;
                subjectType: z.ZodEnum<{
                    party: "party";
                    user: "user";
                    "patient-reference": "patient-reference";
                    "tenant-account": "tenant-account";
                    "external-subject": "external-subject";
                }>;
            }, z.core.$strict>;
            requestKind: z.ZodEnum<{
                export: "export";
                erasure: "erasure";
            }>;
            dueBy: z.ZodString;
            requestedScopes: z.ZodArray<z.ZodString>;
        }, z.core.$strict>;
    }, z.core.$strict>;
}, {
    readonly name: "cura.compliance.subject-rights.step-completed";
    readonly schemaVersion: 1;
    readonly schema: z.ZodObject<{
        eventId: z.ZodString;
        tenantId: z.ZodString;
        actorId: z.ZodOptional<z.ZodString>;
        occurredAt: z.ZodString;
        correlationId: z.ZodOptional<z.ZodString>;
        traceId: z.ZodOptional<z.ZodString>;
        causationId: z.ZodOptional<z.ZodString>;
        type: z.ZodLiteral<"cura.compliance.subject-rights.step-completed">;
        payload: z.ZodObject<{
            requestId: z.ZodString;
            serviceName: z.ZodString;
            step: z.ZodEnum<{
                "capability-checked": "capability-checked";
                "reference-map-exported": "reference-map-exported";
                "export-fragment-ready": "export-fragment-ready";
                "erasure-applied": "erasure-applied";
                "legal-hold-checked": "legal-hold-checked";
            }>;
            itemCounts: z.ZodObject<{
                subjectReferences: z.ZodOptional<z.ZodNumber>;
                records: z.ZodOptional<z.ZodNumber>;
                attachments: z.ZodOptional<z.ZodNumber>;
                bundles: z.ZodOptional<z.ZodNumber>;
            }, z.core.$strict>;
        }, z.core.$strict>;
    }, z.core.$strict>;
}, {
    readonly name: "cura.compliance.subject-rights.export-ready";
    readonly schemaVersion: 1;
    readonly schema: z.ZodObject<{
        eventId: z.ZodString;
        tenantId: z.ZodString;
        actorId: z.ZodOptional<z.ZodString>;
        occurredAt: z.ZodString;
        correlationId: z.ZodOptional<z.ZodString>;
        traceId: z.ZodOptional<z.ZodString>;
        causationId: z.ZodOptional<z.ZodString>;
        type: z.ZodLiteral<"cura.compliance.subject-rights.export-ready">;
        payload: z.ZodObject<{
            requestId: z.ZodString;
            bundleRef: z.ZodObject<{
                storageRef: z.ZodString;
                checksumSha256: z.ZodString;
            }, z.core.$strict>;
            itemCounts: z.ZodObject<{
                subjectReferences: z.ZodOptional<z.ZodNumber>;
                records: z.ZodOptional<z.ZodNumber>;
                attachments: z.ZodOptional<z.ZodNumber>;
                bundles: z.ZodOptional<z.ZodNumber>;
            }, z.core.$strict>;
            expiresAt: z.ZodString;
        }, z.core.$strict>;
    }, z.core.$strict>;
}, {
    readonly name: "cura.compliance.subject-rights.erasure-requested";
    readonly schemaVersion: 1;
    readonly schema: z.ZodObject<{
        eventId: z.ZodString;
        tenantId: z.ZodString;
        actorId: z.ZodOptional<z.ZodString>;
        occurredAt: z.ZodString;
        correlationId: z.ZodOptional<z.ZodString>;
        traceId: z.ZodOptional<z.ZodString>;
        causationId: z.ZodOptional<z.ZodString>;
        type: z.ZodLiteral<"cura.compliance.subject-rights.erasure-requested">;
        payload: z.ZodObject<{
            requestId: z.ZodString;
            subject: z.ZodObject<{
                subjectRef: z.ZodString;
                subjectType: z.ZodEnum<{
                    party: "party";
                    user: "user";
                    "patient-reference": "patient-reference";
                    "tenant-account": "tenant-account";
                    "external-subject": "external-subject";
                }>;
            }, z.core.$strict>;
            serviceName: z.ZodString;
            action: z.ZodEnum<{
                delete: "delete";
                anonymize: "anonymize";
            }>;
            dueBy: z.ZodString;
            legalHold: z.ZodBoolean;
        }, z.core.$strict>;
    }, z.core.$strict>;
}, {
    readonly name: "cura.compliance.subject-rights.erasure-completed";
    readonly schemaVersion: 1;
    readonly schema: z.ZodObject<{
        eventId: z.ZodString;
        tenantId: z.ZodString;
        actorId: z.ZodOptional<z.ZodString>;
        occurredAt: z.ZodString;
        correlationId: z.ZodOptional<z.ZodString>;
        traceId: z.ZodOptional<z.ZodString>;
        causationId: z.ZodOptional<z.ZodString>;
        type: z.ZodLiteral<"cura.compliance.subject-rights.erasure-completed">;
        payload: z.ZodObject<{
            requestId: z.ZodString;
            serviceName: z.ZodString;
            action: z.ZodEnum<{
                delete: "delete";
                anonymize: "anonymize";
            }>;
            itemCounts: z.ZodObject<{
                subjectReferences: z.ZodOptional<z.ZodNumber>;
                records: z.ZodOptional<z.ZodNumber>;
                attachments: z.ZodOptional<z.ZodNumber>;
                bundles: z.ZodOptional<z.ZodNumber>;
            }, z.core.$strict>;
        }, z.core.$strict>;
    }, z.core.$strict>;
}, {
    readonly name: "cura.compliance.subject-rights.erasure-failed";
    readonly schemaVersion: 1;
    readonly schema: z.ZodObject<{
        eventId: z.ZodString;
        tenantId: z.ZodString;
        actorId: z.ZodOptional<z.ZodString>;
        occurredAt: z.ZodString;
        correlationId: z.ZodOptional<z.ZodString>;
        traceId: z.ZodOptional<z.ZodString>;
        causationId: z.ZodOptional<z.ZodString>;
        type: z.ZodLiteral<"cura.compliance.subject-rights.erasure-failed">;
        payload: z.ZodObject<{
            requestId: z.ZodString;
            serviceName: z.ZodString;
            reason: z.ZodEnum<{
                "legal-hold": "legal-hold";
                retryable: "retryable";
                permanent: "permanent";
            }>;
            retryable: z.ZodBoolean;
            failureRef: z.ZodOptional<z.ZodString>;
        }, z.core.$strict>;
    }, z.core.$strict>;
}];
export type SubjectReference = z.infer<typeof subjectReferenceSchema>;
export type SubjectRightsRequestedEvent = z.infer<typeof SubjectRightsRequestedEventSchema>;
export type SubjectRightsStepCompletedEvent = z.infer<typeof SubjectRightsStepCompletedEventSchema>;
export type SubjectRightsExportReadyEvent = z.infer<typeof SubjectRightsExportReadyEventSchema>;
export type SubjectRightsErasureRequestedEvent = z.infer<typeof SubjectRightsErasureRequestedEventSchema>;
export type SubjectRightsErasureCompletedEvent = z.infer<typeof SubjectRightsErasureCompletedEventSchema>;
export type SubjectRightsErasureFailedEvent = z.infer<typeof SubjectRightsErasureFailedEventSchema>;
export type SubjectRightsCapabilityManifest = z.infer<typeof SubjectRightsCapabilityManifestSchema>;
export {};
//# sourceMappingURL=subject-rights.d.ts.map