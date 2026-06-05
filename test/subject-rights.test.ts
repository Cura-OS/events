import { describe, expect, test } from 'bun:test';
import {
  SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION,
  SubjectRightsCapabilityManifestSchema,
  SubjectRightsErasureCompletedEventSchema,
  SubjectRightsErasureFailedEventSchema,
  SubjectRightsErasureRequestedEventSchema,
  SubjectRightsExportReadyEventSchema,
  SubjectRightsRequestedEventSchema,
  SubjectRightsStepCompletedEventSchema,
  subjectRightsEventDefinitions,
} from '../src';

const tenantId = '550e8400-e29b-41d4-a716-446655440000';
const eventId = '550e8400-e29b-41d4-a716-446655440001';
const requestId = '550e8400-e29b-41d4-a716-446655440002';
const actorId = '550e8400-e29b-41d4-a716-446655440003';
const occurredAt = '2026-06-05T00:00:00.000Z';
const dueBy = '2026-07-05T00:00:00.000Z';

const subject = {
  subjectRef: 'party:550e8400-e29b-41d4-a716-446655440004',
  subjectType: 'party',
} as const;

const envelope = {
  eventId,
  tenantId,
  occurredAt,
  correlationId: 'subject-rights-request-1',
  traceId: 'trace-subject-rights-1',
};

describe('subject-rights event definitions', () => {
  test('export the six ADR-0102 topic names without version suffixes', () => {
    expect(SUBJECT_RIGHTS_EVENT_SCHEMA_VERSION).toBe(1);
    expect(subjectRightsEventDefinitions.map((definition) => definition.name)).toEqual([
      'cura.compliance.subject-rights.requested',
      'cura.compliance.subject-rights.step-completed',
      'cura.compliance.subject-rights.export-ready',
      'cura.compliance.subject-rights.erasure-requested',
      'cura.compliance.subject-rights.erasure-completed',
      'cura.compliance.subject-rights.erasure-failed',
    ]);
    for (const definition of subjectRightsEventDefinitions) {
      expect(definition.schemaVersion).toBe(1);
      expect(definition.name.endsWith('.v1')).toBe(false);
    }
  });

  test('validates request lifecycle event payloads with subject references and counts only', () => {
    const requested = SubjectRightsRequestedEventSchema.parse({
      ...envelope,
      type: 'cura.compliance.subject-rights.requested',
      actorId,
      payload: {
        requestId,
        subject,
        requestKind: 'export',
        dueBy,
        requestedScopes: ['neutral-reference-map', 'healthstack-overlay'],
      },
    });
    expect(requested.payload.subject.subjectRef).toBe(subject.subjectRef);

    const stepCompleted = SubjectRightsStepCompletedEventSchema.parse({
      ...envelope,
      type: 'cura.compliance.subject-rights.step-completed',
      payload: {
        requestId,
        serviceName: 'party-core-service',
        step: 'reference-map-exported',
        itemCounts: {
          subjectReferences: 1,
          records: 12,
        },
      },
    });
    expect(stepCompleted.payload.itemCounts.records).toBe(12);

    const exportReady = SubjectRightsExportReadyEventSchema.parse({
      ...envelope,
      type: 'cura.compliance.subject-rights.export-ready',
      payload: {
        requestId,
        bundleRef: {
          storageRef: 'storage://tenant/subject-rights/export-1',
          checksumSha256: 'f'.repeat(64),
        },
        itemCounts: {
          subjectReferences: 1,
          records: 42,
        },
        expiresAt: dueBy,
      },
    });
    expect(Object.keys(exportReady.payload).toSorted()).toEqual([
      'bundleRef',
      'expiresAt',
      'itemCounts',
      'requestId',
    ]);
  });

  test('validates erasure lifecycle events with closed failure reasons', () => {
    expect(
      SubjectRightsErasureRequestedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-requested',
        payload: {
          requestId,
          subject,
          serviceName: 'party-core-service',
          action: 'anonymize',
          dueBy,
          legalHold: false,
        },
      }).payload.action,
    ).toBe('anonymize');

    expect(
      SubjectRightsErasureCompletedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-completed',
        payload: {
          requestId,
          serviceName: 'party-core-service',
          action: 'delete',
          itemCounts: {
            subjectReferences: 1,
            records: 5,
          },
        },
      }).payload.itemCounts.records,
    ).toBe(5);

    expect(
      SubjectRightsErasureFailedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-failed',
        payload: {
          requestId,
          serviceName: 'party-core-service',
          reason: 'legal-hold',
          retryable: false,
          failureRef: 'audit-event:550e8400-e29b-41d4-a716-446655440005',
        },
      }).payload.reason,
    ).toBe('legal-hold');

    expect(
      SubjectRightsErasureFailedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-failed',
        payload: {
          requestId,
          serviceName: 'party-core-service',
          reason: 'retryable',
          retryable: true,
        },
      }).payload.retryable,
    ).toBe(true);

    expect(() =>
      SubjectRightsErasureFailedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-failed',
        payload: {
          requestId,
          serviceName: 'party-core-service',
          reason: 'patient-name-present',
          retryable: false,
        },
      }),
    ).toThrow();

    expect(() =>
      SubjectRightsErasureFailedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-failed',
        payload: {
          requestId,
          serviceName: 'party-core-service',
          reason: 'permanent',
          retryable: true,
        },
      }),
    ).toThrow(/retryable/);

    expect(() =>
      SubjectRightsErasureFailedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-failed',
        payload: {
          requestId,
          serviceName: 'party-core-service',
          reason: 'retryable',
          retryable: false,
        },
      }),
    ).toThrow(/retryable/);
  });

  test('rejects PHI-like payload keys and values', () => {
    expect(() =>
      SubjectRightsExportReadyEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.export-ready',
        payload: {
          requestId,
          bundleRef: {
            storageRef: 'storage://tenant/subject-rights/export-1',
            checksumSha256: 'a'.repeat(64),
          },
          itemCounts: {
            subjectReferences: 1,
            records: 1,
          },
          expiresAt: dueBy,
          dateOfBirth: '1970-01-01',
        },
      }),
    ).toThrow();

    expect(() =>
      SubjectRightsErasureFailedEventSchema.parse({
        ...envelope,
        type: 'cura.compliance.subject-rights.erasure-failed',
        payload: {
          requestId,
          serviceName: 'party-core-service',
          reason: 'permanent',
          retryable: false,
          failureRef: 'incident-123-45-6789',
        },
      }),
    ).toThrow(/PHI/);
  });
});

describe('subject-rights capability manifest', () => {
  test('describes export/delete support and legal-hold response per service', () => {
    const manifest = SubjectRightsCapabilityManifestSchema.parse({
      manifestVersion: 1,
      serviceName: 'party-core-service',
      subjectReferenceKinds: ['party', 'user'],
      export: {
        supported: true,
        producesBundle: false,
        itemCountKinds: ['subjectReferences', 'records'],
      },
      erasure: {
        supported: true,
        actions: ['delete', 'anonymize'],
        legalHoldResponse: 'block',
      },
    });

    expect(manifest.erasure.legalHoldResponse).toBe('block');
    expect(manifest.export.itemCountKinds).toContain('records');
  });

  test('represents unsupported services without fake export counts or erasure actions', () => {
    const manifest = SubjectRightsCapabilityManifestSchema.parse({
      manifestVersion: 1,
      serviceName: 'notify-service',
      subjectReferenceKinds: ['user'],
      export: {
        supported: false,
        producesBundle: false,
        itemCountKinds: [],
      },
      erasure: {
        supported: false,
        actions: [],
        legalHoldResponse: 'report-only',
      },
    });

    expect(manifest.export.itemCountKinds).toEqual([]);
    expect(manifest.erasure.actions).toEqual([]);

    expect(() =>
      SubjectRightsCapabilityManifestSchema.parse({
        ...manifest,
        export: {
          supported: false,
          producesBundle: true,
          itemCountKinds: [],
        },
      }),
    ).toThrow(/bundles/);

    expect(() =>
      SubjectRightsCapabilityManifestSchema.parse({
        ...manifest,
        erasure: {
          supported: false,
          actions: ['delete'],
          legalHoldResponse: 'report-only',
        },
      }),
    ).toThrow(/actions/);
  });
});
