# @curaos/events

Typed event schema contracts for CuraOS event-led architecture.

## Current surface

- `SubjectRightsRequestedEventSchema`
- `SubjectRightsStepCompletedEventSchema`
- `SubjectRightsExportReadyEventSchema`
- `SubjectRightsErasureRequestedEventSchema`
- `SubjectRightsErasureCompletedEventSchema`
- `SubjectRightsErasureFailedEventSchema`
- `SubjectRightsCapabilityManifestSchema`
- `subjectRightsEventDefinitions`

Subject-rights events follow ADR-0102 topic naming:

```text
cura.compliance.subject-rights.<event-type>
```

Schema version is exported as metadata on each definition, not as a `.v1` topic
suffix and not as a payload field. Payloads carry subject references, service
names, bundle references, legal-hold outcomes, and item counts. PHI values stay
in overlay services and are rejected by the Zod schemas.

## Commands

```bash
bunx turbo run test --filter=@curaos/events
bunx turbo run typecheck --filter=@curaos/events
bunx turbo run lint --filter=@curaos/events
bunx turbo run build --filter=@curaos/events
```
