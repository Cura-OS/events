<div align="center">


# events

**Typed event bus client and schema registry for CuraOS event-led architecture. Frontend BFFs and server-side consumers use this to publish/subscribe to durable events via the messaging broker. Browser bundles get typed event schemas without publish/subscribe runtime.**

Part of the CuraOS (Care Oriented Stack) platform. Typed event bus client and schema registry for CuraOS event-led architecture. Frontend BFFs and server-side consumers use this to publish/subscribe to durable events via the messaging broker. Browser bundles get typed event schemas without publish/subscribe runtime. Domain: neutral.

[![Status](https://img.shields.io/badge/status-private--alpha-informational)](#status)
[![License: Proprietary](https://img.shields.io/badge/license-Proprietary-red)](./LICENSE)
[![Exposure: Closed](https://img.shields.io/badge/exposure-Closed-red)](#license)
[![Module: Package](https://img.shields.io/badge/module-Package-informational)](#how-it-works)

[Why](#why) · [Quick Start](#quick-start) · [Capabilities](#capabilities) · [How it Works](#how-it-works) · [Status](#status) · [Security](#security)

</div>

---

## Why

Typed event bus client and schema registry for CuraOS event-led architecture. Frontend BFFs and server-side consumers use this to publish/subscribe to durable events via the messaging broker. Browser bundles get typed event schemas without publish/subscribe runtime.

<!-- curaos:keep -->
<!-- /curaos:keep -->

---

## Quick Start

```bash
bun add @curaos/events
```

<!-- curaos:keep -->
<!-- /curaos:keep -->

---

## Capabilities

- createEventClient connects to broker (Kafka/NATS configurable).
- Typed schemas for ≥ 5 core domain events following .. naming (e.g. identity.user.created, tenancy.tenant.provisioned) per charter §7 versioned/stable topic naming; see ADR-0102 §"Topic / Subject Naming Convention" for canonical event-name contract.
- Zod validation on inbound events in subscription handler.
- useEventSubscription hook connects to SSE endpoint; handles reconnect.

<!-- curaos:keep -->
<!-- /curaos:keep -->

---

## How it Works

| Area | Detail |
|---|---|
| Package | `@curaos/events` |
| Source | `backend/packages/events` |
| Domain | `neutral` |
| Layer | `package` |
| Exposure | Closed |

- Source path: `backend/packages/events`
- Generated documentation owner: `tools/codegen/src/repo-docs-emit.ts`

<!-- curaos:keep -->
<!-- /curaos:keep -->

---

## API and Usage

See [docs.curaos.abualruz.com](https://docs.curaos.abualruz.com) (interim).

See [API reference](./src/index.ts) or generated TypeDoc.

<!-- curaos:keep -->
<!-- /curaos:keep -->

---

## Status

private alpha

- Docs generated from `tools/codegen/src/repo-docs-emit.ts`.
- Public documentation: [docs.curaos.abualruz.com](https://docs.curaos.abualruz.com).
- Changelog: [CHANGELOG.md](./CHANGELOG.md) when present.

---

## Security

See [SECURITY.md](./SECURITY.md) for vulnerability reporting policy.

---

## Maintainers

- CuraOS Team - [GitHub](https://github.com/Cura-OS)

---

## Contributing

Contributions are handled through the repository maintainers. Public contribution guidelines are emitted for open and source-available repositories.

By contributing, you agree that your contributions will be licensed under the same license as this project.

---

## License

LicenseRef-CuraOS-Proprietary - CuraOS (Care Oriented Stack). See [LICENSE](./LICENSE) for details.
