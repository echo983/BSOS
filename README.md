# BSOS — Bare Space Object Storage

BSOS is a lite object storage design for a trusted, bounded-size internal
environment. It takes NBSS's raw-block-device, content-addressed storage
engine as its architectural reference, then strips out everything that
engine pays for only because it has to work for large-scale, mutually
untrusted participants.

## Status

Design is complete at `design-v0`. Implementation milestones 1–7 are
complete; milestone 8 (Go client library) is next.
Continuous mandatory Trim defragmentation, container packing, crash-safety
failpoints, Bonnie RPC, one-hop jump alias protocol, and operational polish (TOML config,
Health & diagnostics, and graceful shutdown) have passed all unit and race tests.
Routing/recovery tests, real two-disk verification, real zram host
acceptance gate, and systemd daemon deployment on Debian 13 VPS have passed. See [milestone 4 validation](docs/MILESTONE_4_VALIDATION_2026-09-10.md),
[milestone 5 validation](docs/MILESTONE_5_VALIDATION_2026-09-10.md), [milestone 6 validation](docs/MILESTONE_6_VALIDATION_2026-09-10.md),
and [milestone 7 validation](docs/MILESTONE_7_VALIDATION_2026-09-10.md).
The tree also includes the foundation repairs described in
[the foundation validation report](docs/FOUNDATION_VALIDATION_2026-09-10.md).
See `docs/IMPLEMENTATION_PLAN.md` for milestone scope and remaining work,
`docs/DESIGN.md` for server semantics, and `docs/CLIENT_SPEC.md` for the
client-facing contract.

## Relationship to NBSS

NBSS is a private reference implementation studied for its physical layout
and scheduling ideas (disk layout, slot addressing, collision handling,
in-memory index rebuild, elevator-style I/O queues, background
defragmentation). BSOS reuses that physical layer essentially unchanged.
Per `docs/IMPLEMENTATION_PLAN.md`, the pieces of NBSS's codebase that
carry over unchanged (disk layout, addressing, index encoding, CLI
backup/restore/scrub tooling) are being copied in with provenance recorded
in `THIRD_PARTY_ORIGIN.md`; everything else is a fresh implementation
targeting a compatible on-disk byte format.

## Trust model

Usage is bounded to a small, mutually trusted group of internal
collaborators (on the order of Dunbar's number), and the system never
stores third-party ("customer") data. Compliance, multi-tenancy, auth, and
adversarial-client defenses are out of scope by design, not by oversight —
see `docs/DESIGN.md` for what that trades away and why it's acceptable at
this scope.
