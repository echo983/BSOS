# BSOS — Bare Space Object Storage

BSOS is a lite object storage design for a trusted, bounded-size internal
environment. It takes NBSS's raw-block-device, content-addressed storage
engine as its architectural reference, then strips out everything that
engine pays for only because it has to work for large-scale, mutually
untrusted participants.

## Status

Design is complete at `design-v0`. Implementation milestones 1–3 are
committed; milestone 4 (multi-disk pooling and zram startup) is in progress.
The working tree also includes the foundation repairs described in
[the foundation validation report](docs/FOUNDATION_VALIDATION_2026-09-10.md).
See `docs/IMPLEMENTATION_PLAN.md` for milestone scope and remaining work,
`docs/DESIGN.md` for server semantics, and `docs/CLIENT_SPEC.md` for the
client-facing contract. Passing the default tests does not yet constitute
completion of the zram or physical-device acceptance gates.

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
