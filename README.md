# BSOS — Bare Space Object Storage

BSOS is a lite object storage design for a trusted, bounded-size internal
environment. It takes NBSS's raw-block-device, content-addressed storage
engine as its architectural reference, then strips out everything that
engine pays for only because it has to work for large-scale, mutually
untrusted participants.

## Status

Design is complete as of the `design-v0` tag — see `docs/DESIGN.md` for
the full spec, `docs/OPEN_QUESTIONS.md` for what's still undecided
(currently nothing), and `docs/NBSS_REUSE_PLAN.md` for what carries over
from NBSS's codebase versus what needs real rework. Construction planning
is underway: see `docs/IMPLEMENTATION_PLAN.md` for the test strategy,
milestones, repo bootstrap approach, and config schema. No code has
landed yet.

## Relationship to NBSS

NBSS is a private reference implementation studied for its physical layout
and scheduling ideas (disk layout, slot addressing, collision handling,
in-memory index rebuild, elevator-style I/O queues, background
defragmentation). BSOS reuses that physical layer essentially unchanged.
No NBSS source is vendored into this repository; BSOS is a fresh
implementation that happens to target a compatible on-disk byte format.

## Trust model

Usage is bounded to a small, mutually trusted group of internal
collaborators (on the order of Dunbar's number), and the system never
stores third-party ("customer") data. Compliance, multi-tenancy, auth, and
adversarial-client defenses are out of scope by design, not by oversight —
see `docs/DESIGN.md` for what that trades away and why it's acceptable at
this scope.
