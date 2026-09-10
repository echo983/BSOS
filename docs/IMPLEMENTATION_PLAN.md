# Implementation plan

Status: milestones 1–4 implemented; milestone 5 (Trim, fragmentation, Bonnie) next, following the `design-v0` milestone in
`docs/DESIGN.md`. This document is about *how* to build what's already
designed, not further design decisions — revise `docs/DESIGN.md` first
if something here turns out to need an actual design change.

## 1. Concurrency test strategy

The three real bugs found during design review (missing fid-level
reservation, its wrong per-disk scope, missing timeouts — see
`docs/DESIGN.md` §3.3's history) were all found by reasoning, not by
running code. That means this logic cannot be trusted to unit tests and
incidental goroutine scheduling luck — it needs tests that deliberately
target the exact race windows already identified:

- Concurrent writes to the same fid on one disk: exactly one succeeds,
  the other gets conflict.
- Concurrent writes to the same fid where multi-disk best-fit could
  plausibly route them to *different* disks: the pool-wide gate (§3.3
  step 0) must still allow only one to succeed.
- A plain write to fid A racing a write with `alias_for: A`: no silent
  data loss — the loser must be rejected, never invisibly overwritten.
- A stalled write (no forward progress): the stall timeout must release
  its reservation, and a subsequent write for the same fid must then
  succeed.
- Declared `total_size` vs. actual byte count mismatch (both directions):
  abort, reservation released, no index entry written, slot state
  indistinguishable from never having been attempted.
- Client-side stream cancellation: same cleanup guarantee.
- A GET against a fid with only a pending (unconfirmed) reservation:
  must return not-found, never a torn/partial read.

These race windows are narrow; relying on natural goroutine scheduling
to hit them in CI is not reliable. The write path should expose
test-only hooks that let a test deterministically pause execution
between "reservation acquired" and "commit," so these scenarios can be
triggered on demand rather than hoped for. Every concurrent path runs
under `-race` in CI, unconditionally.

NBSS's existing tests for logic that doesn't change (`trim_test.go`,
`packed_table_test.go`, `fragmentation_test.go`, `index_v2_test.go`,
`index_compact_test.go`, and similar) port over directly as regression
protection — see `docs/NBSS_REUSE_PLAN.md`.

## 2. Milestones

Ordered specifically to separate "does streaming Put/Get land data
correctly" from "do concurrent writes stay correct and fast" — conflating
those two concerns is exactly where the design-review bugs lived, so the
plan avoids debugging both at once.

1. **Bootstrap — done.** Ported `internal/blk` and `internal/pan`
   unchanged (provenance in `THIRD_PARTY_ORIGIN.md`); `bsos blk
   init`/`find`/`info`/`index-backup`/`index-restore`/`index-compact`/
   `packed-scrub` all build, vet clean, and the copied test suite
   (`internal/blk`'s `*_test.go`) passes. Verified for real, not just
   compiled: `bsos blk init` against `/dev/sdb` (the dev/test USB drive)
   produced a valid NBSS v2 header and index stream, `bsos blk find`
   located it via `/dev/disk/by-id` and wrote a correct `pan.json`, and
   `bsos blk info` read it back consistently — the "on-disk format is
   unchanged from NBSS" claim (§5) is now demonstrated, not just
   asserted. `go.mod` uses module `bsos`; the current module and active toolchain
   require Go 1.25.0.
2. **Single-disk Put/Get — done.** `internal/daemon` (`state.go`,
   `server.go`, `config.go`) and `cmd/bsosd` implement the new wire
   format end to end: client-declared fid, `PutHeader`-first streaming
   Put that writes chunks straight to disk as they arrive (no
   full-payload buffering — that part of §3.3's benefit already holds),
   `alias_for` registering a one-hop jump pair, Get/Head/Health. The
   write path still holds its disk lock for the whole operation (NBSS's
   current approach, no two-phase reservation yet) — that's the one
   piece deferred to milestone 3, deliberately.

   Verified against `/dev/sdb` with a real gRPC client, not just unit
   tests: Put + idempotent-conflict retry (`AlreadyExists`), Head, Get
   (byte-exact match), an `alias_for` write resolved correctly through
   the jump indicator on both Head and the size arithmetic, a
   missing-fid `NotFound`, and — the one that matters most — a
   declared-100/sent-9-bytes stream aborting with a clear error *and*
   confirmed to leave no index entry behind (`Head` on that fid still
   returns `NotFound` afterward), exactly matching §3.3's "no silent
   pad, slot free again as if never attempted" requirement.

   Updated 2026-09-10: the original single-message Get simplification
   failed with a default gRPC client on a 5 MiB object. Get now reads and
   sends at most 1 MiB per response and reads only the requested range;
   default-client large-object and range tests pass.

3. **Concurrency model — done.** Replaced milestone 2's whole-operation
   locking with docs/DESIGN.md §3.3's real two-phase commit:
   `gate.go` (pool-wide fid reservation, step 0), `interval.go`
   (per-disk extent reservation, step 1), `index_state.go` (boot replay
   rebuilding confirmed references and extents), `server.go`'s `Put`
   orchestrating reserve → unlocked stream → commit/abort (steps 2-4),
   and a stall timeout (`stallingChunkReader`) plus the pool gate's own
   fan-out timeout. `state.go`'s `DeviceState` also gained the §3.12
   per-disk bounded-concurrency dispatcher (a semaphore around each I/O
   operation; simplified to one acquisition per whole transfer rather
   than per aligned chunk flush — a known simplification, not a
   correctness gap, noted for revisiting).

   `internal/daemon/concurrency_test.go`, run under `-race`: same-fid
   concurrent writes (exactly one wins), the exact bug found in design
   review — a plain write to fid A racing a write with `alias_for: A` —
   confirmed to always produce exactly one winner with the loser's data
   never silently lost, an aborted write confirmed to leave its fid
   completely free for an immediate retry, and the stall timeout firing
   within its configured window against a client that never responds.
   Updated 2026-09-10: cross-disk plain/alias races in both winning
   orders, 64 concurrent RPC writes with readback, cancellation/stall
   cleanup, index rollback fault injection, and restart replay now have
   tests. Successful Put releases the pending gate entries; a per-disk
   in-memory index publishes a complete alias pair after index sync.
   See `FOUNDATION_VALIDATION_2026-09-10.md` for evidence and limits.

   Historical milestone-3 hardware verification (before the foundation
   repairs above): against `/dev/sdb`, a
   fresh Put/Get round-trip still works end to end through the new
   orchestration, not just the unit tests in isolation.
4. **Multi-disk pooling — done.**
   Best-fit routing, CHD reservation accounting, zram snapshot discovery,
   cold-start empty-device allocation, restore validation, current-path
   reconciliation and atomic pan.json updates are implemented. Missing or
   broken zram tiers are logged and reflected in Health. Routing/recovery
   tests and a real two-USB-disk Put/Get/alias/reopen run pass.
   Real zram recovery gate (`TestRealZramRecovery`) passed in 4.20s on the
   authorized Debian 13 VPS, and end-to-end service cold-start snapshot
   discovery, decompress, restore, and range readback verified via `vps-smoke`.
   See `MILESTONE_4_VALIDATION_2026-09-10.md` for reproducible commands and evidence.

5. **Trim, fragmentation, Bonnie.** Port `trim.go`/`packed_table.go`/
   `fragmentation.go` (near-unchanged), wire up the `Bonnie` RPC.
6. **One-hop alias (`alias_for`).** Deliberately its own milestone after
   the core write path and Trim are both stable — it touches both the
   pool-wide gate (two fids, not one) and Trim's jump-target exclusion,
   so it's the wrong thing to bolt on simultaneously with either.
7. **Health and operational polish.** Timeouts, config surface,
   diagnostics.
8. **Go client library.** Implements `docs/CLIENT_SPEC.md` in full
   (§4 below): fid computation, streaming Put/Get/Head/Bonnie/Health,
   jump-retry helper, readback-based retry-safety helper.
9. **Basic CLI**, built on milestone 8's library.

## 3. Repo bootstrap approach

Files verdicted **keep** in `docs/NBSS_REUSE_PLAN.md`
(`internal/blk`, `internal/pan`, `internal/zram/*`,
`internal/debug/index_stats.go`) are copied directly from NBSS into this
repo, with provenance recorded (a `THIRD_PARTY_ORIGIN.md`-style note,
following the precedent in echo983/notFinderLinux). Everything verdicted
**modify**, **rewrite**, or **new** in that plan (essentially all of
`internal/daemon`, `cmd/bsosd`) is written fresh, informed by reading the
corresponding NBSS code rather than copy-pasted from it. `go.mod` and
`buf.gen.yaml`-equivalent proto tooling are set up fresh, mirroring
NBSS's own setup.

## 4. Client deliverables: behavior spec, Go library, basic CLI

Three deliverables, scoped by `docs/CLIENT_SPEC.md`:

- **`docs/CLIENT_SPEC.md`** (written): the client-facing contract — fid
  computation, the write protocol's binding requirements, collision/jump
  handling, retry-safety, and how to interpret `Bonnie`. Authoritative on
  what a client does; `docs/DESIGN.md` stays authoritative on why.
- **Go library**: implements everything `CLIENT_SPEC.md` specifies — fid
  computation, streaming `Put`/`Get`/`Head`/`Bonnie`/`Health`, the
  jump-retry helper, the readback-based retry-safety helper. Permanently
  excludes chunking/manifest support — not deferred, out of scope: BSOS
  is infrastructure and has no concept of files, directories, or
  multi-object logical structure, and neither does its client library.
  That's an application-layer project's job, not this one's.
- **Basic CLI** (`bsos put|get|head|bonnie|health`): a thin wrapper over
  the Go library. `put` handles collisions by running the jump-retry
  algorithm automatically (matching `nbss file write`'s existing
  behavior) and prints the resulting fid; none of the commands know what
  a directory or a multi-part file is — input/output is raw bytes in,
  raw bytes out, one fid per object.

Sequenced after the core daemon milestones below (5-7) — the library and
CLI need a working `Put`/`Get` to test against — except writing
`CLIENT_SPEC.md` itself, which has no such dependency and can happen any
time.

## 5. `bsosd.toml` draft

```toml
pan_path = "pan.json"
grpc_listen = ":9090"
grpc_chunk = "1MB"

data_dir = "/dev/shm/bsos-pan"
max_put = "256MB"
small_file_pow2 = 6

zram_snapshot_dir = "~/.bsos_zram_snapshots"
zram_readonly = false
zram_readonly_dir = "~/.bsos_zram_readonly"
zram_flush_seconds = 3600

reservation_stall_timeout_ms = 30000    # §3.3 per-write stall timeout
pool_gate_fanout_timeout_ms = 2000      # §3.3 pool-wide gate fan-out timeout
write_dispatch_concurrency = 64         # §3.12 per-disk bounded-concurrency cap

chd_target_p = 0.2
chd_debug = false
pan_refresh_seconds = 15

trim_interval_seconds = 900
trim_min_file_count = 100
trim_threshold_ratio = 0.2
trim_min_threshold_bytes = "1MB"
trim_max_threshold_bytes = "16MB"
trim_temp_dir = "/dev/shm"

direct_io = true
debug = false
```

Removed relative to NBSS's `nbssd.toml`: `listen` (no HTTP, §4),
`mem_diag*`, `write_memory_budget_bytes`/`write_memory_wait_ms` (§3.3
deletes that subsystem), `trim_enabled` (no longer optional, §3.8).
Added: the three concurrency-model parameters above, corresponding to
mechanisms `docs/DESIGN.md` §3.3/§3.12 introduce that have no NBSS
equivalent.

## Dev/test hardware

Two USB block devices are available for this work (identified via
`lsblk`, `TRAN=usb`, `RM=1`, distinct from this machine's own `vda`/`vdb`
system disks): `/dev/sda` (57.3G, SanDisk) and `/dev/sdb` (7.5G,
PHILIPS). Both were wiped clean (filesystem signatures and partition
tables zeroed) on 2026-09-10 and are free to reuse/re-wipe for BSOS
development and testing.
