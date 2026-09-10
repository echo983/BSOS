# Implementation plan

Status: construction planning, following the `design-v0` milestone in
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

1. **Bootstrap.** Port `internal/blk` (layout, addressing, index
   encoding, CLI backup/restore/scrub) essentially unchanged. Get
   `bsos blk init`/`find`/`info` working. Cheaply validates the
   "on-disk format is unchanged from NBSS" claim (§5) before anything
   hard is attempted.
2. **Single-disk Put/Get, correct but not yet concurrency-optimized.**
   New wire format (client-declared fid, streaming header-first Put) end
   to end, but the write path still holds its disk lock for the whole
   operation (NBSS's current approach) rather than the two-phase model.
   Validates the protocol and streaming plumbing in isolation.
3. **Concurrency model.** Replace the placeholder locking from
   milestone 2 with the real two-phase commit: pool-wide fid gate,
   per-disk extent reservation, bounded-concurrency dispatcher. Highest
   risk milestone — §1's test suite is written alongside or before this,
   not after.
4. **Multi-disk pooling.** `multidisk.go`'s best-fit routing and zram
   tiering, with the pool-wide gate now genuinely exercised across more
   than one disk.
5. **Trim, fragmentation, Bonnie.** Port `trim.go`/`packed_table.go`/
   `fragmentation.go` (near-unchanged), wire up the `Bonnie` RPC.
6. **One-hop alias (`alias_for`).** Deliberately its own milestone after
   the core write path and Trim are both stable — it touches both the
   pool-wide gate (two fids, not one) and Trim's jump-target exclusion,
   so it's the wrong thing to bolt on simultaneously with either.
7. **Health and operational polish.** Timeouts, config surface,
   diagnostics.

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

## 4. Client SDK: minimal reference client only, in scope; full SDK is separate

This phase builds a **minimal reference client** — just enough to
compute fid, speak the streaming Put protocol correctly, and issue
Get/Head/Bonnie/Health calls — sufficient to test the daemon end to end
and serve as living example code. It is explicitly **not** a production
client SDK (no manifest/chunking, no jump-retry helper, no
retry-safety-readback wrapper). Those belong to a separate, later
project once the daemon itself is proven; bundling them into this phase
would slow down the part `docs/DESIGN.md` is actually about.

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
