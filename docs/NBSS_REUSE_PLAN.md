# Reuse plan against NBSS's codebase

Status: first pass, at file/package granularity, not yet started.
Verdicts assume the concurrency-model change in `docs/DESIGN.md` §3.3
("Concurrency model: streaming writes are not atomic...").

Legend: **keep** (unchanged or near-unchanged) · **modify** (real edits,
not a rewrite) · **rewrite** (existing file isn't a useful starting
point) · **drop** (not needed) · **regenerate** (generated code, not
hand-written).

## `internal/blk` — disk layout, addressing, index encoding, CLI tooling

**Keep, essentially all of it.** This package *is* the on-disk format,
and the format doesn't change (§5): `layout.go`, `addr.go`, `index.go`,
`index_v2.go`, `header.go`, `init.go`, `info.go`, `device.go`, `find.go`,
`index_backup.go`, `index_restore.go`, `index_compact.go`,
`index_packed_scrub.go` (and their tests). None of it knows or cares who
computed an fid or whether the write that produced an entry was streamed.

## `internal/pan`, `internal/zram/*`, `internal/debug/index_stats.go`

**Keep.** Unrelated to fid ownership or transport.

## `internal/daemon` — the server

- `multidisk.go`: **keep**, minus `probeItems` and `deleteAll` (Probe
  and Delete are both gone, §4/§3.7). `selectDiskForWrite`/`pickZram`/
  `pickNonZram`/`readAny`/`findExistingSize` carry over per §3.11.
- `fragmentation.go` (+test): **keep**. The jump-target exclusion logic
  already does exactly what §3.8 needs.
- `trim.go`, `packed_table.go` (+tests), `jump_info.go`: **keep**, minus
  the `trim_enabled` on/off switch — §3.8 makes Trim non-optional, so the
  config knob to disable it shouldn't exist rather than just default to
  true.
- `zram.go`, `readonly.go`: **keep**.
- `config.go` (+test): **modify**. Remove HTTP-related fields, remove
  `write_memory_budget_bytes`/`write_memory_wait_ms` and friends (§3.3
  deletes that subsystem), remove `trim_enabled`.
- `state.go`: **modify, not rewrite**. `loadIndexIntoDB`,
  `findLatestRecord`, `computeCHD`, packed-table loading are pure
  replay/lookup logic and don't need to change shape. `handleWrite`,
  `writeDataAt`, and the `intervals` occupancy structure need the
  pending/confirmed two-phase model from `docs/DESIGN.md` §3.3.
- `directio.go`: **modify**. The aligned-chunk write loop itself is
  reusable; its input changes from a fully-materialized `[]byte` to a
  stream plus the internal re-alignment buffer described in §3.3.
- `queue.go`: **modify**. Same data structure (a slot-index-ordered
  min-heap), but the queued unit drops from "one write operation" to
  "one aligned chunk flush," per §3.3.
- `grpc.go`: **Put handler: rewrite. Get/Head/Bonnie/Health: modify.**
  The current `Put` handler accumulates every chunk into one `[]byte`
  before writing once — that's precisely the pattern being eliminated,
  not something to adapt in place. The other four handlers are close to
  reusable once `fid` replaces the server-derived value and HTTP-only
  glue is stripped out.
- `server.go`: **mostly delete, rewrite what's left.** Every HTTP
  handler goes (§4: gRPC only). Multi-disk wiring and startup
  orchestration that isn't HTTP-specific needs reorganizing around
  what's left.
- `write_budget.go` (+2 tests): **drop.** The subsystem it implements no
  longer exists (§3.3).
- `memdiag.go`: **modify**, reduced scope. Its heap-delta-triggered
  profiling was partly motivated by diagnosing the write-buffer
  subsystem; general RSS/heap visibility may still be worth keeping, but
  the original trigger doesn't apply. Low priority either way.
- `nbsspb/*.pb.go`: **regenerate** from `proto/bsos.proto`, not migrated
  by hand.

## `internal/fileop` — CLI single-file read/write/delete

- `write.go`, `read.go`, `index_helpers.go`: **keep, essentially as
  is.** `write.go`'s jump-retry algorithm (rehash content + jumpCode
  byte) is already exactly the algorithm §3.4/§5 recommend BSOS clients
  use — if BSOS wants an equivalent local CLI, this is close to a direct
  port, not new design.
- `delete.go`: **drop.** No DELETE.

## `cmd/nbss`, `cmd/nbssd`

**Modify.** Flag/entrypoint wiring follows whatever `config.go` ends up
with; no complex logic of its own.

## `proto/nbss.proto`

**Drop.** Replaced by `proto/bsos.proto`, already drafted in this repo.

## Rough shape of the effort

The largest, most mechanically-safe share of NBSS's code — everything
that defines or reads the on-disk format, multi-disk routing, Trim,
fragmentation analysis, zram handling — carries over with little to no
change. The work concentrates entirely in the request-handling core of
`internal/daemon` (`state.go`'s write path, `directio.go`, `queue.go`,
`grpc.go`'s `Put`, and the parts of `server.go` that survive gRPC-only),
because that's where the streaming/concurrency model in `docs/DESIGN.md`
§3.3 actually bites. Everything HTTP-shaped and the write-memory-budget
subsystem are deleted outright, not ported.
