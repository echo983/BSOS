# Open questions

## Resolved (2026-09-10)

- `total_size` vs. actual streamed byte count → abort on any mismatch,
  never silently pad/truncate. See `docs/DESIGN.md` §3.3.
- Trim scheduling/locking fairness under sustained write load → accepted
  as an occasional operational risk, not engineered around. See §3.8.
- Health RPC → added, same shape as NBSS's. See §4.

## Companion systems: found, not missing — but need porting

A design-closure review had flagged "no manifest format" and "no shared
client SDK" as unstarted. Both already exist, in two real client
repositories built on NBSS:

- https://github.com/echo983/notFinder (Windows, WinFSP)
- https://github.com/echo983/notFinderLinux (Linux, FUSE3)

Both vendor a shared `nbss-core` Rust crate that is exactly the "one
shared client library" the trust model discussion assumed. It already
implements:

- A typed, versioned chunk manifest (`nbss-core/src/manifest.rs`, magic
  `S0L0UN0^`): header with `chunk_pow2` + `chunk_count` + `tail_size`,
  entries of `(fid, is_manifest)` — manifests can nest, not just a flat
  chunk list.
- A "PVLog" layer (`pvlog.rs`/`pvlog_writer.rs`/`pvlog_replay.rs`) that
  builds versioning, rollback, and zero-copy cross-directory moves as a
  client-side log over NBSS's immutable blobs — i.e., this product
  already treats NBSS as a dumb content-addressed store and puts all the
  smart layering on the client side, the same split BSOS is built around.

**But it's built against NBSS's current wire contract** (server-derived
fid, HTTP+gRPC, `DeleteRequest`/`delete_object`) — none of it speaks
BSOS's contract (client-declared fid, gRPC-only, header-first streaming
Put, no Delete). Porting `nbss-core` (and whatever in both daemons calls
it directly) to BSOS is real work across two production codebases, not a
side effect of finishing this design.

## New conflict found while reading them

`nbss-core`'s only caller of NBSS's per-object `delete_object` is PVLog's
own compaction routine (`fs_state.rs`, the function that consolidates
many small log segments into one frame): it deletes the now-superseded
segment and head objects after compaction succeeds. This is the client's
own log-GC, analogous to what Trim does for BSOS's data grid — not
deletion of user file content.

Checked: the user-facing per-file/per-reality delete path
(`notfinder-daemon`'s `soft_delete`/`archive`/`purge_reality`,
`/api/v1/realities/{id}/files/delete`) never calls `delete_object` on a
content fid anywhere in the tree — it's a namespace/PVLog-level
unreference, already compatible with BSOS's no-DELETE model (§3.7) as
designed.

So the actual conflict is narrow: **PVLog's own log-compaction currently
relies on deleting superseded internal segments**, and BSOS has no
DELETE at all. Under BSOS, those superseded PVLog segments would become
permanent garbage until whatever eventually retires/migrates the whole
pool (§3.7's still-unbuilt higher layer) — a real behavior change from
what this code does today, not yet decided whether that's acceptable or
whether it changes anything about §3.7.

## Still open

- Whether PVLog's own segment garbage accumulating forever (previous
  section) is acceptable, or whether it changes the §3.7 "no DELETE at
  all, ever" decision for this one narrow internal case.
- Porting plan/scope for `nbss-core` (both repos) from NBSS's current
  contract to BSOS's.
- Higher-layer pool-lifecycle/retention system (§3.7) — still doesn't
  exist anywhere, still needed regardless of the above.
