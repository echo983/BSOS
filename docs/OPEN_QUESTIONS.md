# Open questions

Surfaced by an explicit design-closure review. Resolve into `docs/DESIGN.md`
when decided.

## Gaps in the storage engine's own spec (block implementation)

- **`total_size` vs. actual streamed byte count.** §3.3 has the server
  compute the target extent from `PutHeader.total_size` before any data
  arrives, then stream bytes in. Nothing says what happens if the stream
  ends short, or carries more bytes than declared. Silently zero-padding
  a shortfall would violate `fid = hash(content)` (§3.1) even for an
  honest client that hit a transient error — this isn't a content-
  validation question (§3.6), it's stream-length accounting needed for
  the extent model to be correct at all. Leaning towards: any mismatch
  aborts the write with an error, no silent pad/truncate.
- **No guaranteed scheduling/locking fairness for Trim under sustained
  write load.** §3.8 says Trim must run continuously because it's the
  only relief valve for index-stream exhaustion (no DELETE/GC), but
  nothing says how it's guaranteed to actually get to run if writes keep
  contending for the same per-disk lock. In NBSS this was a soft
  degradation (more fragmentation); in BSOS it's a hard failure mode
  (index fills up, no recourse) if it stays unresolved.
- **No Health/liveness RPC decided either way.** Purely operational
  infrastructure (something for a load balancer/monitor to poll), not
  discussed yet, not deliberately excluded like Probe/Pan/Delete were.

## Companion systems this design assumes but hasn't started

- **No manifest format for client-side chunking of large objects (§3.5).**
  The storage engine correctly doesn't need to know about this, but if
  more than one internal tool needs to read/write the same chunked
  objects, they need a shared, designed format — doesn't exist yet, not
  even "is the manifest itself a plain object and how is its fid derived."
- **No shared client SDK yet.** Several decisions (§3.4's recommended
  jump-target algorithm, §3.9's readback-based retry safety, §3.5's
  chunking) are only clean if there's exactly one shared client library
  everyone uses, per the trust-model discussion. That library doesn't
  exist — no language, no repo, nothing built.
- **No higher-layer pool-lifecycle/retention system.** §3.7 explicitly
  delegates space reclaim ("拣选迁移") to a layer this system doesn't
  own. Without it, a BSOS pool that fills up has no path forward except
  wholesale retirement/reinit. This is a deliberate scope boundary, not
  an oversight, but it means BSOS alone is not a complete, usable
  solution — worth remembering before treating this design as "done."
