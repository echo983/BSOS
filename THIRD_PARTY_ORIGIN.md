# Third-party origin

Some packages in this repository are copied from the private NBSS
reference implementation rather than written fresh, per
`docs/NBSS_REUSE_PLAN.md` and `docs/IMPLEMENTATION_PLAN.md` §3 (files
verdicted **keep**: they define or read the on-disk format, which is
unchanged between NBSS and BSOS, per `docs/DESIGN.md` §5).

| Path | Origin | Notes |
|---|---|---|
| `internal/blk/` | NBSS `internal/blk/` | Import paths rewritten from `nbss/internal/...` to `bsos/internal/...`; no other changes. |
| `internal/pan/` | NBSS `internal/pan/` | Same. |
| `internal/zram/` | NBSS `internal/zram/` | Same import-path rewrite; no other changes. `docs/DESIGN.md` §3.13's startup auto-load is new orchestration in `internal/daemon/server.go`, not a change to this package. |

Also ported, as logic (not files) rather than copied verbatim, per
`docs/NBSS_REUSE_PLAN.md`: `internal/daemon/multidisk.go` (best-fit
disk selection) and `internal/daemon/chd.go` (`computeCHD` and its
helpers) reproduce NBSS's `internal/daemon` algorithms exactly, adapted
to this repo's own `interval`/`DeviceState` types rather than copied
byte-for-byte, since NBSS's originals are entangled with types this
repo's write path doesn't share (docs/DESIGN.md §3.3's two-phase model).

Copied (or logic-ported) as of the point each was brought in during
construction. Future changes to these packages should be made directly
in this repository; they are not kept in sync with NBSS afterward.
