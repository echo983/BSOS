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

Copied as-is at the point BSOS construction began. Future changes to
these packages should be made directly in this repository; they are not
kept in sync with NBSS after the initial copy.
