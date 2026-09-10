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
| `internal/zram/` | NBSS `internal/zram/` | Initially copied with import-path rewrites. BSOS now adds snapshot validation/recovery orchestration, corrects the hot_add read interface, checks device identity, and uses BSOS default paths; see the milestone 4 validation report. |

Also ported, as logic (not files) rather than copied verbatim, per
`docs/NBSS_REUSE_PLAN.md`:
- `internal/daemon/multidisk.go` (best-fit disk selection) and `internal/daemon/chd.go` (`computeCHD` and its helpers) follow NBSS's `internal/daemon` algorithms, with BSOS boundary checks for invalid sizes/probabilities and failed devices, adapted to this repo's own `interval`/`DeviceState` types.
- `internal/daemon/packed_table.go` (packed table record encoding and decoding) follows NBSS's packed table format.
- `internal/daemon/fragmentation.go` (fragmentation metrics and power-of-two size calculations) follows NBSS's fragmentation threshold algorithms.
- `internal/daemon/trim.go` (mandatory background trim, container packing, packed table creation, anchor append, tombstone writing, and index compaction) adapted to BSOS's RAM index and two-phase write model.

Copied (or logic-ported) as of the point each was brought in during
construction. Future changes to these packages should be made directly
in this repository; they are not kept in sync with NBSS afterward.
