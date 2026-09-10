package blk

const (
	HeaderBytes   = 4096
	GridStart     = 0x10000000
	IndexStart    = HeaderBytes
	IndexBytes    = GridStart - HeaderBytes
	SlotSize      = 4096
	FormatVersion = 2
)
