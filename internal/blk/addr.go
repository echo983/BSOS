package blk

import "errors"

var ErrOutOfRange = errors.New("extent exceeds grid boundary")

func AddrForFID(diskBytes uint64, fid uint64, sizeBytes uint64) (addr uint64, slotIndex uint64, slotsNeeded uint64, err error) {
	slots := (diskBytes - GridStart) / SlotSize
	if slots == 0 {
		return 0, 0, 0, ErrOutOfRange
	}
	slotsNeeded = (sizeBytes + SlotSize - 1) / SlotSize
	if slotsNeeded == 0 || slotsNeeded > slots {
		return 0, 0, 0, ErrOutOfRange
	}
	slotIndex = fid % (slots - slotsNeeded + 1)
	if slotIndex+slotsNeeded > slots {
		return 0, 0, 0, ErrOutOfRange
	}
	addr = GridStart + slotIndex*SlotSize
	return addr, slotIndex, slotsNeeded, nil
}
