package rdb

import (
	"encoding/binary"
	"testing"
)

func TestCreateValueDumpUsesSourceRDBVersion(t *testing.T) {
	ld := &Loader{rdbVersion: 12}
	val := []byte("abc")
	dump := ld.createValueDump(1, val)

	versionOffset := 1 + len(val)
	got := binary.LittleEndian.Uint16([]byte(dump[versionOffset : versionOffset+2]))
	if got != 12 {
		t.Fatalf("unexpected dump version, got=%d want=%d", got, 12)
	}
}

func TestCreateValueDumpUsesFallbackVersionWhenUnset(t *testing.T) {
	ld := &Loader{}
	val := []byte("abc")
	dump := ld.createValueDump(1, val)

	versionOffset := 1 + len(val)
	got := binary.LittleEndian.Uint16([]byte(dump[versionOffset : versionOffset+2]))
	if got != 6 {
		t.Fatalf("unexpected fallback dump version, got=%d want=%d", got, 6)
	}
}
