package hyperliquid

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// The updateLeverage action must msgpack-encode byte-for-byte like the reference
// SDK's dict (insertion-ordered: type, asset, isCross, leverage). The signature
// hashes these bytes, so a wrong field order or tag silently changes the hash and
// Hyperliquid rejects the action. This pins the canonical encoding by hand.
func TestLeverageActionEncoding(t *testing.T) {
	action := leverageAction{
		Type:     "updateLeverage",
		Asset:    1,
		IsCross:  true,
		Leverage: 3,
	}

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true) // same setting actionHash uses
	if err := enc.Encode(action); err != nil {
		t.Fatalf("encode: %v", err)
	}

	want := []byte{
		0x84,                     // fixmap, 4 entries
		0xa4, 't', 'y', 'p', 'e', // "type"
		0xae, 'u', 'p', 'd', 'a', 't', 'e', 'L', 'e', 'v', 'e', 'r', 'a', 'g', 'e', // "updateLeverage"
		0xa5, 'a', 's', 's', 'e', 't', // "asset"
		0x01,                                    // 1
		0xa7, 'i', 's', 'C', 'r', 'o', 's', 's', // "isCross"
		0xc3,                                         // true
		0xa8, 'l', 'e', 'v', 'e', 'r', 'a', 'g', 'e', // "leverage"
		0x03, // 3
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoding mismatch:\n got %x\nwant %x", buf.Bytes(), want)
	}
}
