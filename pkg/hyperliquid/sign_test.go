package hyperliquid

import (
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
)

// A throwaway, well-known test key (the Hyperliquid SDK's own fixture key). Never
// used for anything but exercising the signing path.
const testPrivHex = "0123456789012345678901234567890123456789012345678901234567890123"

// limitOrder mirrors the wire shape of one order leg, used only to give the
// signer a representative action to hash. Field order and msgpack tags must match
// what the real client sends.
type testLimit struct {
	Tif string `msgpack:"tif" json:"tif"`
}
type testOrderType struct {
	Limit testLimit `msgpack:"limit" json:"limit"`
}
type testOrder struct {
	Asset      uint32        `msgpack:"a" json:"a"`
	IsBuy      bool          `msgpack:"b" json:"b"`
	Price      string        `msgpack:"p" json:"p"`
	Size       string        `msgpack:"s" json:"s"`
	ReduceOnly bool          `msgpack:"r" json:"r"`
	Type       testOrderType `msgpack:"t" json:"t"`
}
type testAction struct {
	Type     string      `msgpack:"type" json:"type"`
	Orders   []testOrder `msgpack:"orders" json:"orders"`
	Grouping string      `msgpack:"grouping" json:"grouping"`
}

func sampleAction() testAction {
	return testAction{
		Type: "order",
		Orders: []testOrder{{
			Asset: 1, IsBuy: true, Price: "100", Size: "100", ReduceOnly: false,
			Type: testOrderType{Limit: testLimit{Tif: "Gtc"}},
		}},
		Grouping: "na",
	}
}

// The signature must recover to the signing key's own address: this proves the
// secp256k1 sign + v-normalization are correct over the EIP-712 digest we build.
func TestSignL1ActionRecoversSigner(t *testing.T) {
	key, err := crypto.HexToECDSA(testPrivHex)
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	want := crypto.PubkeyToAddress(key.PublicKey)

	const nonce = 1700000000000
	sig, err := signL1Action(key, sampleAction(), nil, nonce, true)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	// Rebuild the exact digest that was signed, then ecrecover the public key.
	hash, err := actionHash(sampleAction(), nil, nonce)
	if err != nil {
		t.Fatalf("action hash: %v", err)
	}
	digest, err := eip712Digest(hash, true)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}

	r, err := hexutil.Decode(sig.R)
	if err != nil {
		t.Fatalf("decode r: %v", err)
	}
	s, err := hexutil.Decode(sig.S)
	if err != nil {
		t.Fatalf("decode s: %v", err)
	}
	recovered := append(append(r, s...), sig.V-27) // back to {0,1} for SigToPub
	pub, err := crypto.SigToPub(digest, recovered)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := crypto.PubkeyToAddress(*pub); got != want {
		t.Fatalf("recovered %s, want %s", got, want)
	}
}

// actionHash must be stable across calls for the same input — the nonce and
// action fully determine it, so a replay derives the identical connectionId.
func TestActionHashDeterministic(t *testing.T) {
	const nonce = 42
	a, err := actionHash(sampleAction(), nil, nonce)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	b, err := actionHash(sampleAction(), nil, nonce)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if a != b {
		t.Fatalf("action hash not deterministic: %x vs %x", a, b)
	}
}
