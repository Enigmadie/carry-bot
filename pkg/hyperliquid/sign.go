package hyperliquid

// Hyperliquid does not authenticate with an API-key HMAC like Bybit. Every L1
// action (an order, a cancel) is signed as an EIP-712 typed message with the
// account's (or an agent wallet's) secp256k1 key, the same signature scheme used
// for on-chain Ethereum transactions. The exchange recovers the signer address
// from the signature and checks it owns (or is an approved agent of) the account.
//
// The signed digest is built in three steps, mirroring the reference Python/Rust
// SDKs exactly — any byte that differs changes the hash and the order is rejected:
//
//  1. actionHash: msgpack-encode the action, append the nonce (8 bytes, big
//     endian) and a vault flag, then keccak256. This is the "connectionId".
//  2. Wrap it in a phantom "Agent" struct whose `source` is "a" on mainnet, "b"
//     on testnet, and EIP-712-hash that under a fixed Exchange domain.
//  3. secp256k1-sign the resulting digest; split into r, s, v (27/28).
//
// The msgpack must match Python's `msgpack.packb`: ints in their most compact
// form (UseCompactInts) and struct fields in declaration order (so we encode
// structs, never maps, whose key order is undefined).

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/vmihailenco/msgpack/v5"
)

// Signature is the r/s/v an L1 action carries in its JSON body. r and s are
// 0x-prefixed, 32-byte left-padded hex; v is 27 or 28.
type Signature struct {
	R string `json:"r"`
	S string `json:"s"`
	V byte   `json:"v"`
}

// actionHash is the connectionId: keccak256(msgpack(action) ++ nonce ++ vault).
// vault is nil for a normal account; otherwise a 0x01 flag plus the 20-byte
// vault address selects sub-account trading.
func actionHash(action any, vault *common.Address, nonce uint64) ([32]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true) // match Python msgpack: small ints as fixint, not padded
	if err := enc.Encode(action); err != nil {
		return [32]byte{}, fmt.Errorf("msgpack action: %w", err)
	}

	var n [8]byte
	binary.BigEndian.PutUint64(n[:], nonce)
	buf.Write(n[:])

	if vault == nil {
		buf.WriteByte(0x00)
	} else {
		buf.WriteByte(0x01)
		buf.Write(vault.Bytes())
	}

	return crypto.Keccak256Hash(buf.Bytes()), nil
}

// eip712Digest hashes the phantom Agent{source, connectionId} under the fixed
// Exchange domain (chainId 1337, zero verifying contract) and returns the final
// 32-byte digest that gets signed. source distinguishes mainnet ("a") from
// testnet ("b") so a testnet signature can't be replayed on mainnet.
func eip712Digest(connectionID [32]byte, mainnet bool) ([]byte, error) {
	source := "b"
	if mainnet {
		source = "a"
	}
	typedData := apitypes.TypedData{
		Domain: apitypes.TypedDataDomain{
			Name:              "Exchange",
			Version:           "1",
			ChainId:           math.NewHexOrDecimal256(1337),
			VerifyingContract: "0x0000000000000000000000000000000000000000",
		},
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Agent": {
				{Name: "source", Type: "string"},
				{Name: "connectionId", Type: "bytes32"},
			},
		},
		PrimaryType: "Agent",
		Message: apitypes.TypedDataMessage{
			"source":       source,
			"connectionId": connectionID[:],
		},
	}

	domainSep, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return nil, fmt.Errorf("hash domain: %w", err)
	}
	msgHash, err := typedData.HashStruct("Agent", typedData.Message)
	if err != nil {
		return nil, fmt.Errorf("hash agent: %w", err)
	}

	// EIP-712: keccak256("\x19\x01" ++ domainSeparator ++ hashStruct(message)).
	raw := append([]byte{0x19, 0x01}, append(domainSep, msgHash...)...)
	return crypto.Keccak256(raw), nil
}

// signL1Action signs an action for the /exchange endpoint with key, returning the
// r/s/v the request body carries. nonce is a millisecond timestamp that also
// guards against replay; mainnet selects the phantom-agent source.
func signL1Action(key *ecdsa.PrivateKey, action any, vault *common.Address, nonce uint64, mainnet bool) (Signature, error) {
	hash, err := actionHash(action, vault, nonce)
	if err != nil {
		return Signature{}, err
	}
	digest, err := eip712Digest(hash, mainnet)
	if err != nil {
		return Signature{}, err
	}

	sig, err := crypto.Sign(digest, key)
	if err != nil {
		return Signature{}, fmt.Errorf("sign: %w", err)
	}
	// crypto.Sign yields [R || S || V] with V in {0,1}; Hyperliquid wants {27,28}.
	return Signature{
		R: hexutil.Encode(sig[0:32]),
		S: hexutil.Encode(sig[32:64]),
		V: sig[64] + 27,
	}, nil
}
