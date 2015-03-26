package utils

import (
	"bytes"
	"math/big"
	"math/rand"
	"testing"
)

const (
	hexChars  = "0123456789abcdef"
	flumChars = "abcdefghijklm98765432"
)

func assertSync(t *testing.T, chars string, i uint32) {
	bI := big.NewInt(int64(i))
	b := EncodeBigInt(chars, bI)
	j := DecodeBigInt(chars, b)
	if j.Cmp(bI) != 0 {
		t.Errorf("Decoding %#v in %#v should give %v but gave %v", b, chars, bI, j)
	}
}

func assertEnc(t *testing.T, chars string, i int64, res string) {
	bI := big.NewInt(i)
	b := EncodeBigInt(chars, bI)
	if b != res {
		t.Errorf("Encoding %v in #%v should give %#v but gave %#v", bI, chars, res, b)
	}
	j := DecodeBigInt(chars, b)
	if j.Cmp(bI) != 0 {
		t.Errorf("Decoding %#v in %#v should give %v but gave %v", b, chars, bI, j)
	}
}

func TestEncodeBigInt(t *testing.T) {
	assertEnc(t, hexChars, 15, "f")
	assertEnc(t, hexChars, 16, "10")
	for i := 0; i < 1000; i++ {
		assertSync(t, hexChars, rand.Uint32())
		assertSync(t, flumChars, rand.Uint32())
	}
}

func getRandBytes() []byte {
	b := make([]byte, rand.Uint32()%256)
	for index, _ := range b {
		b[index] = byte(rand.Uint32())
	}
	for len(b) > 0 && b[0] == 0 {
		b = b[1:]
	}
	if len(b) == 0 {
		return getRandBytes()
	}
	return b
}

func TestEncodeBytes(t *testing.T) {
	for i := 0; i < 1000; i++ {
		b := getRandBytes()
		enc := EncodeBytes(flumChars, b)
		dec := DecodeBytes(flumChars, enc)
		if bytes.Compare(dec, b) != 0 {
			t.Fatalf("%#v should be %#v", dec, b)
		}
	}
}
