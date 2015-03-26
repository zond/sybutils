package encoding

import (
	"encoding/base64"
	"strings"
)

/*
Non-padded base64 url encoding
*/

func URLDecode(src string) (string, error) {
	if m := len(src) % 4; m != 0 {
		src += strings.Repeat("=", 4-m)
	}

	decoded, err := base64.URLEncoding.DecodeString(src)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func URLEncode(src string) string {
	encoded := base64.URLEncoding.EncodeToString([]byte(src))
	return strings.TrimRight(encoded, "=")
}
