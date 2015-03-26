package web

import (
	"encoding/base64"
	"net/http"
	"strings"
)

func BasicAuth(r *http.Request, name, password string) (ok bool, err error) {
	auth := r.Header.Get("Authorization")
	if auth == "" || auth[:len("Basic")] != "Basic" {
		return
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.Replace(auth, "Basic ", "", -1))
	if err != nil {
		return
	}
	parts := strings.Split(string(decoded), ":")
	if len(parts) != 2 {
		return
	}
	if parts[0] != name || parts[1] != password {
		return
	}
	ok = true
	return
}
