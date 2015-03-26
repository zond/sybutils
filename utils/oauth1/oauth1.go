package oauth1

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"math/rand"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Pair struct {
	Key   string
	Value string
}

type Params []*Pair

func (p Params) Len() int { return len(p) }

func (p Params) Less(i, j int) bool {
	if p[i].Key == p[j].Key {
		return p[i].Value < p[j].Value
	}
	return p[i].Key < p[j].Key
}

func (p Params) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (p *Params) Add(pair *Pair) {
	a := *p
	n := len(a)

	if n+1 > cap(a) {
		s := make([]*Pair, n, 2*n+1)
		copy(s, a)
		a = s
	}
	a = a[0 : n+1]
	a[n] = pair
	*p = a

}

// isEncodable returns true if a given character should be percent-encoded
// according to RFC 3986.
func isEncodable(c byte) bool {
	// return false if c is an unreserved character (see RFC 3986 section 2.3)
	switch {
	case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'):
		return false
	case c >= '0' && c <= '9':
		return false
	case c == '-' || c == '.' || c == '_' || c == '~':
		return false
	}
	return true
}

// Encode percent-encodes a string as defined in RFC 3986.
func Encode(s string) string {
	var enc string
	for _, c := range []byte(s) {
		if isEncodable(c) {
			enc += "%"
			enc += string("0123456789ABCDEF"[c>>4])
			enc += string("0123456789ABCDEF"[c&15])
		} else {
			enc += string(c)
		}
	}
	return enc
}

var headerReg = regexp.MustCompile("^(?i)oauth\\s*(.*=.*(,.*=.*)*)$")

func GetParams(r *http.Request) (result Params) {
	params := Params{}
	for key, values := range r.URL.Query() {
		if key != "oauth_signature" {
			for _, value := range values {
				params.Add(&Pair{Key: key, Value: value})
			}
		}
	}
	authHeader := ""
	for _, value := range r.Header["Authorization"] {
		authHeader += value
	}
	if authHeader != "" {
		if match := headerReg.FindStringSubmatch(authHeader); match != nil {
			for _, part := range strings.Split(match[1], ",") {
				kv := strings.Split(part, "=")
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				value = strings.Replace(value, `"`, "", -1) // No fnuts for you
				params.Add(&Pair{Key: key, Value: value})
			}
		}
	}
	result = params
	return
}

func GenerateSignature(r *http.Request, secret string) (result string, err error) {
	params := GetParams(r)
	sort.Sort(params)
	sigBaseCol := []string{}
	for _, param := range params {
		if param.Key != "realm" && param.Key != "oauth_signature" {
			sigBaseCol = append(sigBaseCol, Encode(param.Key)+"="+Encode(param.Value))
		}
	}
	if r.URL.Path == "" {
		r.URL.Path = "/"
	}
	hostAndPort := strings.Split(r.URL.Host, ":")
	if len(hostAndPort) > 1 {
		if r.URL.Scheme == "http" && hostAndPort[1] == "80" {
			hostAndPort = []string{hostAndPort[0]}
		} else if r.URL.Scheme == "https" && hostAndPort[1] == "443" {
			hostAndPort = []string{hostAndPort[0]}
		}
	}
	sigBaseStr := r.Method + "&" + Encode(r.URL.Scheme+"://"+strings.Join(hostAndPort, ":")+r.URL.Path) + "&" + Encode(strings.Join(sigBaseCol, "&"))
	key := Encode(secret) + "&"
	h := hmac.New(sha1.New, []byte(key))
	h.Write([]byte(sigBaseStr))
	result = base64.StdEncoding.EncodeToString(h.Sum(nil))
	return
}

func GenerateNonce() string {
	return strconv.FormatInt(rand.New(rand.NewSource(time.Now().UnixNano())).Int63(), 10)
}

func GenerateTimestamp() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}
