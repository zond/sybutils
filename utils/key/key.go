package key

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/soundtrackyourbrand/utils/json"

	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"
)

type genealogyAssertion struct {
	kind          string
	parentKinds   []string
	stringIDKinds []string
}

func (self *genealogyAssertion) StringIDKinds(allowed ...string) *genealogyAssertion {
	for _, kind := range allowed {
		self.stringIDKinds = append(self.stringIDKinds, kind)
	}
	return self
}

func (self *genealogyAssertion) ParentKinds(allowed ...string) *genealogyAssertion {
	for _, kind := range allowed {
		self.parentKinds = append(self.parentKinds, kind)
	}
	return self
}

var genealogyAssertions = map[string]*genealogyAssertion{}

func AssertedKinds() (result []string) {
	for kind, _ := range genealogyAssertions {
		result = append(result, kind)
	}
	return
}

func AssertGenealogy(kind string) (result *genealogyAssertion) {
	result, found := genealogyAssertions[kind]
	if !found {
		result = &genealogyAssertion{
			kind: kind,
		}
		genealogyAssertions[kind] = result
	}
	return
}

func split(s string, delim byte) (before, after string) {
	buf := &bytes.Buffer{}
	i := 0
	for i = 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			buf.WriteByte(s[i])
			i++
			buf.WriteByte(s[i])
		case delim:
			before, after = buf.String(), s[i+1:]
			return
		default:
			buf.WriteByte(s[i])
		}
	}
	before = buf.String()
	return
}

func escape(s string) string {
	buf := &bytes.Buffer{}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ',':
			buf.WriteString("\\,")
		case '/':
			buf.WriteString("\\/")
		case '\\':
			buf.WriteString("\\\\")
		default:
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

func unescape(s string) string {
	buf := &bytes.Buffer{}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i+1 < len(s) {
				i++
				switch s[i] {
				case '\\':
					buf.WriteByte('\\')
				case ',':
					buf.WriteByte(',')
				case '/':
					buf.WriteByte('/')
				default:
					buf.WriteByte('\\')
				}
			} else {
				buf.WriteByte('\\')
			}
		default:
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

func For(i interface{}, StringId string, IntId int64, parent Key) (result Key, err error) {
	val := reflect.ValueOf(i)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return New(val.Type().Name(), StringId, IntId, parent)
}

type Key string

func NewWithoutValidate(kind string, stringID string, intID int64, parent Key) (result Key) {
	result = Key(fmt.Sprintf("%v,%v,%v/%v", escape(kind), escape(stringID), escape(strconv.FormatInt(intID, 36)), string(parent)))
	return
}

func New(kind string, stringID string, intID int64, parent Key) (result Key, err error) {
	result = NewWithoutValidate(kind, stringID, intID, parent)
	err = result.validate()
	return
}

func (self Key) validate() (err error) {
	kind, stringID, _, parent := self.Split()
	if assertion, found := genealogyAssertions[kind]; found {
		if len(assertion.parentKinds) > 0 {
			parentKind := parent.Kind()
			ok := false
			for _, okKind := range assertion.parentKinds {
				if okKind == parentKind {
					ok = true
					break
				}
			}
			if !ok {
				err = utils.Errorf("%v doesn't have a valid parent", self)
				return
			}
		}
		if len(assertion.stringIDKinds) > 0 {
			var stringIDKey = Key("")
			if stringIDKey, err = Decode(stringID); err != nil {
				return
			}
			stringIDKind := stringIDKey.Kind()
			ok := false
			for _, okKind := range assertion.stringIDKinds {
				if okKind == stringIDKind {
					ok = true
					break
				}
			}
			if !ok {
				err = utils.Errorf("%v doesn't have a valid StringID", self)
				return
			}
		}
	}
	return
}

func (self Key) String() string {
	if len(self) == 0 {
		return ""
	}
	buf := &bytes.Buffer{}
	self.describe(buf)
	return string(buf.Bytes())
}

func (self Key) Split() (kind string, stringID string, intID int64, parent Key) {
	rest, after := split(string(self), '/')
	kind, rest = split(rest, ',')
	stringID, rest = split(rest, ',')
	intID, err := strconv.ParseInt(unescape(rest), 36, 64)
	if err != nil {
		kind = ""
		stringID = ""
		intID = 0
		parent = Key("")
		return
	}
	kind, stringID, parent = unescape(kind), unescape(stringID), Key(after)
	return
}

func (self Key) describe(w io.Writer) {
	if len(self) == 0 {
		return
	}
	kind, stringID, intID, parent := self.Split()
	parent.describe(w)
	fmt.Fprintf(w, "/%s,", kind)
	if stringID != "" {
		fmt.Fprintf(w, "%s", stringID)
	}
	if intID != 0 {
		fmt.Fprintf(w, "%d", intID)
	}
	return
}

func (self Key) MarshalJSON() (b []byte, err error) {
	return json.Marshal(self.Encode())
}

func (self *Key) UnmarshalJSON(b []byte) (err error) {
	encoded := ""
	if err = json.Unmarshal(b, &encoded); err != nil {
		return
	}
	var unmarshalled Key
	if unmarshalled, err = Decode(encoded); err == nil {
		*self = unmarshalled
	}
	return
}

func (self Key) Kind() (result string) {
	result, _, _, _ = self.Split()
	return
}

func (self Key) StringID() (result string) {
	_, result, _, _ = self.Split()
	return
}

func (self Key) IntID() (result int64) {
	_, _, result, _ = self.Split()
	return
}

func (self Key) Parent() (result Key) {
	_, _, _, result = self.Split()
	return
}

func (self Key) Encode() (result string) {
	return strings.Replace(base64.URLEncoding.EncodeToString([]byte(self)), "=", ".", -1)
}

func DecodeKind(kind string, s string) (result Key, err error) {
	if result, err = Decode(s); err != nil {
		return
	}
	if result.Kind() != kind {
		err = jsoncontext.NewError(417, fmt.Sprintf("Expected a key of kind %#v, but got %#v", kind, result.Kind()), "", nil)
		return
	}
	return
}

func Decode(s string) (result Key, err error) {
	if s == "" {
		return
	}
	b := []byte{}
	b, err = base64.URLEncoding.DecodeString(strings.Replace(s, ".", "=", -1))
	if err != nil {
		err = httpcontext.NewError(400, err.Error(), err.Error(), err)
		return
	}
	result = Key(string(b))
	err = result.validate()
	return
}

func (s Key) Equal(k Key) bool {
	return s == k
}
