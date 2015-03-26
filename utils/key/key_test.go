package key

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/soundtrackyourbrand/utils/json"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	AssertGenealogy("Location").ParentKinds("Account")
	AssertGenealogy("SpotifyAccount").StringIDKinds("Location")
}

func randomString() string {
	buf := make([]byte, 15)
	for index := range buf {
		buf[index] = byte(rand.Int())
	}
	return string(buf)
}

func randomKey(parents int) Key {
	if parents == 0 {
		key, err := New(randomString(), randomString(), rand.Int63(), "")
		if err != nil {
			panic(err)
		}
		return key
	}
	key, err := New(randomString(), randomString(), rand.Int63(), randomKey(parents-1))
	if err != nil {
		panic(err)
	}
	return key
}

func assertSplit(t *testing.T, source, before, after string) {
	if b, a := split(source, '/'); b != before || a != after {
		t.Fatalf("wrong split %#v => %#v, %#v, wanted %#v, %#v", source, b, a, before, after)
	}
}

func TestSplit(t *testing.T) {
	assertSplit(t, "apapapa/blblbl", "apapapa", "blblbl")
	assertSplit(t, "apa\\/papa/blblbl", "apa\\/papa", "blblbl")
	assertSplit(t, unescape("apa\\/papa"), "apa", "papa")
	assertSplit(t, escape("apa/gapa")+"/"+escape("gnu/hehu"), escape("apa/gapa"), escape("gnu/hehu"))
	assertSplit(t, escape(escape("apa/gapa")+"/"+escape("gnu/hehu"))+"/"+escape(escape("ja/nej")+"/"+escape("yes/no")),
		escape(escape("apa/gapa")+"/"+escape("gnu/hehu")),
		escape(escape("ja/nej")+"/"+escape("yes/no")))
}

func TestEscapeUnescape(t *testing.T) {
	for i := 0; i < 10000; i++ {
		s := randomString()
		e := s
		times := rand.Int() % 20
		for j := 0; j < times; j++ {
			e = escape(e)
		}
		d := e
		for j := 0; j < times; j++ {
			d = unescape(d)
		}
		if d != s {
			t.Fatalf("%#v != %#v", s, s)
		}
	}
}

func TestEncodeDecode(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(2)
		enc := k.Encode()
		k2, err := Decode(enc)
		if err != nil {
			t.Fatalf("Failed decoding %s: %v", enc, err)
		}
		if !reflect.DeepEqual(k, k2) {
			t.Fatalf("%#v != %#v", k, k2)
		}
	}
}

type testWrapper struct {
	Id   Key
	Name string
}

type testWrapperString struct {
	Id   string
	Name string
}

func TestToAndFromJSONInsideWrapper(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(5)
		w := &testWrapper{
			Id:   k,
			Name: "hehu",
		}
		enc, err := json.Marshal(w)
		if err != nil {
			t.Fatalf(err.Error())
		}
		var i interface{}
		err = json.Unmarshal(enc, &i)
		if err != nil {
			t.Fatalf("Bad json: %#v: %v", string(enc), err.Error())
		}
		w2 := &testWrapper{}
		if err := json.Unmarshal(enc, w2); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(w, w2) {
			t.Fatalf("%+v != %+v", w, w2)
		}
		w3 := &testWrapperString{}
		if err := json.Unmarshal(enc, w3); err != nil {
			t.Fatalf(err.Error())
		}
		k2, err := Decode(w3.Id)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if !k.Equal(k2) {
			t.Fatalf("%v != %v", k, k2)
		}
	}

}

func TestValidations(t *testing.T) {
	if _, err := New("Location", "ff", 0, ""); err == nil {
		t.Fatalf("should not work")
	}
	acc, err := New("Account", "fg", 0, "")
	if err != nil {
		panic(err)
	}
	loc, err := New("Location", "ff", 0, acc)
	if err != nil {
		t.Fatalf("should work")
	}
	if _, err := New("SpotifyAccount", "11212", 0, ""); err == nil {
		t.Fatalf("should not work")
	}
	if _, err := New("SpotifyAccount", loc.Encode(), 0, ""); err != nil {
		t.Fatalf("should work")
	}
}

func TestToAndFromJSON(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(5)
		enc, err := k.MarshalJSON()
		if err != nil {
			t.Fatalf(err.Error())
		}
		var i interface{}
		err = json.Unmarshal(enc, &i)
		if err != nil {
			t.Fatalf("Bad json: %#v: %v", string(enc), err.Error())
		}
		k2 := Key("")
		if err := k2.UnmarshalJSON(enc); err != nil {
			t.Fatalf(err.Error())
		}
		if !reflect.DeepEqual(k, k2) {
			t.Fatalf("\n%#v\n%#v\n", k, k2)
		}
	}
}

func TestEqual(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(6)
		k2, err := New(k.Kind(), k.StringID(), k.IntID(), k.Parent())
		if err != nil {
			panic(err)
		}
		if !k.Equal(k2) {
			t.Fatalf("Keys not equal")
		}
	}
}

func TestNilKeys(t *testing.T) {
	var k Key
	var k2 Key
	if !k.Equal(k2) || !k2.Equal(k) {
		t.Fatalf("wth")
	}
	k = randomKey(3)
	if k.Equal(k2) || k2.Equal(k) {
		t.Fatalf("wtf")
	}
}
