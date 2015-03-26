package gaekey

import (
	"appengine_internal"
	"github.com/soundtrackyourbrand/utils/key"
	"math/rand"
	"reflect"
	"testing"
)

func randomString() string {
	buf := make([]byte, 15)
	for index := range buf {
		buf[index] = byte(rand.Int())
	}
	return string(buf)
}

func randomKey(parents int) key.Key {
	if parents == 0 {
		key, err := key.New(randomString(), randomString(), rand.Int63(), "")
		if err != nil {
			panic(err)
		}
		return key
	}
	key, err := key.New(randomString(), randomString(), rand.Int63(), randomKey(parents-1))
	if err != nil {
		panic(err)
	}
	return key
}

type dummyContext struct {
	AppID string
}

func (self dummyContext) Debugf(s string, args ...interface{})    {}
func (self dummyContext) Infof(s string, args ...interface{})     {}
func (self dummyContext) Warningf(s string, args ...interface{})  {}
func (self dummyContext) Criticalf(s string, args ...interface{}) {}
func (self dummyContext) Errorf(s string, args ...interface{})    {}
func (self dummyContext) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	return nil
}
func (self dummyContext) FullyQualifiedAppID() string { return self.AppID }
func (self dummyContext) Request() interface{}        { return nil }

func TestFromAndToGAE(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := randomKey(3)
		k2 := ToGAE(dummyContext{"myapp"}, k)
		k3, err := FromGAE(k2)
		if err != nil {
			panic(err)
		}
		k4 := ToGAE(dummyContext{"myapp"}, k3)
		if !reflect.DeepEqual(k, k3) {
			t.Fatalf("%+v != %+v", k, k3)
		}
		if !reflect.DeepEqual(k2, k4) {
			t.Fatalf("%+v != %+v", k2, k4)
		}
	}
}
