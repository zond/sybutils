package web

import (
	"appengine/datastore"
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/gaecontext"
	"github.com/soundtrackyourbrand/utils/gae/key"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"github.com/soundtrackyourbrand/utils/web/jsoncontext"
	"net/http"
	"reflect"
	"sync"
	"time"
)

type Bench struct {
	N            int
	Measurements []string
}

func (self *Bench) Measure(factor int, name string, f func()) {
	start := time.Now()
	for i := 0; i < self.N*factor; i++ {
		f()
	}
	total := time.Now().Sub(start)
	self.Measurements = append(self.Measurements, fmt.Sprintf("%v x %v: %v, AVG: %v", factor*self.N, name, total, total/time.Duration(self.N*factor)))
}

type str struct {
	Id  key.Key `datastore:"-"`
	Age int
}

func (self *str) IsOk(b bool) bool {
	return b
}

func test(c gaecontext.JSONContext) (result jsoncontext.Resp, err error) {
	b := &Bench{}
	c.DecodeJSON(b)
	if b.N == 0 {
		b.N = 100
	}
	m := map[int]bool{}
	for i := 0; i < b.N; i++ {
		m[i] = true
	}
	i := 0
	b.Measure(10000, "MapPut", func() {
		m[i] = false
		i++
	})
	i = 0
	x := false
	b.Measure(100000, "MapGet", func() {
		x = m[i]
		i++
	})
	lock := &sync.Mutex{}
	b.Measure(100000, "LockUnlock", func() {
		lock.Lock()
		lock.Unlock()
	})
	t := &str{}
	b.Measure(1, "MemcacheGet", func() {
		if _, err = memcache.Get(c, "blapapa", t); err != nil {
			panic(err)
		}
	})
	t.Age = 21
	b.Measure(1, "MemcachePut", func() {
		if err = memcache.Put(c, "blapapa", t); err != nil {
			panic(err)
		}
	})
	i = 1
	b.Measure(1, "DatastorePut", func() {
		t.Id = key.New("str", "", int64(i), nil)
		t.Age = i
		if _, err = datastore.Put(c, t.Id.ToGAE(c), t); err != nil {
			panic(err)
		}
		i++
	})
	i = 1
	b.Measure(1, "DatastoreGet", func() {
		t.Id = key.New("str", "", int64(i), nil)
		if err = datastore.Get(c, t.Id.ToGAE(c), t); err != nil {
			panic(err)
		}
		i++
	})
	i = 1
	all := &[]str{}
	b.Measure(1, "DatastoreQuery", func() {
		if _, err = datastore.NewQuery("str").Filter("Age=", i).GetAll(c, all); err != nil {
			panic(err)
		}
	})
	b.Measure(100, "ReflectCall", func() {
		x = reflect.ValueOf(t).MethodByName("IsOk").Call([]reflect.Value{reflect.ValueOf(true)})[0].Interface().(bool)
	})
	sl := make([]int, b.N)
	i = 0
	b.Measure(100, fmt.Sprintf("Linear seek in %v long slice", b.N), func() {
		for _, e := range sl {
			x = e == i
		}
		i++
	})
	ch := make(chan bool, 1)
	b.Measure(10000, "Channel send/receive", func() {
		ch <- true
		x = <-ch
	})
	result.Body = b
	return
}

func init() {
	http.Handle("/", gaecontext.JSONHandlerFunc(test))
}
