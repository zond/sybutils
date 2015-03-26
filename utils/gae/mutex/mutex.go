package mutex

import (
	"appengine/datastore"
	"fmt"
	"github.com/soundtrackyourbrand/utils/gae/gaecontext"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"time"
)

const (
	spintime = time.Millisecond * 100
)

func lockKeyForName(name string) string {
	return fmt.Sprintf("github.com/soundtrackyourbrand/utils/gae/mutex.Mutex{Name:%v}", name)
}

func lockIdForName(c gaecontext.GAEContext, name string) *datastore.Key {
	return datastore.NewKey(c, "github.com/soundtrackyourbrand/utils/gae/mutex.Mutex{Name:%v}", name, 0, nil)
}

type Mutex struct {
	Name     string
	LockedAt time.Time
}

func New(name string) *Mutex {
	return &Mutex{
		Name: name,
	}
}

func (self *Mutex) Lock(c gaecontext.GAEContext, timeout time.Duration) (err error) {
	success := false
	lockKey := lockKeyForName(self.Name)
	id := lockIdForName(c, self.Name)
	for !success {
		var waiting uint64
		for waiting != 1 {
			if waiting, err = memcache.Incr(c, lockKey, 1, 0); err != nil {
				return
			}
			time.Sleep(spintime)
		}
		if err = c.Transaction(func(c gaecontext.GAEContext) (err error) {
			err = datastore.Get(c, id, self)
			if err == datastore.ErrNoSuchEntity || (err == nil && self.LockedAt.Add(timeout).Before(time.Now())) {
				self.LockedAt = time.Now()
				if _, err = datastore.Put(c, id, self); err == nil {
					success = true
				}
			}
			return
		}, false); err != nil {
			return
		}
	}
	return nil
}

func (self *Mutex) Unlock(c gaecontext.GAEContext) (err error) {
	if err = datastore.Delete(c, lockIdForName(c, self.Name)); err != nil {
		return
	}
	err = memcache.Del(c, lockKeyForName(self.Name))
	return
}
