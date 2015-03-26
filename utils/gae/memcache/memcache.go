package memcache

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"time"

	"github.com/soundtrackyourbrand/utils"

	"appengine"
	"appengine/delay"
	"appengine/memcache"
	"appengine/taskqueue"
)

var MemcacheEnabled = true

type TransactionContext interface {
	appengine.Context
	InTransaction() bool
	AfterTransaction(interface{}) error
}

const (
	regular = iota
	nilCache
)

var Codec = memcache.Gob
var ErrCacheMiss = memcache.ErrCacheMiss

var deleteFunc = delay.Func("github.com/soundtrackyourbrand/utils/gae/memcache.delayedDelete", delayedDelete)

func delayedDelete(c appengine.Context, keys []string) (err error) {
	return del(c, keys...)
}

/*
Keyify will create a memcache-safe key from k by hashing and base64-encoding it.
*/
func Keyify(k string) (result string, err error) {
	buf := new(bytes.Buffer)
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	h := sha1.New()
	io.WriteString(h, k)
	sum := h.Sum(nil)
	wrote, err := enc.Write(sum)
	if err != nil {
		return
	} else if wrote != len(sum) {
		err = utils.Errorf("Tried to write %v bytes but wrote %v bytes", len(sum), wrote)
		return
	}
	if err = enc.Close(); err != nil {
		return
	}
	result = string(buf.Bytes())
	return
}

func Incr(c TransactionContext, key string, delta int64, initial uint64) (newValue uint64, err error) {
	k, err := Keyify(key)
	if err != nil {
		return
	}
	if newValue, err = memcache.Increment(c, k, delta, initial); err != nil {
		err = utils.Errorf("Error doing Increment %#v: %v", k, err)
		return
	}
	return
}

func IncrExisting(c TransactionContext, key string, delta int64) (newValue uint64, err error) {
	k, err := Keyify(key)
	if err != nil {
		return
	}
	if newValue, err = memcache.IncrementExisting(c, k, delta); err != nil {
		err = utils.Errorf("Error doing IncrementExisting %#v: %v", k, err)
		return
	}
	return
}

/*
Del will delete the keys from memcache.

If c is InTransaction it will put the actual deletion inside c.AfterTransaction, otherwise
the deletion will execute immediately.
*/
func Del(c TransactionContext, keys ...string) (err error) {
	if !MemcacheEnabled {
		return
	}
	if c.InTransaction() {
		return c.AfterTransaction(func(c TransactionContext) error {
			return delWithRetry(c, keys...)
		})
	}
	return delWithRetry(c, keys...)
}

/*
delWithRetry will delete the keys from memcache. If it fails, it will retry.
*/
func delWithRetry(c TransactionContext, keys ...string) (err error) {
	waitTime := time.Millisecond * 10
	deadline := time.Now().Add(time.Second * 2)

	for time.Now().Before(deadline) {
		err = del(c, keys...)
		if err == nil {
			break
		}
		time.Sleep(waitTime)
		waitTime = waitTime * 2
	}
	if err != nil {
		var task *taskqueue.Task
		if task, err = deleteFunc.Task(keys); err != nil {
			return
		}
		if _, err = taskqueue.Add(c, task, "delayed-memcache-invalidate"); err != nil {
			return
		}
	}
	return
}

/*
del will delete the keys from memcache.
*/
func del(c appengine.Context, keys ...string) (err error) {
	for index, key := range keys {
		var k string
		k, err = Keyify(key)
		if err != nil {
			return
		}
		keys[index] = k
	}
	if err = memcache.DeleteMulti(c, keys); err != nil {
		if merr, ok := err.(appengine.MultiError); ok {
			errors := make(appengine.MultiError, len(merr))
			actualErrors := 0
			for index, serr := range merr {
				if serr != memcache.ErrCacheMiss {
					errors[index] = utils.Errorf("Error doing DeleteMulti: %v", serr)
					actualErrors++
				}
			}
			if actualErrors > 0 {
				err = errors
				return
			} else {
				err = nil
			}
		} else {
			if err == ErrCacheMiss {
				err = nil
			} else {
				err = utils.Errorf("Error doing DeleteMulti: %v", err)
				return
			}
		}
	}
	return
}

/*
Get will lookup key and load it into val.

If c is in a transaction no lookup will take place.
*/
func Get(c TransactionContext, key string, val interface{}) (found bool, err error) {
	if !MemcacheEnabled {
		return
	}
	if c.InTransaction() {
		return
	}
	k, err := Keyify(key)
	if err != nil {
		return
	}
	if _, err = Codec.Get(c, k, val); err != nil {
		if err == memcache.ErrCacheMiss {
			err = nil
		} else {
			c.Errorf("Error doing Get %#v: %v", k, err)
		}
		return
	}

	found = true
	return
}

/*
CAS will replace expected with replacement in memcache if expected is the current value.
*/
func CAS(c TransactionContext, key string, expected, replacement interface{}) (success bool, err error) {
	keyHash, err := Keyify(key)
	if err != nil {
		return
	}
	var item *memcache.Item
	if item, err = memcache.Get(c, keyHash); err != nil {
		if err == memcache.ErrCacheMiss {
			err = nil
		} else {
			err = utils.Errorf("Error doing Get %#v: %v", keyHash, err)
		}
		return
	}
	var encoded []byte
	if encoded, err = Codec.Marshal(expected); err != nil {
		return
	}
	if bytes.Compare(encoded, item.Value) != 0 {
		success = false
		return
	}
	if encoded, err = Codec.Marshal(replacement); err != nil {
		return
	}
	item.Value = encoded
	if err = memcache.CompareAndSwap(c, item); err != nil {
		if err == memcache.ErrCASConflict {
			err = nil
		} else {
			marshalled, _ := Codec.Marshal(replacement)
			err = utils.Errorf("Error doing CompareAndSwap %#v to %v bytes: %v", item.Key, len(marshalled), err)
		}
		return
	}
	success = true
	return
}

/*
Put will put val under key.
*/
func Put(c TransactionContext, key string, val interface{}) (err error) {
	return putUntil(c, nil, key, val)
}

/*
PutUntil will put val under key for at most until.
*/
func PutUntil(c TransactionContext, until time.Duration, key string, val interface{}) (err error) {
	return putUntil(c, &until, key, val)
}

/*
codecSetWithRetry will try to use codec.Set to set the value. If it fails it will retry.
*/
func codecSetWithRetry(c TransactionContext, codec memcache.Codec, item *memcache.Item) (err error) {
	waitTime := time.Millisecond * 10
	deadline := time.Now().Add(time.Second * 2)

	for time.Now().Before(deadline) {
		err = codec.Set(c, item)
		if err == nil {
			break
		}
		time.Sleep(waitTime)
		waitTime *= 2
	}
	if err != nil {
		marshalled, _ := codec.Marshal(item.Object)
		err = utils.Errorf("Error doing Codec.Set %#v with %v bytes: %v", item.Key, len(marshalled), err)
	}
	return
}

func putUntil(c TransactionContext, until *time.Duration, key string, val interface{}) (err error) {
	if !MemcacheEnabled {
		return
	}
	k, err := Keyify(key)
	if err != nil {
		return
	}
	item := &memcache.Item{
		Key:    k,
		Object: val,
	}
	if until != nil {
		item.Expiration = *until
	}
	return codecSetWithRetry(c, Codec, item)
}

/*
Memoize will lookup super and generate a new key from its contents and key. If super is missing a new random value will be inserted there.

It will then lookup that key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache.

It returns whether the value was nil (either from memcache or from the generatorFunction).

Deleting super will invalidate all keys under it due to the composite keys being impossible to regenerate again.
*/
func Memoize2(c TransactionContext, super, key string, destP interface{}, f func() (interface{}, error)) (err error) {
	superH, err := Keyify(super)
	if err != nil {
		return
	}
	var seed string
	var item *memcache.Item
	if item, err = memcache.Get(c, superH); err != nil && err != memcache.ErrCacheMiss {
		c.Errorf("Error doing Get %#v: %v", superH, err)
		err = memcache.ErrCacheMiss
	}
	if err == memcache.ErrCacheMiss {
		seed = fmt.Sprint(rand.Int63())
		item = &memcache.Item{
			Key:   superH,
			Value: []byte(seed),
		}
		if err = memcache.Set(c, item); err != nil {
			err = utils.Errorf("Error doing Set %#v with %v bytes: %v", item.Key, len(item.Value), err)
			return
		}
	} else {
		seed = string(item.Value)
	}
	return Memoize(c, fmt.Sprintf("%v@%v", key, seed), destP, f)
}

/*
MemoizeDuringSmart will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache with a timeout of duration.
*/
func MemoizeDuringSmart(c TransactionContext, key string, cacheNil bool, destP interface{}, f func() (interface{}, time.Duration, error)) (err error) {
	errSlice := memoizeMulti(c, []string{key}, cacheNil, []interface{}{destP}, []func() (interface{}, time.Duration, error){f})
	return errSlice[0]
}

/*
MemoizeDuring will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache with a timeout of duration.
*/
func MemoizeDuring(c TransactionContext, key string, duration time.Duration, cacheNil bool, destP interface{}, f func() (interface{}, error)) (err error) {
	errSlice := memoizeMulti(c, []string{key}, cacheNil, []interface{}{destP}, []func() (interface{}, time.Duration, error){
		func() (res interface{}, dur time.Duration, err error) {
			res, err = f()
			dur = duration
			return
		},
	})
	return errSlice[0]
}

/*
Memoize will lookup key and load it into destinatinoPointer. A missing value will be generated by the generatorFunction and saved in memcache.
*/
func Memoize(c TransactionContext, key string, destP interface{}, f func() (interface{}, error)) (err error) {
	errSlice := memoizeMulti(c, []string{key}, true, []interface{}{destP}, []func() (interface{}, time.Duration, error){
		func() (res interface{}, dur time.Duration, err error) {
			res, err = f()
			return
		},
	})
	return errSlice[0]
}

/*
memGetMulti will look for all provided keys, and load them into the destinatinoPointers.

It will return the memcache.Items it found, and any errors the lookups caused.

If c is within a transaction no lookup will take place and errors will be slice of memcache.ErrCacheMiss.
*/
func memGetMulti(c TransactionContext, keys []string, destinationPointers []interface{}) (items []*memcache.Item, errors appengine.MultiError) {
	items = make([]*memcache.Item, len(keys))
	errors = make(appengine.MultiError, len(keys))
	if !MemcacheEnabled || c.InTransaction() {
		for index, _ := range errors {
			errors[index] = memcache.ErrCacheMiss
		}
		return
	}

	itemHash, err := memcache.GetMulti(c, keys)
	if err != nil {
		c.Errorf("Error doing GetMulti: %v", err)
		for index, _ := range errors {
			errors[index] = ErrCacheMiss
		}
		err = errors
	}

	var item *memcache.Item
	var ok bool
	for index, keyHash := range keys {
		if item, ok = itemHash[keyHash]; ok {
			items[index] = item
			if err := Codec.Unmarshal(item.Value, destinationPointers[index]); err != nil {
				errors[index] = err
			}
		} else {
			errors[index] = memcache.ErrCacheMiss
		}
	}
	return
}

/*
MemoizeMulti will look for all provided keys, and load them into the destinationPointers.

Any missing values will be generated using the generatorFunctions and put in memcache without a timeout.
*/
func MemoizeMulti(c TransactionContext, keys []string, destinationPointers []interface{}, generatorFunctions []func() (interface{}, error)) (errors appengine.MultiError) {
	newFunctions := make([]func() (interface{}, time.Duration, error), len(generatorFunctions))
	for index, gen := range generatorFunctions {
		genCpy := gen
		newFunctions[index] = func() (res interface{}, dur time.Duration, err error) {
			res, err = genCpy()
			return
		}
	}
	return memoizeMulti(c, keys, true, destinationPointers, newFunctions)
}

/*
memoizeMulti will look for all provided keys, and load them into the destinationPointers.

Any missing values will be generated using the generatorFunctions and put in memcache with a duration timeout.

If cacheNil is true, nil results or memcache.ErrCacheMiss errors from the generator function will be cached.

It returns a slice of bools that show whether each value was found (either from memcache or from the genrator function).
*/
func memoizeMulti(
	c TransactionContext,
	keys []string,
	cacheNil bool,
	destinationPointers []interface{},
	generatorFunctions []func() (interface{}, time.Duration, error)) (errors appengine.MultiError) {

	// First generate memcache friendly key hashes from all the provided keys.
	keyHashes := make([]string, len(keys))
	for index, key := range keys {
		k, err := Keyify(key)
		if err != nil {
			errors = appengine.MultiError{err}
			return
		}
		keyHashes[index] = k
	}

	// Then, run a memGetMulti using these keys, and warn if it is slow.
	t := time.Now()
	var items []*memcache.Item
	items, errors = memGetMulti(c, keyHashes, destinationPointers)
	if d := time.Now().Sub(t); d > time.Millisecond*10 {
		c.Debugf("SLOW memGetMulti(%v): %v", keys, d)
	}

	// Create a channel to handle any panics produced by the concurrent code.
	panicChan := make(chan interface{}, len(items))

	// For all the items we tried to fetch...
	for i, item := range items {

		// set up variables to use in the iteration
		index := i
		err := errors[index]
		keyHash := keyHashes[index]
		destinationPointer := destinationPointers[index]
		if err == memcache.ErrCacheMiss {
			// for cache misses, background do..
			go func() (err error) {
				// defer fetching any panics and sending them to the panic channel
				defer func() {
					errors[index] = err
					if e := recover(); e != nil {
						c.Infof("Panic: %v", e)
						panicChan <- fmt.Errorf("%v\n%v", e, utils.Stack())
					} else {
						// no panics will send a nil, which is necessary since we wait for all goroutines to send SOMETHING on the channel
						panicChan <- nil
					}
				}()
				var result interface{}
				var duration time.Duration
				found := true
				// try to run the generator function
				if result, duration, err = generatorFunctions[index](); err != nil {
					if err != memcache.ErrCacheMiss {
						return
					} else {
						// ErrCacheMiss from the generator function means that we want the caller to think there is no data to return
						found = false
					}
				} else {
					// if there is no error, check if we got a nil
					found = !utils.IsNil(result)
					if !found {
						// if we did, we fake an ErrCacheMiss
						err = memcache.ErrCacheMiss
					}
				}
				// If we are not inside a transaction, we have to store the result in memcache
				if !c.InTransaction() && (found || cacheNil) {
					obj := result
					var flags uint32
					if !found {
						// if the generator responded with nil or a cache miss, flag this cache entry as a cache miss for future reference
						obj = reflect.Indirect(reflect.ValueOf(destinationPointer)).Interface()
						flags = nilCache
					}
					if err2 := codecSetWithRetry(c, Codec, &memcache.Item{
						Key:        keyHash,
						Flags:      flags,
						Object:     obj,
						Expiration: duration,
					}); err2 != nil {
						err = err2
						return
					}
				}
				if found {
					// if we actually found something, copy the result to the destination
					utils.ReflectCopy(result, destinationPointer)
				}
				return
			}()
		} else if err != nil {
			// any errors will bubble up the panic channel
			panicChan <- nil
		} else {
			// if we FOUND something, but it was flagged as a cache miss, fake a cache miss
			if item.Flags&nilCache == nilCache {
				errors[index] = memcache.ErrCacheMiss
			}
			panicChan <- nil
		}
	}

	// collect any panics, and raise them if we found any
	panics := []interface{}{}
	for _, _ = range items {
		if e := <-panicChan; e != nil {
			panics = append(panics, e)
		}
	}
	if len(panics) > 0 {
		panic(panics)
	}
	return
}
