package gae

import (
	"fmt"
	"reflect"

	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/gae/memcache"
	"github.com/soundtrackyourbrand/utils/key"
	"github.com/soundtrackyourbrand/utils/key/gaekey"

	"appengine"
	"appengine/datastore"
)

// finder encapsulates the knowledge that a model type is findable by a given set of fields.
type finder struct {
	fields []reflect.StructField
	model  interface{}
	typ    string
}

// registeredFinders is used to find what cache keys to invalidate when a model is CRUDed.
var registeredFinders = map[string][]finder{}

// newFinder returns an optionally registered finder after having validated the correct type of input data.
func newFinder(finderType string, model interface{}, register bool, fields ...string) (result finder) {
	typ := reflect.TypeOf(model).Elem()
	structFields := make([]reflect.StructField, len(fields))
	for index, field := range fields {
		if f, found := typ.FieldByName(field); found {
			structFields[index] = f
		} else {
			panic(fmt.Errorf("%+v doesn't have a field named %#v", model, field))
		}
	}
	result = finder{
		fields: structFields,
		model:  model,
		typ:    finderType,
	}
	if register {
		name := reflect.TypeOf(model).Elem().Name()
		registeredFinders[name] = append(registeredFinders[name], result)
	}
	return
}

/*
Finder will return a finder function that runs a datastore query to find matching models.

The returned function will set the Id field of all found models, and call their AfterLoad functions if any.
*/
func Finder(model interface{}, fields ...string) func(c PersistenceContext, dst interface{}, values ...interface{}) error {
	return newFinder("get", model, false, fields...).get
}

/*
AncestorFinder will return a finder function that memoizes running a datastore query to find matching models.

It will also register the finder so that MemcacheKeys will return keys to invalidate the result each time a matching model is CRUDed.

The returned function will set the Id field of all found models, and call their AfterLoad functions if any.
*/
func AncestorFinder(model interface{}, fields ...string) func(c PersistenceContext, dst interface{}, ancestor key.Key, values ...interface{}) error {
	return newFinder("get", model, true, fields...).getWithAncestor
}

func Counter(model interface{}, fields ...string) func(c PersistenceContext, values ...interface{}) (int, error) {
	return newFinder("count", model, false, fields...).count
}

func AncestorCounter(model interface{}, fields ...string) func(c PersistenceContext, ancestor key.Key, values ...interface{}) (int, error) {
	return newFinder("count", model, true, fields...).countWithAncestor
}

// find runs a datastore query, if ancestor != nil an ancestor query, and sets the id of all found models.
func (self finder) find(c PersistenceContext, dst interface{}, ancestor key.Key, values []interface{}) (err error) {
	q := datastore.NewQuery(reflect.TypeOf(self.model).Elem().Name())
	if ancestor != "" {
		q = q.Ancestor(gaekey.ToGAE(c, ancestor))
	}
	for index, value := range values {
		q = q.Filter(fmt.Sprintf("%v=", self.fields[index].Name), value)
	}
	var ids []*datastore.Key
	ids, err = q.GetAll(c, dst)
	if err = FilterOkErrors(err); err != nil {
		return
	}
	dstElem := reflect.ValueOf(dst).Elem()
	var element reflect.Value
	for index, id := range ids {
		element = dstElem.Index(index)
		if element.Kind() == reflect.Ptr {
			element = element.Elem()
		}
		var k key.Key
		if k, err = gaekey.FromGAE(id); err != nil {
			return
		}
		element.FieldByName(idFieldName).Set(reflect.ValueOf(k))
	}
	return
}

func (self finder) getCount(c PersistenceContext, ancestor key.Key, values []interface{}) (result int, err error) {
	q := datastore.NewQuery(reflect.TypeOf(self.model).Elem().Name())
	if ancestor != "" {
		q = q.Ancestor(gaekey.ToGAE(c, ancestor))
	}
	for index, value := range values {
		q = q.Filter(fmt.Sprintf("%v=", self.fields[index].Name), value)
	}
	result, err = q.Count(c)
	if err = FilterOkErrors(err); err != nil {
		return
	}
	return
}

// keyForValues returns the memcache key to use for the given ancestor and values searched for
func (self finder) keyForValues(ancestor key.Key, values []interface{}) string {
	return fmt.Sprintf("%v{Typ:%v,Ancestor:%v,%+v:%+v}", self.typ, reflect.TypeOf(self.model).Elem().Name(), ancestor, self.fields, values)
}

// cacheKeys will append to oldKeys, and also return as newKeys, all cache keys this finder may use to find the provided model.
// the reason there may be multiple keys is that we don't know which ancestor will be used when finding the model.
func (self finder) cacheKeys(c PersistenceContext, model interface{}, oldKeys *[]string) (newKeys []string, err error) {
	var id key.Key
	if _, id, err = getTypeAndId(model); err != nil {
		return
	}
	values := make([]interface{}, len(self.fields))
	val := reflect.ValueOf(model).Elem()
	for index, field := range self.fields {
		values[index] = val.FieldByName(field.Name).Interface()
	}
	if oldKeys == nil {
		oldKeys = &[]string{}
	}
	for id != "" {
		*oldKeys = append(*oldKeys, self.keyForValues(id.Parent(), values))
		id = id.Parent()
	}
	newKeys = *oldKeys
	return
}

func (self finder) count(c PersistenceContext, values ...interface{}) (result int, err error) {
	return self.countWithAncestor(c, "", values...)
}

// see Finder
func (self finder) get(c PersistenceContext, dst interface{}, values ...interface{}) (err error) {
	return self.getWithAncestor(c, dst, "", values...)
}

type countResult struct {
	Count int
}

// see AncestorFinder
func (self finder) countWithAncestor(c PersistenceContext, ancestor key.Key, values ...interface{}) (result int, err error) {
	if len(values) != len(self.fields) {
		err = fmt.Errorf("%+v does not match %+v", values, self.fields)
		return
	}
	// We can't really cache finders that don't use ancestor fields, since they are eventually consistent which might fill the cache with inconsistent data
	if ancestor == "" {
		if result, err = self.getCount(c, "", values); err != nil {
			return
		}
	} else {
		count := &countResult{}
		if err = memcache.Memoize(c, self.keyForValues(ancestor, values), count, func() (result interface{}, err error) {
			var num int
			if num, err = self.getCount(c, ancestor, values); err == nil {
				result = &countResult{
					Count: num,
				}
			}
			return
		}); err != nil {
			return
		}
		result = count.Count
	}
	return
}

// see AncestorFinder
func (self finder) getWithAncestor(c PersistenceContext, dst interface{}, ancestor key.Key, values ...interface{}) (err error) {
	if len(values) != len(self.fields) {
		wantedTypeNames := []string{}
		for _, field := range self.fields {
			wantedTypeNames = append(wantedTypeNames, field.Type.Name())
		}
		givenTypeNames := []string{}
		for _, val := range values {
			givenTypeNames = append(givenTypeNames, reflect.TypeOf(val).Name())
		}
		err = utils.Errorf("Finder wants %+v as arguments, but got %+v", wantedTypeNames, givenTypeNames)
		return
	}
	// We can't really cache finders that don't use ancestor fields, since they are eventually consistent which might fill the cache with inconsistent data
	if ancestor == "" {
		if err = self.find(c, dst, "", values); err != nil {
			return
		}
	} else {
		if err = memcache.Memoize(c, self.keyForValues(ancestor, values), dst, func() (result interface{}, err error) {
			if err = self.find(c, dst, ancestor, values); err == nil {
				result = dst
			}
			return
		}); err != nil {
			return
		}
	}
	val := reflect.ValueOf(dst).Elem()
	errors := appengine.MultiError{}
	for i := 0; i < val.Len(); i++ {
		el := val.Index(i)
		if err = runProcess(c, el.Addr().Interface(), AfterLoadName, nil); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		err = errors
	}
	return
}
