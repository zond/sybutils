package gae

import (
	"fmt"
	"reflect"
	"time"
)

const (
	BeforeCreateName = "BeforeCreate"
	BeforeUpdateName = "BeforeUpdate"
	BeforeSaveName   = "BeforeSave"
	BeforeDeleteName = "BeforeDelete"
	AfterCreateName  = "AfterCreate"
	AfterUpdateName  = "AfterUpdate"
	AfterSaveName    = "AfterSave"
	AfterLoadName    = "AfterLoad"
	AfterDeleteName  = "AfterDelete"
)

var processors = []string{
	BeforeCreateName,
	BeforeUpdateName,
	BeforeSaveName,
	BeforeDeleteName,
	AfterCreateName,
	AfterUpdateName,
	AfterSaveName,
	AfterLoadName,
	AfterDeleteName,
}

/*
runProcess will run a function with name first on the provided context with model as parameter, then on model, passing it c and arg.
*/
func runProcess(c PersistenceContext, model interface{}, name string, arg interface{}) error {
	timer := time.Now()
	typ := reflect.TypeOf(model)
	// First run the method with name in the provided context
	contextFunc := reflect.ValueOf(c).MethodByName(name).Interface().(func(interface{}) error)
	if err := contextFunc(model); err != nil {
		return err
	}
	// check if there is a function with name in model that takes arg
	if process, found, err := getProcess(model, name, arg); err != nil {
		return err
	} else if found {
		// if there is, run it with the proper number of arguments
		var results []reflect.Value
		if process.Type().NumIn() == 2 {
			if arg == nil {
				results = process.Call([]reflect.Value{reflect.ValueOf(c), reflect.Zero(process.Type().In(1))})
			} else {
				results = process.Call([]reflect.Value{reflect.ValueOf(c), reflect.ValueOf(arg)})
			}
		} else {
			results = process.Call([]reflect.Value{reflect.ValueOf(c)})
		}
		if !results[len(results)-1].IsNil() {
			// if the processor returned an error as the last return value, return the error
			if time.Now().Sub(timer) > (500 * time.Millisecond) {
				c.Infof("%v for %s is slow, took: %v", name, typ, time.Now().Sub(timer))
			}
			return results[len(results)-1].Interface().(error)
		}
	}
	if time.Now().Sub(timer) > (500 * time.Millisecond) {
		c.Infof("%v for %s is slow, took: %v", name, typ, time.Now().Sub(timer))
	}
	return nil
}

/*
getProcess tries to find a function with name in model that takes a PersistenceContext implementation and an arg (if arg != nil),
and return the function and if it was found.
*/
func getProcess(model interface{}, name string, arg interface{}) (process reflect.Value, found bool, err error) {
	val := reflect.ValueOf(model)
	if process = val.MethodByName(name); process.IsValid() {
		// if there was a function with name in model
		processType := process.Type()
		// if it has two in parameters, check that they are something that implements PersistenceContext, and something that arg is assignable to
		if processType.NumIn() == 2 {
			if !processType.In(0).Implements(reflect.TypeOf((*PersistenceContext)(nil)).Elem()) {
				err = fmt.Errorf("%+v#%v takes a %v, not a PersistenceContext as first argument", model, name, processType.In(0))
				return
			}
			if arg != nil {
				if !reflect.TypeOf(arg).AssignableTo(processType.In(1)) {
					err = fmt.Errorf("%+v#%v takes a %v, not a %v as second argument", model, name, processType.In(0), reflect.TypeOf(arg))
					return
				}
			}
		} else if processType.NumIn() == 1 {
			// if only one parameter, check that it implements PersistenceContext
			if !processType.In(0).Implements(reflect.TypeOf((*PersistenceContext)(nil)).Elem()) {
				err = fmt.Errorf("%+v#%v takes a %v, not a gae.PersistenceContext as argument", model, name, processType.In(0))
				return
			}
		} else {
			err = fmt.Errorf("%+v#%v doesn't take exactly one or two arguments", model, name)
			return
		}
		// check that the function returns exactly one value
		if processType.NumOut() != 1 {
			err = fmt.Errorf("%+v#%v doesn't produce exactly one return value", model, name)
			return
		}
		// that is assignable to error
		if !processType.Out(0).AssignableTo(reflect.TypeOf((*error)(nil)).Elem()) {
			err = fmt.Errorf("%+v#%v doesn't return an error", model, name)
			return
		}
		found = true
	}
	return
}
