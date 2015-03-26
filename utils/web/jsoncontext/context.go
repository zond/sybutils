package jsoncontext

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"time"

	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/json"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
)

const (
	APIVersionHeader = "X-API-Version"
	RespondMarshal   = "respond"
)

func APIVersionMatcher(minAPIVersion, maxAPIVersion int) mux.MatcherFunc {
	return func(req *http.Request, match *mux.RouteMatch) bool {
		if minAPIVersion == 0 && maxAPIVersion == 0 {
			return true
		}
		header := req.Header.Get(APIVersionHeader)
		if header == "" {
			return false
		}
		apiVersion, err := strconv.Atoi(header)
		if err != nil {
			return false
		}
		if apiVersion < minAPIVersion || apiVersion > maxAPIVersion {
			return false
		}
		return true
	}
}

type JSONContext interface {
	httpcontext.HTTPContext
	APIVersion() int
	DecodeJSON(i interface{}) error
	DecodedBody() []byte
	LoadJSON(i interface{}) error
	CopyJSON(in, out interface{}) error
	MarshalJSON(subContext interface{}, body interface{}, reason interface{}) ([]byte, error)
}

type JSONContextLogger interface {
	JSONContext
	httpcontext.Logger
}

type DefaultJSONContext struct {
	httpcontext.HTTPContextLogger
	apiVersion      int
	decodedBody     []byte
	marshalSyncLock *utils.SyncLock
}

func NewJSONContext(c httpcontext.HTTPContextLogger) (result *DefaultJSONContext) {
	result = &DefaultJSONContext{
		HTTPContextLogger: c,
		marshalSyncLock:   &utils.SyncLock{},
	}
	if result.Req() != nil {
		if header := result.Req().Header.Get(APIVersionHeader); header != "" {
			if version, err := strconv.Atoi(header); err == nil {
				result.apiVersion = version
			}
		}
	}
	return
}

func (self *DefaultJSONContext) CopyJSON(in, out interface{}) (err error) {
	token, err := self.AccessToken(nil)
	if err != nil {
		return
	}
	return json.CopyJSON(in, out, self.Req().Method, token.Scopes()...)
}

func (self *DefaultJSONContext) DecodedBody() []byte {
	return self.decodedBody
}

func (self *DefaultJSONContext) DecodeJSON(i interface{}) (err error) {
	buf := &bytes.Buffer{}
	bodyReader := io.TeeReader(self.Req().Body, buf)
	err = json.NewDecoder(bodyReader).Decode(i)
	self.decodedBody = buf.Bytes()
	return
}

func (self *DefaultJSONContext) LoadJSON(out interface{}) (err error) {
	at, err := self.AccessToken(nil)
	if err != nil {
		return json.LoadJSON(self.Req().Body, out, self.Req().Method)
	}
	scopes := at.Scopes()
	return json.LoadJSON(self.Req().Body, out, self.Req().Method, scopes...)
}

func (self *DefaultJSONContext) APIVersion() int {
	return self.apiVersion
}

type Resp struct {
	Status int
	Body   interface{}
}

func (self Resp) Error() string {
	return fmt.Sprint(self.Body)
}

/*
MarshalJSON will recursively run any found `BeforeMarshal` functions on the content with arg and a stack of container instances, and then json marshal it.

It will not recurse down further after a BeforeMarshal function has been found, but it will run all top level BeforeMarshal functions that it finds.
*/
func (self *DefaultJSONContext) MarshalJSON(c interface{}, body interface{}, arg interface{}) (result []byte, err error) {
	// declare a function that recursively will run itself
	var runRecursive func(reflect.Value, reflect.Value) error

	cVal := reflect.ValueOf(c)
	contextType := reflect.TypeOf((*JSONContextLogger)(nil)).Elem()
	// initialize an empty container stack
	stackType := reflect.TypeOf([]interface{}{})

	// implement the function
	runRecursive = func(val reflect.Value, stack reflect.Value) error {
		// push this instance to the stack
		stack = reflect.Append(stack, val)

		// Try run BeforeMarshal
		fun := val.MethodByName("BeforeMarshal")
		if fun.IsValid() {
			// make sure we don't run BeforeMarshal on any other things at the same time, at least in this context.
			return self.marshalSyncLock.Sync(val.Interface(), func() (err error) {
				// Validate BeforeMarshal takes something that implements JSONContextLogger
				if err = utils.ValidateFuncInput(fun.Interface(), []reflect.Type{contextType, stackType}); err != nil {
					if err = utils.ValidateFuncInput(fun.Interface(), []reflect.Type{contextType, stackType, reflect.TypeOf(arg)}); err != nil {
						return fmt.Errorf("BeforeMarshal needs to take an JSONContextLogger")
					}
				}

				// Validate BeforeMarshal returns an error
				if err = utils.ValidateFuncOutput(fun.Interface(), []reflect.Type{reflect.TypeOf((*error)(nil)).Elem()}); err != nil {
					return fmt.Errorf("BeforeMarshal needs to return an error")
				}

				args := []reflect.Value{cVal, stack}
				if fun.Type().NumIn() == 3 {
					args = append(args, reflect.ValueOf(arg))
				}
				timer := time.Now()

				// run the actual BeforeMarshal
				res := fun.Call(args)

				if time.Now().Sub(timer) > (500 * time.Millisecond) {
					self.Infof("BeforeMarshal for %s is slow, took: %v", val.Type(), time.Now().Sub(timer))
				}

				if !res[0].IsNil() {
					err = res[0].Interface().(error)
				}
				return
			})
		}

		// Try do recursion on these types, if we didn't find a BeforeMarshal func on the val itself
		switch val.Kind() {
		case reflect.Ptr, reflect.Interface:
			if val.IsNil() {
				return nil
			}
			return runRecursive(val.Elem(), stack)

		case reflect.Slice:
			for i := 0; i < val.Len(); i++ {
				if err := runRecursive(val.Index(i).Addr(), stack); err != nil {
					return err
				}
			}
			break

		case reflect.Struct:
			for i := 0; i < val.NumField(); i++ {
				if val.Type().Field(i).PkgPath == "" {
					if err := runRecursive(val.Field(i), stack); err != nil {
						return err
					}
				}
			}
			break
		}
		return nil
	}

	// Run recursive reflection on self.Body that executes BeforeMarshal on every object possible.
	stack := []interface{}{}
	if err = runRecursive(reflect.ValueOf(body), reflect.ValueOf(stack)); err != nil {
		return
	}

	// This makes sure that replies that returns a slice that is empty returns a '[]' instad of 'null'
	bodyVal := reflect.ValueOf(body)
	if bodyVal.Kind() == reflect.Slice && bodyVal.IsNil() {
		reflect.ValueOf(&body).Elem().Set(reflect.MakeSlice(bodyVal.Type(), 0, 0))
	}

	if result, err = json.MarshalIndent(body, "", "  "); err != nil {
		return
	}

	return
}

func respond(c JSONContextLogger, status int, body interface{}) (err error) {
	if body != nil {
		c.Resp().Header().Set("Content-Type", "application/json; charset=UTF-8")
	}
	// This timestamp is to be used by Tyson as an authoritative source of time, to compensate for broken
	// clocks in devices.
	t := time.Now().UTC()
	c.Resp().Header().Set("X-UTC-Time", fmt.Sprintf("%d", t.Unix())) // .UnixNano is also available
	if status != 0 {
		c.Resp().WriteHeader(status)
	}
	if body != nil {
		var marshalled []byte
		marshalled, err = c.MarshalJSON(c, body, RespondMarshal)
		if err != nil {
			return
		}
		_, err = c.Resp().Write(marshalled)
		return
	}
	return nil
}

func (self Resp) Respond(c httpcontext.HTTPContextLogger) (err error) {
	return respond(c.(JSONContextLogger), self.Status, self.Body)
}

type JSONError struct {
	httpcontext.HTTPError
}

func (self JSONError) GetStatus() int {
	return self.Status
}

func (self JSONError) Respond(c httpcontext.HTTPContextLogger) (err error) {
	return respond(c.(JSONContextLogger), self.Status, self.Body)
}

func NewError(status int, body interface{}, info string, cause error) (result JSONError) {
	return JSONError{httpcontext.NewError(status, body, info, cause)}
}

type field struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
	Cause   error  `json:"-"`
}

type ValidationError struct {
	Status int
	Cause  error
	Info   string
	Fields map[string]field `json:"fields,omitempty"`
}

func (self ValidationError) GetStack() string {
	return ""
}

func (self ValidationError) GetStatus() int {
	return self.Status
}

func (self *ValidationError) AddField(fieldName, message string, code int, cause error, status int) *ValidationError {
	if self == nil {
		return &ValidationError{
			Fields: map[string]field{
				fieldName: {
					Message: message,
					Code:    code,
					Cause:   cause,
				},
			},
			Status: status,
		}
	}
	if self.Fields == nil {
		self.Fields = make(map[string]field)
	}
	self.Fields[fieldName] = field{
		Message: message,
		Code:    code,
		Cause:   cause,
	}
	if status > self.Status {
		self.Status = status
	}
	return self
}

func (self ValidationError) Error() string {
	return fmt.Sprint(self.Fields)
}

func (self ValidationError) Respond(c httpcontext.HTTPContextLogger) error {
	if self.Fields != nil {
		c.Resp().Header().Set("Content-Type", "application/json; charset=UTF-8")
	}
	if self.Status != 0 {
		c.Resp().WriteHeader(self.Status)
	}
	return json.NewEncoder(c.Resp()).Encode(self)
}

func Handle(c JSONContextLogger, f func() (Resp, error), minAPIVersion, maxAPIVersion int, scopes ...string) {
	httpcontext.Handle(c, func() (err error) {
		if minAPIVersion != 0 && c.APIVersion() < minAPIVersion {
			err = NewError(417, fmt.Sprintf("X-API-Version header has to request API version greater than %v", minAPIVersion), fmt.Sprintf("Headers: %+v", c.Req().Header), nil)
			return
		}
		if maxAPIVersion != 0 && c.APIVersion() > maxAPIVersion {
			err = NewError(417, fmt.Sprintf("X-API-Version header has to request API version less than %v", maxAPIVersion), fmt.Sprintf("Headers: %+v", c.Req().Header), nil)
			return
		}
		resp, err := f()
		if err == nil {
			err = resp.Respond(c)
		}
		return
	}, scopes...)
}

func HandlerFunc(f func(c JSONContextLogger) (Resp, error), minAPIVersion, maxAPIVersion int, scopes ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := NewJSONContext(httpcontext.NewHTTPContext(w, r))
		Handle(c, func() (Resp, error) {
			return f(c)
		}, minAPIVersion, maxAPIVersion, scopes...)
	})
}
