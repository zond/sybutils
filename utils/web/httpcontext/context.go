package httpcontext

import (
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/zond/sybutils/utils"
)

const (
	AuthorizationHeader = "Authorization"
)

var ErrMissingToken = fmt.Errorf("No authorization header or token query parameter found")

var authPattern = regexp.MustCompile("^Bearer (.*)$")

var prefPattern = regexp.MustCompile("^([^\\s;]+)(;q=([\\d.]+))?$")

var ErrorStackTraces = false

type Statuserr interface {
	error
	GetStatus() int
}

type HTTPError struct {
	Status int
	Body   interface{}
	Cause  error
	Info   string
	Stack  string
}

func (self HTTPError) GetStatus() int {
	return self.Status
}

func (self HTTPError) GetStack() string {
	return self.Stack
}

func (self HTTPError) String() string {
	return fmt.Sprintf("Status: %v\nBody: %v\nCause: %v\nInfo: %v\nStack: %s", self.Status, self.Body, self.Cause, self.Info, self.Stack)
}

func NewError(status int, body interface{}, info string, cause error) (result HTTPError) {
	result = HTTPError{
		Status: status,
		Body:   body,
		Cause:  cause,
		Info:   info,
	}

	if ErrorStackTraces && status >= 500 {
		result.Stack = utils.Stack()
	}
	return
}

func (self HTTPError) Respond(c HTTPContext) (err error) {
	if self.Status != 0 {
		c.Resp().WriteHeader(self.Status)
	}
	if self.Body != nil {
		_, err = fmt.Fprint(c.Resp(), self.Body)
	}
	return
}

func (self HTTPError) Error() string {
	return fmt.Sprintf("%v, %+v, %v, %#v", self.Status, self.Body, self.Cause, self.Info)
}

type Responder interface {
	Respond(c HTTPContext) error
}

type MemorableResponseWriter interface {
	http.ResponseWriter
	Status() int
	StartedAt() time.Time
}

type DefaultMemorableResponseWriter struct {
	http.ResponseWriter
	status    int
	startedAt time.Time
}

func (self *DefaultMemorableResponseWriter) StartedAt() time.Time {
	return self.startedAt
}

func (self *DefaultMemorableResponseWriter) Status() int {
	if self.status == 0 {
		return http.StatusOK
	}
	return self.status
}

func (self *DefaultMemorableResponseWriter) Header() http.Header {
	return self.ResponseWriter.Header()
}

func (self *DefaultMemorableResponseWriter) Write(b []byte) (int, error) {
	if self.status == 0 {
		self.status = http.StatusOK
	}
	return self.ResponseWriter.Write(b)
}

func (self *DefaultMemorableResponseWriter) WriteHeader(status int) {
	self.status = status
	self.ResponseWriter.WriteHeader(status)
}

type HTTPContext interface {
	Vars() map[string]string
	Req() *http.Request
	Resp() MemorableResponseWriter
	MostAccepted(name, def string) string
	AccessToken(dst utils.AccessToken) (utils.AccessToken, error)
	CheckScopes([]string) error
}

type DefaultHTTPContext struct {
	response MemorableResponseWriter
	request  *http.Request
	vars     map[string]string
}

func NewHTTPContext(w http.ResponseWriter, r *http.Request) (result *DefaultHTTPContext) {
	result = &DefaultHTTPContext{
		response: &DefaultMemorableResponseWriter{
			ResponseWriter: w,
			startedAt:      time.Now(),
		},
		request: r,
		vars:    mux.Vars(r),
	}
	return
}

func MostAccepted(r *http.Request, name, def string) string {
	bestValue := def
	var bestScore float64 = -1
	var score float64
	for _, pref := range strings.Split(r.Header.Get(name), ",") {
		if match := prefPattern.FindStringSubmatch(pref); match != nil {
			score = 1
			if match[3] != "" {
				score, _ = strconv.ParseFloat(match[3], 64)
			}
			if score > bestScore {
				bestScore = score
				bestValue = match[1]
			}
		}
	}
	return bestValue
}

func (self *DefaultHTTPContext) AccessToken(dst utils.AccessToken) (result utils.AccessToken, err error) {
	if self.Req() == nil {
		err = ErrMissingToken
		return
	}
	for _, authHead := range self.Req().Header[AuthorizationHeader] {
		match := authPattern.FindStringSubmatch(authHead)
		if match != nil {
			result, err = utils.ParseAccessToken(match[1], dst)
			return
		}
	}
	if authToken := self.Req().URL.Query().Get("token"); authToken != "" {
		result, err = utils.ParseAccessToken(authToken, dst)
		return
	}
	if cookie, _ := self.Req().Cookie("token"); cookie != nil {
		result, err = utils.ParseAccessToken(cookie.Value, dst)
		return
	}
	err = ErrMissingToken
	return
}

func (self *DefaultHTTPContext) MostAccepted(name, def string) string {
	return MostAccepted(self.Req(), name, def)
}

func (self *DefaultHTTPContext) Req() *http.Request {
	return self.request
}

func (self *DefaultHTTPContext) Resp() MemorableResponseWriter {
	return self.response
}

func (self *DefaultHTTPContext) Vars() map[string]string {
	return self.vars
}

func (self *DefaultHTTPContext) CheckScopes(allowedScopes []string) (err error) {
	if len(allowedScopes) == 0 {
		return
	}
	token, err := self.AccessToken(nil)
	if err != nil {
		err = NewError(401, "Unauthorized", "", err)
		return
	}
	for _, allowedScope := range allowedScopes {
		for _, scope := range token.Scopes() {
			if scope == allowedScope {
				return
			}
		}
	}
	return NewError(401, "Unauthorized", fmt.Sprintf("Requires one of %+v, but got %+v", allowedScopes, token.Scopes()), nil)
}

func Handle(c HTTPContext, f func() error, scopes ...string) {
	err := c.CheckScopes(scopes)
	if err == nil {
		err = f()
	}
	if err != nil {
		if errResponse, ok := err.(Responder); ok {
			if err2 := errResponse.Respond(c); err2 != nil {
				c.Resp().WriteHeader(500)
				fmt.Fprintf(c.Resp(), "Unable to render the proper error %+v: %v", err, err2)
			}
		} else {
			c.Resp().WriteHeader(500)
			fmt.Fprintf(c.Resp(), "%v", err)
		}
		if c.Resp().Status() >= 500 {
			log.Printf("%v\n%v\n\n", c.Req().URL, err)
		} else {
			log.Printf("%v\n%v\n\n", c.Req().URL, err)
		}
		if stacker, ok := err.(utils.StackError); ok {
			log.Printf("%s", string(stacker.GetStack()))
		}
	}
}

func HandlerFunc(f func(c HTTPContext) error, scopes ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := NewHTTPContext(w, r)
		Handle(c, func() error {
			return f(c)
		}, scopes...)
	})
}
