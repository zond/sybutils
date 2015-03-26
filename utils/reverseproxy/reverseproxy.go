// stolen and modified from go src
package reverseproxy

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"errors"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"appengine"
	"appengine_internal"
	"code.google.com/p/goprotobuf/proto"

	pb "appengine_internal/urlfetch"

	"log"
	"net"
	"sync"
)

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

func NewSingleHostReverseProxy(target *url.URL, proxyRootPath string) *ReverseProxy {
	targetQuery := target.RawQuery
	director := func(req *http.Request) {
		req.URL.Path = req.URL.Path[len(proxyRootPath):]
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.URL.Path = singleJoiningSlash(target.Path, req.URL.Path)
		if targetQuery == "" || req.URL.RawQuery == "" {
			req.URL.RawQuery = targetQuery + req.URL.RawQuery
		} else {
			req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		}
	}
	return &ReverseProxy{Director: director}
}

func (t *Transport) RoundTrip(req *http.Request) (res *http.Response, err error) {
	methNum, ok := pb.URLFetchRequest_RequestMethod_value[req.Method]
	if !ok {
		return nil, fmt.Errorf("urlfetch: unsupported HTTP method %q", req.Method)
	}

	method := pb.URLFetchRequest_RequestMethod(methNum)

	freq := &pb.URLFetchRequest{
		Method:                        &method,
		Url:                           proto.String(urlString(req.URL)),
		FollowRedirects:               proto.Bool(false), // http.Client's responsibility
		MustValidateServerCertificate: proto.Bool(!t.AllowInvalidServerCertificate),
	}
	opts := &appengine_internal.CallOptions{}

	if t.Deadline != 0 {
		freq.Deadline = proto.Float64(t.Deadline.Seconds())
		opts.Timeout = t.Deadline
	}

	for k, vals := range req.Header {
		for _, val := range vals {
			freq.Header = append(freq.Header, &pb.URLFetchRequest_Header{
				Key:   proto.String(k),
				Value: proto.String(val),
			})
		}
	}
	if methodAcceptsRequestBody[req.Method] && req.Body != nil {
		freq.Payload, err = ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
	}

	fres := &pb.URLFetchResponse{}
	if err := t.Context.Call("urlfetch", "Fetch", freq, fres, opts); err != nil {
		return nil, err
	}

	res = &http.Response{}
	res.StatusCode = int(*fres.StatusCode)
	res.Status = fmt.Sprintf("%d %s", res.StatusCode, statusCodeToText(res.StatusCode))
	res.Header = make(http.Header)
	res.Request = req

	// Faked:
	res.ProtoMajor = 1
	res.ProtoMinor = 1
	res.Proto = "HTTP/1.1"
	res.Close = true

	for _, h := range fres.Header {
		hkey := http.CanonicalHeaderKey(*h.Key)
		hval := *h.Value
		if hkey == "Content-Length" {
			// Will get filled in below for all but HEAD requests.
			if req.Method == "HEAD" {
				res.ContentLength, _ = strconv.ParseInt(hval, 10, 64)
			}
			continue
		}

		res.Header.Add(hkey, hval)

	}

	if req.Method != "HEAD" {
		res.ContentLength = int64(len(fres.Content))
	}

	truncated := fres.GetContentWasTruncated()
	res.Body = &bodyReader{content: fres.Content, truncated: truncated}
	return
}

// Copyright 2011 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package urlfetch provides an http.RoundTripper implementation
// for fetching URLs via App Engine's urlfetch service.

// Transport is an implementation of http.RoundTripper for
// App Engine. Users should generally create an http.Client using
// this transport and use the Client rather than using this transport
// directly.
type Transport struct {
	Context  appengine.Context
	Deadline time.Duration // zero means 5-second default

	// Controls whether the application checks the validity of SSL certificates
	// over HTTPS connections. A value of false (the default) instructs the
	// application to send a request to the server only if the certificate is
	// valid and signed by a trusted certificate authority (CA), and also
	// includes a hostname that matches the certificate. A value of true
	// instructs the application to perform no certificate validation.
	AllowInvalidServerCertificate bool
}

// Client returns an *http.Client using a default urlfetch Transport. This
// client will have the default deadline of 5 seconds, and will check the
// validity of SSL certificates.
func Client(context appengine.Context) *http.Client {
	return &http.Client{
		Transport: &Transport{
			Context: context,
		},
	}
}

type bodyReader struct {
	content   []byte
	truncated bool
	closed    bool
}

// ErrTruncatedBody is the error returned after the final Read() from a
// response's Body if the body has been truncated by App Engine's proxy.
var ErrTruncatedBody = errors.New("urlfetch: truncated body")

func statusCodeToText(code int) string {
	if t := http.StatusText(code); t != "" {
		return t
	}
	return strconv.Itoa(code)
}

func (br *bodyReader) Read(p []byte) (n int, err error) {
	if br.closed {
		if br.truncated {
			return 0, ErrTruncatedBody
		}
		return 0, io.EOF
	}
	n = copy(p, br.content)
	if n > 0 {
		br.content = br.content[n:]
		return
	}
	if br.truncated {
		br.closed = true
		return 0, ErrTruncatedBody
	}
	return 0, io.EOF
}

func (br *bodyReader) Close() error {
	br.closed = true
	br.content = nil
	return nil
}

// A map of the URL Fetch-accepted methods that take a request body.
var methodAcceptsRequestBody = map[string]bool{
	"POST":  true,
	"PUT":   true,
	"PATCH": true,
}

// urlString returns a valid string given a URL. This function is necessary because
// the String method of URL doesn't correctly handle URLs with non-empty Opaque values.
// See http://code.google.com/p/go/issues/detail?id=4860.
func urlString(u *url.URL) string {
	if u.Opaque == "" || strings.HasPrefix(u.Opaque, "//") {
		return u.String()
	}
	aux := *u
	aux.Opaque = "//" + aux.Host + aux.Opaque
	return aux.String()
}

func init() {
	appengine_internal.RegisterErrorCodeMap("urlfetch", pb.URLFetchServiceError_ErrorCode_name)
	appengine_internal.RegisterTimeoutErrorCode("urlfetch", int32(pb.URLFetchServiceError_DEADLINE_EXCEEDED))
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// Hop-by-hop headers. These are removed when sent to the backend.
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html
var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te", // canonicalized version of "TE"
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

func (p *ReverseProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	transport := p.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	outreq := new(http.Request)
	*outreq = *req // includes shallow copies of maps, but okay

	p.Director(outreq)
	outreq.Proto = "HTTP/1.1"
	outreq.ProtoMajor = 1
	outreq.ProtoMinor = 1
	outreq.Close = false

	// Remove hop-by-hop headers to the backend.  Especially
	// important is "Connection" because we want a persistent
	// connection, regardless of what the client sent to us.  This
	// is modifying the same underlying map from req (shallow
	// copied above) so we only copy it if necessary.
	copiedHeaders := false
	for _, h := range hopHeaders {
		if outreq.Header.Get(h) != "" {
			if !copiedHeaders {
				outreq.Header = make(http.Header)
				copyHeader(outreq.Header, req.Header)
				copiedHeaders = true
			}
			outreq.Header.Del(h)
		}
	}

	if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		// If we aren't the first proxy retain prior
		// X-Forwarded-For information as a comma+space
		// separated list and fold multiple headers into one.
		if prior, ok := outreq.Header["X-Forwarded-For"]; ok {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		outreq.Header.Set("X-Forwarded-For", clientIP)
	}

	res, err := transport.RoundTrip(outreq)
	if err != nil {
		log.Printf("http: proxy error: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer res.Body.Close()

	//res.Header = nil
	copyHeader(rw.Header(), res.Header)

	// strip hop-by-hop headers for the response
	for _, h := range hopHeaders {
		if rw.Header().Get(h) != "" {
			rw.Header().Del(h)
		}
	}

	rw.WriteHeader(res.StatusCode)
	p.copyResponse(rw, res.Body)
}

func (p *ReverseProxy) copyResponse(dst io.Writer, src io.Reader) {
	if p.FlushInterval != 0 {
		if wf, ok := dst.(writeFlusher); ok {
			mlw := &maxLatencyWriter{
				dst:     wf,
				latency: p.FlushInterval,
				done:    make(chan bool),
			}
			go mlw.flushLoop()
			defer mlw.stop()
			dst = mlw
		}
	}

	io.Copy(dst, src)
}

type writeFlusher interface {
	io.Writer
	http.Flusher
}

type maxLatencyWriter struct {
	dst     writeFlusher
	latency time.Duration

	lk   sync.Mutex // protects Write + Flush
	done chan bool
}

func (m *maxLatencyWriter) Write(p []byte) (int, error) {
	m.lk.Lock()
	defer m.lk.Unlock()
	return m.dst.Write(p)
}

func (m *maxLatencyWriter) flushLoop() {
	t := time.NewTicker(m.latency)
	defer t.Stop()
	for {
		select {
		case <-m.done:
			if onExitFlushLoop != nil {
				onExitFlushLoop()
			}
			return
		case <-t.C:
			m.lk.Lock()
			m.dst.Flush()
			m.lk.Unlock()
		}
	}
}

func (m *maxLatencyWriter) stop() { m.done <- true }

// onExitFlushLoop is a callback set by tests to detect the state of the
// flushLoop() goroutine.
var onExitFlushLoop func()

// ReverseProxy is an HTTP Handler that takes an incoming request and
// sends it to another server, proxying the response back to the
// client.
type ReverseProxy struct {
	// Director must be a function which modifies
	// the request into a new request to be sent
	// using Transport. Its response is then copied
	// back to the original client unmodified.
	Director func(*http.Request)

	// The transport used to perform proxy requests.
	// If nil, http.DefaultTransport is used.
	Transport http.RoundTripper

	// FlushInterval specifies the flush interval
	// to flush to the client while copying the
	// response body.
	// If zero, no periodic flushing is done.
	FlushInterval time.Duration
}
