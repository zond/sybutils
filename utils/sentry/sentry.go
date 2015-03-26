package sentry

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/soundtrackyourbrand/utils"

	"bytes"
	"encoding/json"
)

type Sentry struct {
	projectId  string
	url        string
	authHeader string
	client     *http.Client
}

type Severity string

const (
	// Accepted severity levels by Sentry
	DEBUG   Severity = "debug"
	INFO             = "info"
	WARNING          = "warning"
	ERROR            = "error"
	FATAL            = "fatal"
)

type Packet struct {
	EventId    string      `json:"event_id"`  // Unique id, max 32 characters
	Timestamp  time.Time   `json:"timestamp"` // Sentry assumes it is given in UTC. Use the ISO 8601 format
	Message    string      `json:"message"`   // Human-readable message, max length 1000 characters
	Level      Severity    `json:"level"`     // Defaults to "error"
	Logger     string      `json:"logger"`    // Defaults to "root"
	Culprit    string      `json:"culprit"`   // Becomes main name in Sentry
	ServerName string      `json:"server_name"`
	Tags       interface{} `json:"tags,omitempty"` // Additional optional tags
}

type Error struct {
	Dsn    string
	Packet *Packet
}

/*
Sends error to Sentry
*/
func SendError(client *http.Client, serr *Error) (err error) {
	sentry, err := newSentry(client, serr.Dsn)
	if err != nil {
		return
	}

	p := serr.Packet
	if err = p.init(); err != nil {
		return
	}
	if err = sentry.send(p); err != nil {
		return
	}

	return
}

func newSentry(client *http.Client, dsn string) (sentry *Sentry, err error) {
	if dsn == "" {
		return
	}

	sentry = &Sentry{}
	sentry.client = client

	uri, err := url.Parse(dsn)
	if err != nil {
		return
	}
	if uri.User == nil {
		err = utils.Errorf("Sentry: dsn missing user")
		return
	}
	publicKey := uri.User.Username()
	secretKey, found := uri.User.Password()
	if !found {
		utils.Errorf("Sentry: dsn missing secret")
		return
	}

	if idx := strings.LastIndex(uri.Path, "/"); idx != -1 {
		sentry.projectId = uri.Path[idx+1:]
		uri.Path = uri.Path[:idx+1] + "api/" + sentry.projectId + "/store/"
	}
	if sentry.projectId == "" {
		err = utils.Errorf("Sentry: dsn missing project id")
		return
	}

	sentry.url = fmt.Sprintf("https://app.getsentry.com/api/%v/store/", sentry.projectId)
	sentry.authHeader = fmt.Sprintf("Sentry sentry_version=4, sentry_key=%s, sentry_secret=%s", publicKey, secretKey)

	return
}

type SentryRateLimitError string

func (self SentryRateLimitError) Error() string {
	return string(self)
}

func (self *Sentry) send(p *Packet) (err error) {
	if p == nil { // Nothing to send
		return
	}

	b, err := json.Marshal(p)
	if err != nil {
		return
	}
	buf := bytes.NewBufferString(strings.TrimSpace(string(b)))

	request, _ := http.NewRequest("POST", self.url, buf)
	request.Header.Set("X-Sentry-Auth", self.authHeader)
	request.Header.Set("Content-Type", "application/json")

	curl := utils.ToCurl(request)

	response, err := self.client.Do(request)
	if err != nil {
		err = utils.Errorf("Sentry: tried \n%v\nerr: %v", curl, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		if response.StatusCode == 429 {
			err = SentryRateLimitError(fmt.Sprintf("%+v", response))
		} else {
			err = utils.Errorf("Sentry: did \n%v\n and received response:\n%v", curl, utils.Prettify(response))
		}
	}

	return
}

func (self *Packet) init() (err error) {
	if self.EventId, err = uuid(); err != nil {
		return
	}
	if self.Level == "" {
		self.Level = ERROR
	}
	if self.Level != DEBUG &&
		self.Level != INFO &&
		self.Level != WARNING &&
		self.Level != ERROR &&
		self.Level != FATAL {
		return utils.Errorf("Sentry: packet.Level value not valid")
	}
	if self.Message == "" {
		return utils.Errorf("Sentry: packet.Message missing")
	}
	if self.Logger == "" {
		self.Logger = "golang"
	}
	self.Timestamp = time.Now()
	return
}

func uuid() (string, error) {
	id := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, id)
	if err != nil {
		return "", err
	}
	id[6] &= 0x0F // clear version
	id[6] |= 0x40 // set version to 4 (random uuid)
	id[8] &= 0x3F // clear variant
	id[8] |= 0x80 // set to IETF variant
	return hex.EncodeToString(id), nil
}
