package email

import (
	"github.com/soundtrackyourbrand/utils/key"
)

type Attachment struct {
	ContentID string
	Name      string
	Data      []byte
}

type MailType string

type EmailParameters struct {
	To          string
	Cc          string
	Bcc         string
	Sender      string
	Attachments []Attachment
	Locale      string
	MailContext map[string]interface{}
}

type Filterer interface {
	Filter(mailType MailType) bool
}

type EmailTemplateSender interface {
	SendEmailTemplate(mailType MailType, f func() (ep *EmailParameters, err error), accountId key.Key, filterer Filterer) error
}
