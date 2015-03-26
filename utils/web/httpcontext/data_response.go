package httpcontext

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"regexp"

	"github.com/soundtrackyourbrand/utils/json"
)

const (
	ContentJSON       = "application/json; charset=UTF-8"
	ContentJSONStream = "application/x-json-stream; charset=UTF-8"
	ContentExcelCSV   = "application/vnd.ms-excel"
	ContentHTML       = "text/html"
)

type DataResp struct {
	Data        chan []interface{}
	Headers     []string
	Status      int
	ContentType string
	Filename    string
	ReportName  string
	Filters     map[string][]string
}

func (self DataResp) Render(c HTTPContextLogger) error {
	if self.Data == nil {
		return nil
	}
	if self.Filename != "" {
		c.Resp().Header().Set("Content-disposition", "attachment; filename="+self.Filename)
	}
	c.Resp().Header().Set("Content-Type", self.ContentType)
	switch self.ContentType {
	case ContentExcelCSV:
		if self.Status != 0 {
			c.Resp().WriteHeader(self.Status)
		}
		fmt.Fprintf(c.Resp(), "sep=\t\n")
		writer := csv.NewWriter(c.Resp())
		writer.Comma = '\t'
		titleRow := []string{self.ReportName}
		for k, v := range self.Filters {
			titleRow = append(titleRow, fmt.Sprintf("%s=%s", k, v[0]))
		}
		writer.Write(titleRow)
		err := writer.Write(self.Headers)
		if err != nil {
			return err
		}
		for row := range self.Data {
			vals := make([]string, 0, len(self.Headers))
			for index := range self.Headers {
				switch row[index].(type) {
				default:
					vals = append(vals, fmt.Sprintf("%v", row[index]))
				case float64:
					vals = append(vals, fmt.Sprintf("%.2f", row[index]))
				}
			}
			err := writer.Write(vals)
			if err != nil {
				return err
			}
		}
		writer.Flush()
		return writer.Error()
	case ContentHTML:
		fmt.Fprintf(c.Resp(), "<html><body><table><thead><tr>")
		for _, k := range self.Headers {
			fmt.Fprintf(c.Resp(), "<th>%v</th>", k)
		}
		fmt.Fprintf(c.Resp(), "</tr></thead><tbody>")
		for row := range self.Data {
			fmt.Fprintf(c.Resp(), "<tr>")
			for _, v := range row {
				switch v.(type) {
				default:
					fmt.Fprintf(c.Resp(), "<td>%v</td>", v)
				case float64:
					fmt.Fprintf(c.Resp(), "<td>%.2f</td>", v)
				}
			}
			fmt.Fprintf(c.Resp(), "</tr>")
		}
		fmt.Fprintf(c.Resp(), "</tbody></body></html>")
	case ContentJSON:
		// I dont know a way of creating json, and streaming it to the user.
		var resp []map[string]interface{}
		for row := range self.Data {
			m := map[string]interface{}{}
			for k, v := range self.Headers {
				m[v] = row[k]
			}
			resp = append(resp, m)
		}
		return json.NewEncoder(c.Resp()).Encode(resp)

	case ContentJSONStream:
		for row := range self.Data {
			m := map[string]interface{}{}
			for k, v := range self.Headers {
				m[v] = row[k]
			}
			err := json.NewEncoder(c.Resp()).Encode(m)
			if err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("Unknown content type %#v", self.ContentType)
}

var suffixPattern = regexp.MustCompile("\\.(\\w{1,6})$")

func DataHandle(c HTTPContextLogger, f func() (*DataResp, error), scopes ...string) {
	Handle(c, func() (err error) {
		resp, err := f()
		if err != nil {
			return
		}
		match := suffixPattern.FindStringSubmatch(c.Req().URL.Path)
		suffix := ""
		if match != nil {
			suffix = match[1]
		}
		switch suffix {
		case "csv":
			resp.ContentType = ContentExcelCSV
		case "html":
			resp.ContentType = ContentHTML
		case "jjson":
			resp.ContentType = ContentJSONStream
		default:
			resp.ContentType = ContentJSON
		}
		if err == nil {
			err = resp.Render(c)
		}
		return
	}, scopes...)
}

func DataHandlerFunc(f func(c HTTPContextLogger) (result *DataResp, err error), scopes ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := NewHTTPContext(w, r)
		DataHandle(c, func() (*DataResp, error) {
			return f(c)
		}, scopes...)
	})
}
