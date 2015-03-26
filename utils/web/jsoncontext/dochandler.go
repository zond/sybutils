package jsoncontext

import (
	"bytes"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/soundtrackyourbrand/utils/json"

	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
)

var knownEncodings = map[reflect.Type]string{
	reflect.TypeOf(time.Time{}):      "string",
	reflect.TypeOf(time.Duration(0)): "int",
}

var knownDocTags = map[reflect.Type]string{
	reflect.TypeOf(time.Duration(0)): "Duration in nanoseconds",
	reflect.TypeOf(time.Time{}):      "Time encoded like '2013-12-12T20:52:20.963842672+01:00'",
}

var DefaultDocTemplate *template.Template

var DefaultDocTemplateContent = `
<html>
<head>
<link rel="stylesheet" href="//netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap.min.css">
<link rel="stylesheet" href="//netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap-theme.min.css">
<script src="//code.jquery.com/jquery-1.10.1.min.js"></script>
<script src="//netdna.bootstrapcdn.com/bootstrap/3.0.3/js/bootstrap.min.js"></script>
<style type="text/css">
table {
	width: 100%;
}
.spec caption {
	text-align: left;
}
.spec em {
	float: right;
}
</style>
<script>
$(document).ready(function() {
	$('.type-template').on('click', '.tab-switch', function(ev) {
		ev.preventDefault();
		var par = $(ev.target).closest('.type-template');
		par.children('.tab').addClass('hidden');
		par.children('.' + $(ev.target).attr('data-tab')).removeClass('hidden');
		par.children('ul').children('li').removeClass('active');
		par.children('ul').children('li.' + $(ev.target).attr('data-tab')).addClass('active');
	});
});
</script>
</head>
<body>
{{range .Endpoints}}
<div class="panel-group" id="accordion">
{{RenderEndpoint .}}
</div>
{{end}}
</body>
`

var DefaultTypeTemplateContent = `
<div class="type-template">
<ul class="nav nav-tabs">
<li class="active example"><a data-tab="example" class="tab-switch" href="#">Example</a></li>
<li class="spec"><a data-tab="spec" class="tab-switch" href="#">Spec</a></li>
</ul>
<pre class="example tab">
{{Example .Type}}
</pre>
<table class="spec tab table-bordered hidden">
<caption><strong>{{.Type.Type}}</strong>{{if .Type.Comment}}<em>{{.Type.Comment}}</em>{{end}}</caption>
{{if .Type.Scopes}}
<tr><td>Scopes</td><td>{{.Type.Scopes}}</td></tr>
{{end}}
{{if .Type.Elem}}
<tr><td valign="top">Element</td><td>{{RenderSubType .Type.Elem .Stack}}</td></tr>
{{end}}
{{ $stack := .Stack }}
{{range $name, $typ := .Type.Fields}}
<tr><td valign="top">{{$name}}</td><td>{{RenderSubType $typ $stack}}</td></tr>
{{end}}
</table>
</div>
`

var DefaultEndpointTemplateContent = `
<div class="panel panel-default">
  <div class="panel-heading" data-toggle="collapse" href="#collapse-{{UUID}}">
    <h4 class="panel-title">
      <a>
        {{.Methods}} {{.Path}}
      </a>
    </h4>
  </div>
  <div id="collapse-{{UUID}}" class="panel-collapse collapse">
    <div class="panel-body">
			{{if .Comment}}
			<p style="font-size: large;">{{.Comment}}</p>
			{{end}}
      <table class="table-bordered">
			<tr>
  			<td valign="top">curl</td>
				<td>
				<pre>curl{{if .In}} -H "Content-Type: application/json" {{end}}{{if .Scopes}} -H "Authorization: Bearer ${TOKEN}"{{end}} -X{{First .Methods}} ${HOST}{{.Path}}{{if .In}} -d'{{Example .In}}'{{end}}</pre>
				</td>
			</tr>
      {{if .MinAPIVersion}}
        <tr>
          <td>Minimum API version</td>
          <td>{{.MinAPIVersion}}</td>
        </tr>
      {{end}}
      {{if .MaxAPIVersion}}
        <tr>
          <td>Maximum API version</td>
          <td>{{.MaxAPIVersion}}</td>
        </tr>
      {{end}}
      {{if .Scopes}}
        <tr>
          <td>Scopes</td>
          <td>{{.Scopes}}</td>
        </tr>
      {{end}}
			{{if .In}}
			  <tr>
				  <td valign="top">JSON request body</td>
					<td>{{RenderType .In}}</td>
				</tr>
			{{end}}
			{{if .Out}}
			  <tr>
				  <td valign="top">JSON response body</td>
					<td>{{RenderType .Out}}</td>
				</tr>
			{{end}}
      </table>
    </div>
  </div>
</div>
`

func first(i interface{}) string {
	return fmt.Sprint(reflect.ValueOf(i).Index(0).Interface())
}

func init() {
	DefaultDocTemplate = template.Must(template.New("DefaultDocTemplate").Funcs(map[string]interface{}{
		"RenderEndpoint": func(r DocumentedRoute) (result string, err error) {
			return
		},
		"JSON": func(i interface{}) (result string, err error) {
			b, err := json.MarshalIndent(i, "", "  ")
			if err != nil {
				return
			}
			result = string(b)
			return
		},
		"UUID": func() string {
			return ""
		},
		"RenderSubType": func(t JSONType, stack []string) (result string, err error) {
			return
		},
		"RenderType": func(t JSONType) (result string, err error) {
			return
		},
		"Example": func(r JSONType) (result string, err error) {
			return
		},
		"First": first,
	}).Parse(DefaultDocTemplateContent))
	template.Must(DefaultDocTemplate.New("EndpointTemplate").Parse(DefaultEndpointTemplateContent))
	template.Must(DefaultDocTemplate.New("TypeTemplate").Parse(DefaultTypeTemplateContent))
	DefaultDocHandler = DocHandler(DefaultDocTemplate)
}

var DefaultDocHandler http.Handler

type DocumentedRoute interface {
	Render(*template.Template) (string, error)
	GetScopes() []string
	GetSortString() string
}

type DocumentedRoutes []DocumentedRoute

var routes = DocumentedRoutes{}

func (a DocumentedRoutes) Len() int           { return len(a) }
func (a DocumentedRoutes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DocumentedRoutes) Less(i, j int) bool { return a[i].GetSortString() < a[j].GetSortString() }

/*
JSONType handles the rendering of input and output types in the generated documentation
*/
type JSONType struct {
	In          bool
	ReflectType reflect.Type
	Type        string
	Fields      map[string]*JSONType
	Scopes      []string
	Elem        *JSONType
	Comment     string
}

func newJSONType(in bool, t reflect.Type, filterOnScopes bool, scopeContexts []string, relevantScopes ...string) (result *JSONType) {
	return newJSONTypeLoopProtector(nil, in, t, filterOnScopes, scopeContexts, relevantScopes...)
}

/*
newJSONTypeLoopProtector is used by newJSONType to ensure that we don't cause eternal recursion when creating the structures necessary to render
the type descriptions and examples
*/
func newJSONTypeLoopProtector(seen []reflect.Type, in bool, t reflect.Type, filterOnScopes bool, scopeContexts []string, relevantScopes ...string) (result *JSONType) {
	result = &JSONType{
		In:          in,
		ReflectType: t,
	}
	// if we have already seen this type, just return a dummy value
	for _, seenType := range seen {
		if t == seenType {
			result.Type = "[loop protector enabled]"
			return
		}
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		result.Type = t.Name()
		result.Fields = map[string]*JSONType{}
		// for structs, iterate over their fields
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			// fields with non-empty package paths are non-exported. skip them.
			if field.PkgPath != "" {
				continue
			}
			if field.Anonymous {
				if field.Type.Kind() == reflect.Struct || (field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct) {
					// add the fields of anonymous struct fields flat into this JSONType
					anonType := newJSONTypeLoopProtector(append(seen, t), in, field.Type, filterOnScopes, scopeContexts, relevantScopes...)
					for name, typ := range anonType.Fields {
						result.Fields[name] = typ
					}
				} else {
					// anonymous fields that aren't structs, if they are even possible, are problematic
					result.Fields[field.Name] = &JSONType{
						In:          in,
						ReflectType: field.Type,
						Type:        fmt.Sprintf("Don't know how to describe anonymous field %#v that isn't struct or pointer to struct", field.Type.Name()),
					}
				}
			} else {
				// and pick up their doc, name and encoding tags
				jsonToTag := field.Tag.Get("jsonTo")
				jsonTag := field.Tag.Get("json")
				docTag := field.Tag.Get("jsonDoc")
				name := field.Name
				updateScopes := []string{}
				// and check what scopes are allowed to update them
				if jsonTag != "-" {
					if jsonTag != "" {
						parts := strings.Split(jsonTag, ",")
						name = parts[0]
					}
					for _, context := range scopeContexts {
						updateScopesTag := field.Tag.Get(context + "_scopes")
						if updateScopesTag != "" {
							for _, updateScope := range strings.Split(updateScopesTag, ",") {
								for _, relevantScope := range relevantScopes {
									if updateScope == relevantScope {
										updateScopes = append(updateScopes, updateScope)
									}
								}
							}
						}
					}
					// fields without update scopes should never be displayed in the input type description
					if !filterOnScopes || len(updateScopes) > 0 {
						if jsonToTag == "" && knownEncodings[field.Type] != "" {
							jsonToTag = knownEncodings[field.Type]
						}
						if docTag == "" && knownDocTags[field.Type] != "" {
							docTag = knownDocTags[field.Type]
						}
						if jsonToTag != "" {
							result.Fields[name] = &JSONType{
								In:          in,
								ReflectType: field.Type,
								Type:        jsonToTag,
								Comment:     docTag,
							}
						} else {
							result.Fields[name] = newJSONTypeLoopProtector(append(seen, t), in, field.Type, filterOnScopes, scopeContexts, relevantScopes...)
							result.Fields[name].Comment = docTag
						}
						result.Fields[name].Scopes = updateScopes
					}
				}
			}
		}
	case reflect.Slice:
		result.Type = "Array"
		result.Elem = newJSONTypeLoopProtector(append(seen, t), in, t.Elem(), filterOnScopes, scopeContexts, relevantScopes...)
	default:
		result.Type = t.Name()
	}
	return
}

type DefaultDocumentedRoute struct {
	Methods       []string
	Path          string
	Scopes        []string
	MinAPIVersion int
	MaxAPIVersion int
	In            *JSONType
	Out           *JSONType
	Comment       string
}

func (self *DefaultDocumentedRoute) GetScopes() []string {
	return self.Scopes
}

func (self *DefaultDocumentedRoute) Render(templ *template.Template) (result string, err error) {
	buf := &bytes.Buffer{}
	r := utils.RandomString(10)
	if err = templ.Funcs(map[string]interface{}{
		"UUID": func() string {
			return r
		},
	}).Execute(buf, self); err != nil {
		return
	}
	result = buf.String()
	return
}

func (self *DefaultDocumentedRoute) GetSortString() string {
	return self.Path + self.Methods[0]
}

/*
Remember will record the doc and make sure it shows up in the documentation.
*/
func Remember(doc DocumentedRoute) {
	routes = append(routes, doc)
}

/*
CreateResponseFunc will take a function type and value, and return a handler function
*/
func CreateResponseFunc(fType reflect.Type, fVal reflect.Value) func(c JSONContextLogger) (response Resp, err error) {
	return func(c JSONContextLogger) (response Resp, err error) {
		// create the arguments, first of which is the context
		args := make([]reflect.Value, fType.NumIn())
		args[0] = reflect.ValueOf(c)
		// if there is a second argument then instantiate it and JSON decode into it from the request body
		if fType.NumIn() == 2 {
			if fType.In(1).Kind() == reflect.Ptr {
				in := reflect.New(fType.In(1).Elem())
				if err = c.DecodeJSON(in.Interface()); err != nil {
					mess := fmt.Sprintf("Unable to parse %#v as JSON: %v", string(c.DecodedBody()), err)
					err = NewError(400, mess, mess, err)
					return
				}
				args[1] = in
			} else {
				in := reflect.New(fType.In(1))
				if err = c.DecodeJSON(in.Interface()); err != nil {
					mess := fmt.Sprintf("Unable to parse %#v as JSON: %v", string(c.DecodedBody()), err)
					err = NewError(400, mess, mess, err)
					return
				}
				args[1] = in.Elem()
			}
		}
		// then run the handler function
		results := fVal.Call(args)
		// if there was en error, then return it
		if !results[len(results)-1].IsNil() {
			err = results[len(results)-1].Interface().(error)
			return
		}
		// if there was a status returned, use it
		if status := int(results[0].Int()); status != 0 {
			response.Status = status
		}
		// if there was a result instance returned by the handler, put it in the body for later encoding
		if len(results) == 3 && (results[1].Kind() != reflect.Ptr || !results[1].IsNil()) {
			response.Body = results[1].Interface()
		}
		return
	}
}

var fNameReg = regexp.MustCompile("^(.*)\\.([^.]+)$")

/*
Document will take a func, a path, a set of methods (separated by |) and a set of scopes that will be used when updating models in the func, and return a documented route and a function suitable for HandlerFunc.

The input func must match func(context JSONContextLogger) (status int, err error)

One extra input argument after context is allowed, and will be JSON decoded from the request body, and used in the documentation struct.

One extra return value between status and error is allowed, and will be JSON encoded to the response body, and used in the documentation struct.
*/
func Document(fIn interface{}, path string, methods string, minAPIVersion, maxAPIVersion int, scopes ...string) (docRoute *DefaultDocumentedRoute, fOut func(JSONContextLogger) (Resp, error)) {
	// first validate that the handler takes either a context, or a context and a decoded JSON body as argument
	if errs := utils.ValidateFuncInputs(fIn, []reflect.Type{
		reflect.TypeOf((*JSONContextLogger)(nil)).Elem(),
		reflect.TypeOf((*interface{})(nil)).Elem(),
	}, []reflect.Type{
		reflect.TypeOf((*JSONContextLogger)(nil)).Elem(),
	}); len(errs) == 2 {
		panic(fmt.Errorf("%v does not conform. Fix one of %+v", runtime.FuncForPC(reflect.ValueOf(fIn).Pointer()).Name(), errs))
	}
	// then validate that it returns either status, responde body object for JSON encoding and error, or just status and error
	if errs := utils.ValidateFuncOutputs(fIn, []reflect.Type{
		reflect.TypeOf(0),
		reflect.TypeOf((*interface{})(nil)).Elem(),
		reflect.TypeOf((*error)(nil)).Elem(),
	}, []reflect.Type{
		reflect.TypeOf(0),
		reflect.TypeOf((*error)(nil)).Elem(),
	}); len(errs) == 2 {
		panic(fmt.Errorf("%v does not conform. Fix one of %+v", runtime.FuncForPC(reflect.ValueOf(fIn).Pointer()).Name(), errs))
	}

	// then create the route object that we store for documentation purposes
	methodNames := strings.Split(methods, "|")
	docRoute = &DefaultDocumentedRoute{
		Path:          path,
		Methods:       methodNames,
		MinAPIVersion: minAPIVersion,
		MaxAPIVersion: maxAPIVersion,
		Scopes:        scopes,
	}
	// calculate the name of the function
	fName := runtime.FuncForPC(reflect.ValueOf(fIn).Pointer()).Name()
	if match := fNameReg.FindStringSubmatch(fName); match != nil {
		docRoute.Comment = fmt.Sprintf("%#v", match)
	}
	fVal := reflect.ValueOf(fIn)
	fType := fVal.Type()
	// if the handler takes two arguments (that is, one decoded JSON body), add an input param type to document with.
	// also send in the scopes, because the input type fields must be filtered so that those without scopes are ignored
	if fType.NumIn() == 2 {
		docRoute.In = newJSONType(true, fType.In(1), true, methodNames, scopes...)
	}
	// if the handler provides three return values (that is, one decoded JSON body), add an output param type to document with.
	if fType.NumOut() == 3 {
		docRoute.Out = newJSONType(false, fType.Out(1), false, methodNames)
	}

	fOut = CreateResponseFunc(fType, fVal)
	return
}

/*
DocHandler will return a handler that renders the documentation for all routes registerd with DocHandle.

The resulting func will do this by going through each route in DocumentedRoutes and render the endpoint
using the provided template, providing it template functions to render separate endpoints, types, sub types
and examples of types.
*/
func DocHandler(templ *template.Template) http.Handler {
	return httpcontext.HandlerFunc(func(c httpcontext.HTTPContextLogger) (err error) {
		c.Resp().Header().Set("Content-Type", "text/html; charset=UTF-8")
		// we define a func to render a type
		// it basically just executes the "TypeTemplate" with the provided
		// stack to avoid infinite recursion
		renderType := func(t JSONType, stack []string) (result string, err error) {
			// if the type is already mentioned in one of the parents we have already mentioned,
			// bail
			for _, parent := range stack {
				if parent != "" && parent == t.ReflectType.Name() {
					result = fmt.Sprintf("[loop protector enabled, render stack: %v]", stack)
					return
				}
			}
			stack = append(stack, t.ReflectType.Name())
			buf := &bytes.Buffer{}
			// then execute the TypeTemplate with this type and this stack
			if err = templ.ExecuteTemplate(buf, "TypeTemplate", map[string]interface{}{
				"Type":  t,
				"Stack": stack,
			}); err != nil {
				return
			}
			result = buf.String()
			return
		}

		// routes are documented alphabetically
		sort.Sort(routes)
		// define all the functions that we left empty earlier
		err = templ.Funcs(map[string]interface{}{
			"RenderEndpoint": func(r DocumentedRoute) (string, error) {
				return r.Render(templ.Lookup("EndpointTemplate"))
			},
			"RenderSubType": func(t JSONType, stack []string) (result string, err error) {
				return renderType(t, stack)
			},
			"RenderType": func(t JSONType) (result string, err error) {
				return renderType(t, nil)
			},
			"First": first,
			"Example": func(r JSONType) (result string, err error) {
				// this will render an example of the provided JSONType
				defer func() {
					if e := recover(); e != nil {
						result = fmt.Sprintf("%v\n%s", e, utils.Stack())
					}
				}()
				x := utils.Example(r.ReflectType)
				b, err := json.MarshalIndent(x, "", "  ")
				if err != nil {
					return
				}
				if len(r.Fields) > 0 {
					var i interface{}
					if err = json.Unmarshal(b, &i); err != nil {
						return
					}
					if m, ok := i.(map[string]interface{}); ok {
						newMap := map[string]interface{}{}
						for k, v := range m {
							if _, found := r.Fields[k]; found {
								newMap[k] = v
							}
						}
						if b, err = json.MarshalIndent(newMap, "", "  "); err != nil {
							return
						}
					}
				}
				result = string(b)
				return
			},
		}).Execute(c.Resp(), map[string]interface{}{
			"Endpoints": routes,
		})
		return
	})
}

/*
DocHandle will register f as handler for path, method, api versions and scopes in router.

It will also reflectively go through the parameters and return values of f, and register those in
the DocumentedRoutes variable.
*/
func DocHandle(router *mux.Router, f interface{}, path string, method string, minAPIVersion, maxAPIVersion int, scopes ...string) {
	doc, fu := Document(f, path, method, minAPIVersion, maxAPIVersion, scopes...)
	Remember(doc)
	methods := strings.Split(method, "|")
	router.Path(path).Methods(methods...).MatcherFunc(APIVersionMatcher(minAPIVersion, maxAPIVersion)).Handler(HandlerFunc(fu, minAPIVersion, maxAPIVersion, scopes...))
}
