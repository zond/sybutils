package elasticsearch

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/soundtrackyourbrand/utils/json"

	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/key"
)

type ElasticConnector interface {
	Client() *http.Client
	GetElasticService() string
	GetElasticUsername() string
	GetElasticPassword() string
}

type ElasticSearchContext interface {
	ElasticConnector
	Debugf(format string, args ...interface{})
}

var UpdateConflictRetries = 10

var IndexNameProcessor = func(s string) string {
	return s
}

var legalizeRegexp = regexp.MustCompile("[^a-z0-9,]")

func processIndexName(s string) string {
	return IndexNameProcessor(legalizeRegexp.ReplaceAllString(strings.ToLower(s), ""))
}

type IndexOption string

const (
	AnalyzedIndex    IndexOption = "analyzed"
	NotAnalyzedIndex IndexOption = "not_analyzed"
	NoIndex          IndexOption = "no"
)

type Properties struct {
	Type     string                `json:"type"`
	Index    IndexOption           `json:"index,omitempty"`
	Store    bool                  `json:"store"`
	Fields   map[string]Properties `json:"fields,omitempty"`
	Analyzer string                `json:"analyzer,omitempty"`
}

type DynamicTemplate struct {
	Match            string      `json:"match"`
	MatchMappingType string      `json:"match_mapping_type"`
	Mapping          *Properties `json:"mapping,omitempty"`
}

type Mapping struct {
	DynamicTemplates []map[string]DynamicTemplate `json:"dynamic_templates,omitempty"`
	Properties       map[string]Properties        `json:"properties,omitempty"`
}

type IndexDef struct {
	Settings Settings           `json:"settings,omitempty"`
	Mappings map[string]Mapping `json:"mappings,omitempty"`
	Template string             `json:"template,omitempty"`
}

type Settings struct {
	Analysis Analyzers `json:"analysis,omitempty"`
}

type Analyzers struct {
	Analyzers map[string]Analyzer `json:"analyzer,omitempty"`
}

type Analyzer struct {
	Tokenizer string   `json:"tokenizer"`
	Filter    []string `json:"filter"`
}

func CreateIndex(c ElasticConnector, name string, def IndexDef) (err error) {
	return createIndexDef(c, "/"+processIndexName(name), def)
}

func createIndexDef(c ElasticConnector, path string, def interface{}) (err error) {
	url := c.GetElasticService() + path
	b, err := json.Marshal(def)
	if err != nil {
		return
	}
	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(b))
	if err != nil {
		return
	}
	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status trying to create index template in elasticsearch %v: %v, body: %v", url, response.Status, string(b))
		return
	}
	return
}

func CreateIndexTemplate(c ElasticConnector, name string, def IndexDef) (err error) {
	return createIndexDef(c, "/_template/"+name, def)
}

/*
Clear will delete things.
If toDelete is empty, EVERYTHING will be deleted.
If toDelete has one element, that index will be deleted.
If toDelete has two elements, that index and doc type will be deleted.
*/
func Clear(c ElasticConnector, toDelete ...string) (err error) {
	url := c.GetElasticService()
	if len(toDelete) > 2 {
		err = fmt.Errorf("Can only give at most 2 string args to Clear")
		return
	} else if len(toDelete) == 2 {
		url += fmt.Sprintf("/%v/%v", processIndexName(toDelete[0]), toDelete[1])
	} else if len(toDelete) == 1 {
		url += fmt.Sprintf("/%v", processIndexName(toDelete[0]))
	} else {
		url += "/_all"
	}

	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return
	}
	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status trying to delete from elasticsearch %v: %v", url, response.Status)
		return
	}
	return
}

/*
CreateDynamicMapping will create a sane default dynamic mapping where all
string type fields are indexed twice, once analyzed under their proper name,
and once non-analyzed under [name].na
*/
func CreateDynamicMapping(c ElasticConnector) (err error) {
	indexDef := IndexDef{
		Template: "*",

		Settings: Settings{
			Analysis: Analyzers{
				Analyzers: map[string]Analyzer{
					"lower_case": Analyzer{
						Tokenizer: "keyword",
						Filter:    []string{"lowercase"},
					},
				},
			},
		},

		Mappings: map[string]Mapping{
			"_default_": Mapping{
				DynamicTemplates: []map[string]DynamicTemplate{
					map[string]DynamicTemplate{
						"default": DynamicTemplate{
							Match:            "*",
							MatchMappingType: "string",
							Mapping: &Properties{
								Type: "multi_field",
								Fields: map[string]Properties{
									"{name}": Properties{
										Index: AnalyzedIndex,
										Type:  "string",
										Store: false,
									},
									"na": Properties{
										Index: NotAnalyzedIndex,
										Type:  "string",
										Store: false,
									},
									"lower_case": Properties{
										Index:    AnalyzedIndex,
										Type:     "string",
										Store:    false,
										Analyzer: "lower_case",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	if err = CreateIndexTemplate(c, "default", indexDef); err != nil {
		return
	}
	return
}

func RemoveFromIndex(c ElasticConnector, index string, source interface{}) (err error) {
	index = processIndexName(index)
	value := reflect.ValueOf(source)
	id := value.Elem().FieldByName("Id").Interface().(key.Key).Encode()

	name := value.Elem().Type().Name()
	url := fmt.Sprintf("%s/%s/%s/%s",
		c.GetElasticService(),
		index,
		name,
		id)

	json, err := json.Marshal(source)
	if err != nil {
		return
	}
	request, err := http.NewRequest("DELETE", url, bytes.NewBuffer(json))
	if err != nil {
		return
	}

	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status code from elasticsearch %v: %v", url, response.Status)
		return
	}
	return
}

type UpdateRequest struct {
	Script string                 `json:"script"`
	Params map[string]interface{} `json:"params"`
}

func UpdateDoc(c ElasticConnector, index string, id key.Key, groovyCode string, params map[string]interface{}) (err error) {
	index = processIndexName(index)

	url := fmt.Sprintf("%s/%s/%s/%s/_update?retry_on_conflict=%v",
		c.GetElasticService(),
		index,
		id.Kind(),
		id.Encode(),
		UpdateConflictRetries)

	json, err := json.Marshal(UpdateRequest{
		Script: groovyCode,
		Params: params,
	})
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(json))
	if err != nil {
		return
	}

	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(response.Body)
		err = fmt.Errorf("Bad status code from elasticsearch %v: %v, %v", url, response.Status, string(body))
		return
	}
	return
}

/*
AddToIndex adds source to a search index.
Source must have a field `Id *datastore.key`.
*/
func AddToIndex(c ElasticConnector, index string, source interface{}) (err error) {
	sourceVal := reflect.ValueOf(source)
	if sourceVal.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer", source)
		return
	}
	if sourceVal.Elem().Kind() != reflect.Struct {
		err = fmt.Errorf("%#v is not a pointer to a struct", source)
		return
	}
	index = processIndexName(index)

	value := reflect.ValueOf(source).Elem()
	id := value.FieldByName("Id").Interface().(key.Key).Encode()

	name := value.Type().Name()

	json, err := json.Marshal(source)
	if err != nil {
		return
	}

	url := fmt.Sprintf("%s/%s/%s/%s",
		c.GetElasticService(),
		index,
		name,
		id)

	updatedAtField := value.FieldByName("UpdatedAt")
	if updatedAtField.IsValid() {
		updatedAtUnixNano := updatedAtField.MethodByName("UnixNano")
		if updatedAtUnixNano.IsValid() {
			if unixNano, ok := updatedAtUnixNano.Call(nil)[0].Interface().(int64); ok && unixNano > 0 {
				url = fmt.Sprintf("%v?version_type=external_gte&version=%v", url, updatedAtUnixNano.Call(nil)[0].Interface())
			}
		}
	}

	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(json))
	if err != nil {
		return
	}

	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}
	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK && response.StatusCode != http.StatusConflict {
		body, _ := ioutil.ReadAll(response.Body)
		err = fmt.Errorf("Bad status code from elasticsearch %v: %v, %v", url, response.Status, string(body))
		return
	}
	return
}

type PageableItems struct {
	Items   []interface{} `json:"items"`
	Total   int           `json:"total"`
	Page    int           `json:"page"`
	PerPage int           `json:"per_page"`
}

type SimpleStringQuery StringQuery

type StringQuery struct {
	Query           string `json:"query"`
	AnalyzeWildcard bool   `json:"analyze_wildcard"`
	DefaultField    string `json:"default_field"`
}

type Query struct {
	String       *StringQuery           `json:"query_string,omitempty"`
	SimpleString *SimpleStringQuery     `json:"simple_query_string,omitempty"`
	Term         map[string]string      `json:"term,omitempty"`
	Range        map[string]RangeDef    `json:"range,omitempty"`
	Bool         *BoolQuery             `json:"bool,omitempty"`
	Filtered     *FilteredQuery         `json:"filtered,omitempty"`
	MatchAll     *MatchAllQuery         `json:"match_all,omitempty"`
	Ids          map[string]interface{} `json:"ids,omitempty"`
}

type MatchAllQuery struct {
	Boost float64 `json:"boost,omitempty"`
}

type SearchRequest struct {
	Query  *Query                  `json:"query,omitempty"`
	From   int                     `json:"from,omitempty"`
	Size   int                     `json:"size,omitempty"`
	Sort   []map[string]Sort       `json:"sort,omitempty"`
	Facets map[string]FacetRequest `json:"facets,omitempty"`
	Aggs   map[string]AggRequest   `json:"aggs,omitempty"`
}

type ValueCountAggRequest struct {
	Field string `json:"field"`
}

type TermsAggRequest struct {
	Field string `json:"field"`
}

type CardinalityAggRequest struct {
	Field string `json:"field"`
}

type AggRequest struct {
	ValueCount  *ValueCountAggRequest  `json:"value_count,omitempty"`
	Cardinality *CardinalityAggRequest `json:"cardinality,omitempty"`
	Terms       *TermsAggRequest       `json:"terms,omitempty"`
	Aggs        map[string]AggRequest  `json:"aggs,omitempty"`
	Filter      *Filter                `json:"filter,omitempty"`
}

type FacetRequest struct {
	Terms *TermsFacetRequest `json:"terms,omitempty"`
}

type TermsFacetRequest struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

type Sort struct {
	Order          string `json:"order"`
	Missing        string `json:"missing,omitempty"`
	IgnoreUnmapped bool   `json:"ignore_unmapped"`
}

type Sources []map[string]*json.RawMessage

type ElasticDoc struct {
	Index  string                      `json:"_index"`
	Type   string                      `json:"_type"`
	Id     string                      `json:"_id"`
	Score  float64                     `json:"_score"`
	Source map[string]*json.RawMessage `json:"_source"`
}

type Hits struct {
	Total    int          `json:"total"`
	MaxScore float64      `json:"max_score"`
	Hits     []ElasticDoc `json:"hits"`
}

type AggregationResult struct {
	Value     int                        `json:"value,omitempty"`
	DocCount  int                        `json:"doc_count,omitempty"`
	TermCount AggregationTermCountResult `json:"termcount,omitempty"`
}

type AggregationTermCountResult struct {
	Buckets []AggregationTermCountBucket `json:"buckets"`
}

type AggregationTermCountBucket struct {
	DocCount int    `json:"doc_count"`
	Key      string `json:"key"`
}

type SearchResponse struct {
	Took         float64                      `json:"took"`
	Hits         Hits                         `json:"hits"`
	Facets       map[string]FacetResponse     `json:"facets,omitempty"`
	Page         int                          `json:"page"`
	PerPage      int                          `json:"per_page"`
	Aggregations map[string]AggregationResult `json:"aggregations,omitempty"`
}

func (self *SearchResponse) Copy(result interface{}) (err error) {
	sources := make(Sources, len(self.Hits.Hits))
	for index, hit := range self.Hits.Hits {
		sources[index] = hit.Source
	}
	buf, err := json.Marshal(sources)
	if err != nil {
		return
	}
	resultValue := reflect.ValueOf(result).Elem()
	for resultValue.Kind() == reflect.Ptr {
		resultValue = resultValue.Elem()
	}
	if err = json.Unmarshal(buf, resultValue.FieldByName("Items").Addr().Interface()); err != nil {
		return
	}
	resultValue.FieldByName("Total").Set(reflect.ValueOf(self.Hits.Total))
	resultValue.FieldByName("Page").Set(reflect.ValueOf(self.Page))
	resultValue.FieldByName("PerPage").Set(reflect.ValueOf(self.PerPage))

	return
}

type FacetResponse struct {
	Type    string              `json:"_type"`
	Missing int                 `json:"missing"`
	Total   int                 `json:"total"`
	Other   int                 `json:"other"`
	Terms   []TermFacetResponse `json:"terms"`
}

type TermFacetResponse struct {
	Term  string `json:"term"`
	Count int    `json:"count"`
}

type FilteredQuery struct {
	Query  *Query  `json:"query"`
	Filter *Filter `json:"filter"`
}

type BoolFilter struct {
	Must    []Filter `json:"must,omitempty"`
	MustNot []Filter `json:"must_not,omitempty"`
	Should  []Filter `json:"should,omitempty"`
}

type BoolQuery struct {
	Must               []Query `json:"must,omitempty"`
	MustNot            []Query `json:"must_not,omitempty"`
	Should             []Query `json:"should,omitempty"`
	MinimumShouldMatch int     `json:"minimum_should_match,omitempty"`
	Boost              float64 `json:"boost,omitempty"`
}

type MissingFilter struct {
	Field string `json:"field"`
}

type Filter struct {
	Or      []Query                `json:"or,omitempty"`
	Query   *Query                 `json:"query,omitempty"`
	Bool    *BoolFilter            `json:"bool,omitempty"`
	Term    map[string]interface{} `json:"term,omitempty"`
	Range   map[string]RangeDef    `json:"range,omitempty"`
	Missing *MissingFilter         `json:"missing,omitempty"`
}

type RangeDef struct {
	Gt    string `json:"gt,omitempty"`
	Gte   string `json:"gte,omitempty"`
	Lt    string `json:"lt,omitempty"`
	Lte   string `json:"lte,omitempty"`
	Boost string `json:"boost,omitempty"`
}

func SearchAndCopy(c ElasticSearchContext, query *SearchRequest, index string, result interface{}) (err error) {
	name := reflect.ValueOf(result).Elem().FieldByName("Items").Type().Elem().Name()
	response, err := Search(c, query, index, name)
	if err != nil {
		return
	}
	if err = response.Copy(result); err != nil {
		return
	}
	return
}

func Search(c ElasticSearchContext, query *SearchRequest, index, typ string) (result *SearchResponse, err error) {
	if query.Size == 0 {
		query.Size = 10
	}
	index = processIndexName(index)

	url := c.GetElasticService()
	if index == "" {
		url += "/_all"
	} else {
		url += "/" + index
	}
	if typ != "" {
		url += "/" + typ
	}
	url += "/_search"

	b, err := json.Marshal(query)
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		return
	}

	if c.GetElasticUsername() != "" {
		request.SetBasicAuth(c.GetElasticUsername(), c.GetElasticPassword())
	}

	response, err := c.Client().Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad status trying to search in elasticsearch %v: %v", url, response.Status)
		return
	}

	result = &SearchResponse{}
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	if err = json.Unmarshal(bodyBytes, &result); err != nil {
		var secondTry interface{}
		if err = json.Unmarshal(bodyBytes, &secondTry); err != nil {
			return
		}
		err = fmt.Errorf("Unable to marshal %v into %#v", utils.Prettify(secondTry), result)
		return
	}

	c.Debugf("Elasticsearch took %v, url:%s", result.Took, url)
	result.Page = 1 + (query.From / query.Size)
	result.PerPage = query.Size
	return
}
