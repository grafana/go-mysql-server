package jsontests

import (
	goJson "encoding/json"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/valyala/fastjson"
	"log"
	"strings"
	"testing"
)

var result sql.JSONWrapper

// BenchmarkJsonExtract-12    	  256358	      4882 ns/op
// cache compiled           	  279912	      4270 ns/op
// no double stringer       	  259893	      4039 ns/op
// custom parsing/search    	  765928	      1556 ns/op
var jsonDocument sql.JSONWrapper = types.JSONDocument{Val: map[string]interface{}{
	"a": []interface{}{float64(1), float64(2), float64(3), float64(4)},
	"b": map[string]interface{}{
		"c": "foo",
		"d": true,
	},
	"e": []interface{}{
		[]interface{}{float64(1), float64(2)},
		[]interface{}{float64(3), float64(4)},
	},
	"f": map[string]interface{}{
		"g": map[string]interface{}{
			"h": strings.Repeat("varchar", 100),
			"i": 4,
		},
	},
}}

func BenchmarkJsonExtract(b *testing.B) {

	e, err := json.NewJSONExtract(
		expression.NewGetField(0, types.LongText, "arg1", false),
		expression.NewGetField(1, types.LongText, "arg2", false),
	)

	ctx := sql.NewEmptyContext()

	jsonBytes, err := types.MarshallJson(jsonDocument)
	jsonStr := string(jsonBytes)

	fmt.Println(len(jsonStr))
	var res interface{}

	for range b.N {
		res, err = e.Eval(ctx, sql.Row{jsonStr, "f.g.i"})
		if err != nil {
			log.Fatal(err)
		}
	}
	result = res.([]sql.JSONWrapper)[0]
}

func BenchmarkJsonMarshall(b *testing.B) {
	jsonBytes, err := types.MarshallJson(jsonDocument)
	jsonStr := string(jsonBytes)

	var jsonData interface{}
	for range b.N {
		if err = goJson.Unmarshal([]byte(jsonStr), &jsonData); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(jsonData)
}

func BenchmarkJsonDecode(b *testing.B) {
	jsonBytes, err := types.MarshallJson(jsonDocument)
	jsonStr := string(jsonBytes)

	var jsonData interface{}
	for range b.N {
		jsonData, err = fastjson.Parse(jsonStr)
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(jsonData)
}
