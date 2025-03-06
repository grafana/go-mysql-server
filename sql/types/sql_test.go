package types

import (
	"bytes"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
)

var result_ sqltypes.Value

func BenchmarkNumI64SQL(b *testing.B) {
	var res sqltypes.Value
	t := Int64
	ctx := sql.NewEmptyContext()
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, i)
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkVarchar10SQL(b *testing.B) {
	var res sqltypes.Value
	t := MustCreateStringWithDefaults(sqltypes.VarChar, 10)
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, "char")
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkTimespanSQL(b *testing.B) {
	var res sqltypes.Value
	t := TimespanType_{}
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, i%60)
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkTimestampSQL(b *testing.B) {
	var res sqltypes.Value
	t := Timestamp
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, "2019-12-31T12:00:00Z")
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkDatetimeSQL(b *testing.B) {
	var res sqltypes.Value
	t := Datetime
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, "2019-12-31T12:00:00Z")
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkEnumSQL(b *testing.B) {
	var res sqltypes.Value
	t, _ := CreateEnumType([]string{"a", "b", "c"}, sql.Collation_Default)
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, "a")
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkSetSQL(b *testing.B) {
	var res sqltypes.Value
	t, _ := CreateSetType([]string{"a", "b", "c"}, sql.Collation_Default)
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, "a")
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkBitSQL(b *testing.B) {
	var res sqltypes.Value
	t := BitType_{numOfBits: 8}
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, i%8)
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkDecimalSQL(b *testing.B) {
	var res sqltypes.Value
	t, _ := CreateColumnDecimalType(2, 2)
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, decimal.New(int64(i), 2))
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}

func BenchmarkNumF64SQL(b *testing.B) {
	var res sqltypes.Value
	t := Float64
	var buf bytes.Buffer
	ctx := sql.NewEmptyContext()
	for i := 0; i < b.N; i++ {
		bufres, _ := t.SQL(ctx, &buf, i)
		res = bufres.ToValue(buf.Bytes())
		buf.Reset()
	}
	result_ = res
}
