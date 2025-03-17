package planbuilder

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"github.com/dolthub/go-mysql-server/sql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"strings"
)

type interner struct {
	hashToExpr  map[uint64]sql.Expression
	uses        map[uint64]int
	exprToHash  map[sql.Expression]uint64
	colIdToExpr map[columnId]sql.Expression
	colId       columnId
}

func newInterner() *interner {
	return &interner{
		hashToExpr:  make(map[uint64]sql.Expression),
		uses:        make(map[uint64]int),
		exprToHash:  make(map[sql.Expression]uint64),
		colIdToExpr: make(map[columnId]sql.Expression),
	}
}

func (i *interner) getExpr(h uint64) (sql.Expression, bool) {
	e, ok := i.hashToExpr[h]
	return e, ok
}

func (i *interner) getHash(e sql.Expression) (uint64, bool) {
	h, ok := i.exprToHash[e]
	return h, ok
}

func (i *interner) set(h uint64, e sql.Expression) {
	i.hashToExpr[h] = e
}

func (i *interner) seen(h uint64) bool {
	_, ok := i.hashToExpr[h]
	return ok
}
func (i *interner) preVisit(e ast.Expr, children ...sql.Expression) (uint64, sql.Expression, error) {
	hashableTree := true
	var childHashes []uint64
	for _, a := range children {
		h, ok := i.getHash(a)
		if !ok {
			hashableTree = false
			break
		}
		childHashes = append(childHashes, h)
	}
	if hashableTree {
		h, err := i.hash(e, childHashes...)
		if err != nil {
			return 0, nil, err
		}
		if e, ok := i.getExpr(h); ok {
			return h, e, nil
		}
		return h, nil, nil
	}
	return 0, nil, nil
}
func (i *interner) postVisit(h uint64, e sql.Expression) {
	if h > 0 {
		i.uses[h]++
		if i.uses[h] > 1 {
			return
		}
		i.hashToExpr[h] = e
		i.exprToHash[e] = h
	}
}
func writeUint64(h *xxhash.Digest, i uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	h.Write(b[:])
}
func colHash(id columnId, tableAlias, tableOrig string) uint64 {
	h := xxhash.New()
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(id))
	h.Write(b[:])
	h.WriteString(tableAlias)
	h.WriteString(tableOrig)
	return h.Sum64()
}
func (i *interner) hash(e ast.Expr, children ...uint64) (uint64, error) {
	switch e.(type) {
	case *ast.SQLVal:
		print()
	}
	h := xxhash.New()
	for _, c := range children {
		writeUint64(h, c)
	}
	switch e := e.(type) {
	case *ast.Default:
		h.WriteString(strings.ToLower(e.ColName))
	case *ast.SubstrExpr:
	case *ast.TrimExpr:
	case *ast.ComparisonExpr:
	case *ast.IsExpr:
	case *ast.NotExpr:
	case *ast.SQLVal:
		if e == nil {
			return 0, nil
		}
		h.Write(e.Val)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(e.Type))
		h.Write(b[:])
	case *ast.NullVal:
	case *ast.ColName:
		// noop, use column ids instead
		return 0, nil
	case *ast.FuncExpr:
		if e.Over != nil {
			return 0, nil
		}
		h.WriteString(e.Name.Lowered())
		h.WriteString(strings.ToLower(e.Qualifier.String()))

		if e.Distinct {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}

	case *ast.GroupConcatExpr:
	case *ast.ParenExpr:
	case *ast.AndExpr:
	case *ast.OrExpr:
	case *ast.XorExpr:
	case *ast.ConvertUsingExpr:
	case *ast.CharExpr:
	case *ast.ConvertExpr:
		h.WriteString(strings.ToLower(e.Name))
		h.WriteString(e.Type.Type)
		h.WriteString(e.Type.Operator)
		h.WriteString(e.Type.Charset)
		scaleHash, err := i.hash(e.Type.Scale)
		if err != nil {
			return 0, err
		}
		lenHash, err := i.hash(e.Type.Length)
		if err != nil {
			return 0, err
		}
		writeUint64(h, scaleHash)
		writeUint64(h, lenHash)
	case *ast.RangeCond:
	case *ast.BinaryExpr:
		h.WriteString(e.Operator)
	case *ast.UnaryExpr:
	case *ast.Subquery:
	case *ast.CaseExpr:
	case *ast.IntervalExpr:
	case *ast.CollateExpr:
	case *ast.ValuesFuncExpr:
	case *ast.ExistsExpr:
	case *ast.TimestampFuncExpr:
	case *ast.ExtractFuncExpr:
	case *ast.MatchExpr:
	default:
	}

	return h.Sum64(), nil
}
