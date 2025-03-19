package planbuilder

import (
	"encoding/binary"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"strconv"
	"strings"
)

// walk the AST
// memoize expressions and columns
// memoize table source columns
// lookups for ast.Expr->hash and hash->sql.Expression
// defer binding column references

// select 1 from dual
// select a + b from ab
// select a from ab, xy where a = x;
// with ww as (select x, x from xy) select w from ww;

/*
select 1 from dual
- walk, find dual, no columns, hash (1) -> 1

// select a + b from ab
- visit ab, two table cols (1,2)
- visit (a+b) -> need to resolve a and b to 1, 2, then intern (1+2)

functional requirements:
- hash expressions, common expressions are interned
- expressions used multiple times ref'd from synthetic col
- pruning still works
- synthetic col created as high in tree as possible

walk expression from bottom up
rolling hash moving upwards
if we hash an expr and it's found, reference previous id
  otherwise record a new expression -> meta for type and children ids
exprs kept in abstract form, colIds for checking semantic correctness?
how to hash subquery expressions?

prototype impl with a handful of expressions/nodes
- col, convert, binary expr equal, json_extract
- agg min/max/count
- select, group by, table scan, join

expression standin?
type BindExpr struct {
  id colId
  hash uint64
  children sql.ColSet
  deps sql.ColSet
}

walk ast.Expr postorder
calculate hash of leaf exprs, intern if not seen before
intern: assign id, save hash -> BindExpr, id -> BindExpr
columns are special, need to be resolved in the context of the scope
accumulate hashes working upwards
hash iternal node expressions, intern if not seen before
return a hash of the ast.Expr


preVisit
postVisit
*/

type exprOpId uint16
const (
	unknownTypeId exprOpId = iota
	selectTypeId
	projectTypeId
	defaultTypeId
	convertTypeId
	jsonExtractTypeId
	colTypeId
	litTypeId
)

type bindExpr struct {
	op exprOpId
	id sql.ColumnId
	children sql.ColSet
	colDeps sql.ColSet
}

func newBindExpr(op exprOpId, id sql.ColumnId, children, colDeps sql.ColSet) bindExpr {
	return bindExpr{op: op, id: id, children: children, colDeps: colDeps}
}

var _ sql.Expression = (*bindExpr)(nil)

func (b bindExpr) Resolved() bool {
	//TODO implement me
	panic("implement me")
}

func (b bindExpr) String() string {
	//TODO implement me
	panic("implement me")
}

func (b bindExpr) Type() sql.Type {
	//TODO implement me
	panic("implement me")
}

func (b bindExpr) IsNullable() bool {
	//TODO implement me
	panic("implement me")
}

func (b bindExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (b bindExpr) Children() []sql.Expression {
	//TODO implement me
	panic("implement me")
}

func (b bindExpr) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	//TODO implement me
	panic("implement me")
}

func (b *Builder) scalarExpr(inScope *scope, e ast.Expr) bindExpr {
	switch e := e.(type) {
	case ast.ParentSQLExpr:
		id, err := b.WalkExpr(inScope, e)
		if err != nil {
			b.handleErr(err)
		}
		return b.intern.colIdToBindExpr[id]
	default:
		sqlExpr := b.buildScalar(inScope, e)
		return b.intern.newExpr(0, unknownTypeId, sqlExpr, sql.ColSet{})
	}
}
func (b *Builder) WalkExpr(inScope *scope, node ast.ParentSQLExpr) (sql.ColumnId, error) {
	digest := xxhash.New()
	var childIds sql.ColSet
	for _, c := range node.Children() {
		cId, err := b.WalkExpr(inScope, c)
		if err != nil {
			return 0, err
		}
		childIds.Add(cId)
		writeColId(digest, cId)
	}
	op := getExprOp(node)
	if err := b.visitScalar(inScope, digest, node, op); err != nil {
		return 0, err
	}
	h := digest.Sum64()
	if id, ok := b.intern.hashToId[h]; ok && h != 0 {
		return id, nil
	} else {
		e := b.buildScalar(inScope, node)
		be := b.intern.newExpr(h, op, e, childIds)
		return be.id, nil
	}
}

func visitRelational(d *xxhash.Digest, n ast.SQLNode) error {
	switch n := n.(type) {
	case ast.SelectStatement:
		writeTypeId(d, selectTypeId)
		return nil
	default:
		return fmt.Errorf("unsupported node type: %T", n)
	}
}
func getExprOp(n ast.ParentSQLExpr) exprOpId {
	switch n.(type) {
	case ast.ExtractFuncExpr:
	case ast.Default:
	case ast.Col
	}
}
func visitTableExpr(d *xxhash.Digest, n ast.SQLNode) error {
	switch n := n.(type) {
	case ast.SelectStatement:
		writeTypeId(d, selectTypeId)
		return nil
	default:
		return fmt.Errorf("unsupported node type: %T", n)
	}
}
func (b *Builder) visitNode(inScope *scope, digest *xxhash.Digest, n ast.ParentSQLNode) (uint64, error) {
	var err error
	switch n := n.(type) {
	case ast.Expr:
		err = visitScalar(inScope, digest, n)
	default:
		return 0, fmt.Errorf("unsupported node type: %T", n)
	}
	if err != nil {
		return 0, err
	}
	return digest.Sum64(), nil
}

func (b *Builder) visitScalar(inScope *scope, d *xxhash.Digest, n ast.Expr, op exprOpId) error {
	writeTypeId(d, op)
	switch v := n.(type) {
	case *ast.Default:
		writeString(d, v.ColName)
	//case *ast.SubstrExpr:
	//	writeTypeId(d, substrTypeId)
	//case *ast.TrimExpr:
	//	writeTypeId(d, trimTypeId)
	//	writeString(d, v.Dir)
	//case *ast.ComparisonExpr:
	//	writeString(d, strings.ToLower(v.Operator))
	//case *ast.IsExpr:
	//	writeString(d, strings.ToLower(v.Operator))
	//case *ast.NotExpr:
	//	writeTypeId(d, notTypeId)

	case *ast.SQLVal:
		writeBytes(d, v.Val)
		writeUint64(d, uint64(v.Type))

	//case ast.BoolVal:
	//	return expression.NewLiteral(bool(v), types.Boolean)
	//case *ast.NullVal:
	//	return expression.NewLiteral(nil, types.Null)
	case *ast.ColName:
		dbName := strings.ToLower(v.Qualifier.DbQualifier.String())
		tblName := strings.ToLower(v.Qualifier.Name.String())
		colName := strings.ToLower(v.Name.String())
		c, ok := inScope.resolveColumn(dbName, tblName, colName, true, false)
		if !ok {
			sysVar, scope, ok := b.buildSysVar(v, ast.SetScope_None)
			if ok {
				return sysVar
			}
			var err error
			if scope == ast.SetScope_User {
				err = sql.ErrUnknownUserVariable.New(colName)
			} else if scope == ast.SetScope_Persist || scope == ast.SetScope_PersistOnly {
				err = sql.ErrUnknownUserVariable.New(colName)
			} else if scope == ast.SetScope_Global || scope == ast.SetScope_Session {
				err = sql.ErrUnknownSystemVariable.New(colName)
			} else if tblName != "" && !inScope.hasTable(tblName) {
				err = sql.ErrTableNotFound.New(tblName)
			} else if tblName != "" {
				err = sql.ErrTableColumnNotFound.New(tblName, colName)
			} else {
				err = sql.ErrColumnNotFound.New(v)
			}
			b.handleErr(err)
		}

		origTbl := b.getOrigTblName(inScope.node, c.table)
		c = c.withOriginal(origTbl, v.Name.String())

		var b [2]byte
		binary.BigEndian.PutUint16(b[:], uint16(c.id))
		writeBytes(d, b[:])
		writeString(d, c.col)
		writeString(d, c.originalCol)

	case *ast.FuncExpr:
		args := make([]sql.Expression, len(v.Exprs))
		for i, e := range v.Exprs {
			args[i] = b.selectExprToExpression(inScope, e)
		}

		name := v.Name.Lowered()
		if name == "json_value" {
			if len(args) == 3 {
				args[2] = b.getJsonValueTypeLiteral(args[2])
			}
		}

		if name == "name_const" {
			return b.buildNameConst(inScope, v)
		} else if name == "icu_version" {
			return expression.NewLiteral(icuVersion, types.MustCreateString(query.Type_VARCHAR, int64(len(icuVersion)), sql.Collation_Default))
		} else if isAggregateFunc(name) && v.Over == nil {
			// TODO this assumes aggregate is in the same scope
			// also need to avoid nested aggregates
			return b.buildAggregateFunc(inScope, name, v)
		} else if isWindowFunc(name) {
			return b.buildWindowFunc(inScope, name, v, (*ast.WindowDef)(v.Over))
		}

		f, ok := b.cat.Function(b.ctx, name)
		if !ok {
			// todo(max): similar names in registry?
			err := sql.ErrFunctionNotFound.New(name)
			b.handleErr(err)
		}

		rf, err := f.NewInstance(args)
		if err != nil {
			b.handleErr(err)
		}

		switch rf.(type) {
		case *function.Sleep, sql.NonDeterministicExpression:
			b.qFlags.Set(sql.QFlagUndeferrableExprs)
		}

		// NOTE: Not all aggregate functions support DISTINCT. Fortunately, the vitess parser will throw
		// errors for when DISTINCT is used on aggregate functions that don't support DISTINCT.
		if v.Distinct {
			if len(args) != 1 {
				return nil
			}
			args[0] = expression.NewDistinctExpression(args[0])
		}

		if _, ok := rf.(sql.NonDeterministicExpression); ok && inScope.nearestSubquery() != nil {
			inScope.nearestSubquery().markVolatile()
		}

		return rf
	//case *ast.GroupConcatExpr:
	//	// TODO this is an aggregation
	//	return b.buildGroupConcat(inScope, v)
	//case *ast.ParenExpr:
	//	return b.buildScalar(inScope, v.Expr)
	//case *ast.AndExpr:
	//	lhs := b.buildScalar(inScope, v.Left)
	//	rhs := b.buildScalar(inScope, v.Right)
	//	return expression.NewAnd(lhs, rhs)
	//case *ast.OrExpr:
	//	lhs := b.buildScalar(inScope, v.Left)
	//	rhs := b.buildScalar(inScope, v.Right)
	//	return expression.NewOr(lhs, rhs)
	//case *ast.XorExpr:
	//	lhs := b.buildScalar(inScope, v.Left)
	//	rhs := b.buildScalar(inScope, v.Right)
	//	return expression.NewXor(lhs, rhs)
	//case *ast.ConvertUsingExpr:
	//	expr := b.buildScalar(inScope, v.Expr)
	//	charset, err := sql.ParseCharacterSet(v.Type)
	//	if err != nil {
	//		b.handleErr(err)
	//	}
	//	return expression.NewConvertUsing(expr, charset)
	//case *ast.CharExpr:
	//	args := make([]sql.Expression, len(v.Exprs))
	//	for i, e := range v.Exprs {
	//		args[i] = b.selectExprToExpression(inScope, e)
	//	}
	//
	//	f, err := function.NewChar(args...)
	//	if err != nil {
	//		b.handleErr(err)
	//	}
	//
	//	collId, err := sql.ParseCollation(v.Type, "", true)
	//	if err != nil {
	//		b.handleErr(err)
	//	}
	//
	//	charFunc := f.(*function.Char)
	//	charFunc.Collation = collId
	//	return charFunc
	case *ast.ConvertExpr:
		expr := b.buildScalar(inScope, v.Expr)

		var err error
		typeLength := 0
		if v.Type.Length != nil {
			// TODO move to vitess
			typeLength, err = strconv.Atoi(v.Type.Length.String())
			if err != nil {
				b.handleErr(err)
			}
		}

		typeScale := 0
		if v.Type.Scale != nil {
			// TODO move to vitess
			typeScale, err = strconv.Atoi(v.Type.Scale.String())
			if err != nil {
				b.handleErr(err)
			}
		}
		ret, err := b.f.buildConvert(expr, v.Type.Type, typeLength, typeScale)
		if err != nil {
			b.handleErr(err)
		}
		return ret
	//case ast.InjectedExpr:
	//	if err := b.cat.AuthorizationHandler().HandleAuth(b.ctx, b.authQueryState, v.Auth); err != nil && b.authEnabled {
	//		b.handleErr(err)
	//	}
	//	resolvedChildren := make([]any, len(v.Children))
	//	for i, child := range v.Children {
	//		resolvedChildren[i] = b.buildScalar(inScope, child)
	//	}
	//	expr, err := v.Expression.WithResolvedChildren(resolvedChildren)
	//	if err != nil {
	//		b.handleErr(err)
	//		return nil
	//	}
	//	if sqlExpr, ok := expr.(sql.Expression); ok {
	//		return sqlExpr
	//	}
	//	b.handleErr(fmt.Errorf("Injected expression does not resolve to a valid expression"))
	//	return nil
	//case *ast.RangeCond:
	//	val := b.buildScalar(inScope, v.Left)
	//	lower := b.buildScalar(inScope, v.From)
	//	upper := b.buildScalar(inScope, v.To)
	//
	//	switch strings.ToLower(v.Operator) {
	//	case ast.BetweenStr:
	//		return expression.NewBetween(val, lower, upper)
	//	case ast.NotBetweenStr:
	//		b.qFlags.Set(sql.QFlgNotExpr)
	//		return expression.NewNot(expression.NewBetween(val, lower, upper))
	//	default:
	//		return nil
	//	}
	//case ast.ValTuple:
	//	var exprs = make([]sql.Expression, len(v))
	//	for i, e := range v {
	//		expr := b.buildScalar(inScope, e)
	//		exprs[i] = expr
	//	}
	//	return expression.NewTuple(exprs...)
	case *ast.BinaryExpr:
		l := b.buildScalar(inScope, v.Left)
		r := b.buildScalar(inScope, v.Right)

		expr, err := b.binaryExprToExpression(inScope, v, l, r)
		if err != nil {
			b.handleErr(err)
		}
		return expr
	//case *ast.UnaryExpr:
	//	return b.buildUnaryScalar(inScope, v)
	//case *ast.Subquery:
	//	sqScope := inScope.pushSubquery()
	//	inScope.refsSubquery = true
	//	selectString := ast.String(v.Select)
	//	selScope := b.buildSelectStmt(sqScope, v.Select)
	//	// TODO: get the original select statement, not the reconstruction
	//	sq := plan.NewSubquery(selScope.node, selectString)
	//	b.qFlags.Set(sql.QFlagScalarSubquery)
	//	sq = sq.WithCorrelated(sqScope.correlated())
	//	if b.TriggerCtx().Active {
	//		sq = sq.WithVolatile()
	//	}
	//	return sq
	//case *ast.CaseExpr:
	//	return b.buildCaseExpr(inScope, v)
	//case *ast.IntervalExpr:
	//	e := b.buildScalar(inScope, v.Expr)
	//	b.qFlags.Set(sql.QFlagInterval)
	//	return expression.NewInterval(e, v.Unit)
	//case *ast.CollateExpr:
	//	// handleCollateExpr is meant to handle generic text-returning expressions that should be reinterpreted as a different collation.
	//	innerExpr := b.buildScalar(inScope, v.Expr)
	//	collation, err := sql.ParseCollation("", v.Collation, false)
	//	if err != nil {
	//		b.handleErr(err)
	//	}
	//	// If we're collating a string literal, we check that the charset and collation match now. Other string sources
	//	// (such as from tables) will have their own charset, which we won't know until after the parsing stage.
	//	charSet := b.ctx.GetCharacterSet()
	//	if _, isLiteral := innerExpr.(*expression.Literal); isLiteral && collation.CharacterSet() != charSet {
	//		b.handleErr(sql.ErrCollationInvalidForCharSet.New(collation.Name(), charSet.Name()))
	//	}
	//	return expression.NewCollatedExpression(innerExpr, collation)
	//case *ast.ValuesFuncExpr:
	//	if b.insertActive {
	//		if v.Name.Qualifier.Name.String() == "" {
	//			v.Name.Qualifier.Name = ast.NewTableIdent(inScope.insertTableAlias)
	//			if len(inScope.insertColumnAliases) > 0 {
	//				v.Name.Name = ast.NewColIdent(inScope.insertColumnAliases[v.Name.Name.Lowered()])
	//			}
	//		}
	//		dbName := strings.ToLower(v.Name.Qualifier.DbQualifier.String())
	//		tblName := strings.ToLower(v.Name.Qualifier.Name.String())
	//		colName := strings.ToLower(v.Name.Name.String())
	//		col, ok := inScope.resolveColumn(dbName, tblName, colName, false, false)
	//		if !ok {
	//			err := fmt.Errorf("expected ON DUPLICATE KEY ... VALUES() to reference a column, found: %s", v.Name.String())
	//			b.handleErr(err)
	//		}
	//		return col.scalarGf()
	//	} else {
	//		col := b.buildScalar(inScope, v.Name)
	//		fn, ok := b.cat.Function(b.ctx, "values")
	//		if !ok {
	//			err := sql.ErrFunctionNotFound.New("values")
	//			b.handleErr(err)
	//		}
	//		values, err := fn.NewInstance([]sql.Expression{col})
	//		if err != nil {
	//			b.handleErr(err)
	//		}
	//		return values
	//	}
	//case *ast.ExistsExpr:
	//	sqScope := inScope.push()
	//	sqScope.initSubquery()
	//	selScope := b.buildSelectStmt(sqScope, v.Subquery.Select)
	//	selectString := ast.String(v.Subquery.Select)
	//	sq := plan.NewSubquery(selScope.node, selectString)
	//	sq = sq.WithCorrelated(sqScope.correlated())
	//	b.qFlags.Set(sql.QFlagScalarSubquery)
	//	return plan.NewExistsSubquery(sq)
	//case *ast.TimestampFuncExpr:
	//	var (
	//		unit  sql.Expression
	//		expr1 sql.Expression
	//		expr2 sql.Expression
	//	)
	//
	//	unit = expression.NewLiteral(v.Unit, types.LongText)
	//	expr1 = b.buildScalar(inScope, v.Expr1)
	//	expr2 = b.buildScalar(inScope, v.Expr2)
	//
	//	if v.Name == "timestampdiff" {
	//		return function.NewTimestampDiff(unit, expr1, expr2)
	//	} else if v.Name == "timestampadd" {
	//		return nil
	//	}
	//	return nil
	case *ast.ExtractFuncExpr:
		childHash := xxhash.New()
		childHash.WriteString(strings.ToUpper(v.Unit))
		childKey := childHash.Sum64()
		var unit sql.Expression
		if sqlExpr, ok := b.intern.getExpr(childKey); ok {
			unit = sqlExpr
		} else {
			unit = expression.NewLiteral(strings.ToUpper(v.Unit), types.LongText)
			b.intern.set(childKey, unit)
		}
		expr := b.buildScalar(inScope, v.Expr)
		return function.NewExtract(unit, expr)
	//case *ast.MatchExpr:
	//	return b.buildMatchAgainst(inScope, v)
	default:
		b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(e)))
	}
	return nil
}

