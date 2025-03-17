package plan

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strings"
)

// ProjectAppend adds a set of synthetic columns on top of
// passthrough child columns
type ProjectAppend struct {
	UnaryNode
	Projections []sql.Expression
	deps        sql.ColSet
}

func NewProjectAppend(child sql.Node, proj []sql.Expression) *ProjectAppend {
	return &ProjectAppend{UnaryNode: UnaryNode{Child: child}, Projections: proj}
}

func (p *ProjectAppend) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, p.Child)
}

func (p *ProjectAppend) ProjectedExprs() []sql.Expression {
	passthru := expression.SchemaToGetFields(p.Child.Schema(), sql.ColSet{})
	return append(passthru, p.Projections...)
}

func (p *ProjectAppend) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("ProjectAppend")
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = expr.String()
	}
	columns := fmt.Sprintf("columns: [%s]", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(columns, p.Child.String())
	return pr.String()
}
func (p *ProjectAppend) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("ProjectAppend")
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = sql.DebugString(expr)
	}
	exprs = append(exprs, sql.DebugString(p.Child))
	//columns := fmt.Sprintf("columns: [%s]", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(exprs...)

	return pr.String()
}
func (p *ProjectAppend) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	np := *p
	np.Child = children[0]
	return &np, nil
}

func (p *ProjectAppend) IsReadOnly() bool {
	return p.Child.IsReadOnly()
}

func (p *ProjectAppend) Expressions() []sql.Expression {
	return p.Projections
}

func (p *ProjectAppend) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(p.Projections) {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), len(p.Projections))
	}
	np := *p
	np.Projections = exprs
	return &np, nil
}

var _ sql.Expressioner = (*ProjectAppend)(nil)
var _ sql.Node = (*ProjectAppend)(nil)
var _ sql.Projector = (*ProjectAppend)(nil)
var _ sql.CollationCoercible = (*ProjectAppend)(nil)
