// Package sqlredact rewrites SQL queries into a form safe to attach to
// trace span attributes. Identifiers (table, column, schema names) and
// literal values are replaced with stable, low-entropy tokens (t1, c1,
// v1, ...) that repeat across queries of the same shape so storage
// compresses well.
//
// This is distinct from sqlparser.Normalize / RedactSQLQuery, which is
// a query-plan-cache primitive that parameterizes literals only. The
// redactor here treats the trace surface as a privacy boundary: it
// covers identifiers, drops margin comments, and falls back to a fixed
// "<unparseable>" string when parsing fails.
//
// The companion *Mapping return value lets callers redact downstream
// trace attributes (e.g. resolved table names emitted from rowexec
// spans) using the same tokens that appeared in the parsed query.
package sqlredact

import (
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// UnparseableMarker is the value substituted when the input cannot be
// parsed. Callers should treat any returned redacted string as opaque
// — never re-parse it or display it as a "fixed" version of the user's
// SQL.
const UnparseableMarker = "<unparseable>"

// RedactSQLForTrace parses sql and returns a copy with identifiers and
// literal values replaced by stable tokens. The returned Mapping
// records the substitutions so callers can redact related downstream
// attributes (resolved table names, etc.) consistently.
//
// On parse failure the redacted string is UnparseableMarker, the
// Mapping is empty, and the parse error is returned. Callers should
// always use the returned redacted string (never the input) when
// publishing to traces, regardless of error.
func RedactSQLForTrace(sql string) (string, *Mapping, error) {
	m := NewMapping()
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return UnparseableMarker, m, err
	}
	if err := redactStmt(stmt, m); err != nil {
		return UnparseableMarker, m, err
	}
	return sqlparser.String(stmt), m, nil
}

// redactStmt mutates stmt in place using the provided Mapping. The
// walker visits the pointer-parent nodes that own value-typed
// identifiers (TableIdent, ColIdent embedded in TableName, ColName,
// AliasedTableExpr, etc.) and rewrites them via reassignment.
//
// Nodes that the visitor handles fully (identifiers and literals) are
// returned with kontinue=false to prevent the walker from descending
// into already-rewritten subtrees and double-counting tokens.
func redactStmt(stmt sqlparser.Statement, m *Mapping) error {
	visit := func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.ColName:
			redactColName(n, m)
			return false, nil
		case *sqlparser.AliasedTableExpr:
			redactAliasedTableExpr(n, m)
			return true, nil
		case sqlparser.TableName:
			// Visited by-value: any mutation here is on a copy and
			// won't propagate. Pointer parents that own a TableName
			// (AliasedTableExpr.Expr, ColName.Qualifier, *Insert.Table,
			// etc.) handle rewriting at their own visit step.
			return false, nil
		case *sqlparser.Insert:
			redactInsert(n, m)
			return true, nil
		case *sqlparser.Update:
			redactUpdate(n, m)
			return true, nil
		case *sqlparser.Delete:
			redactDelete(n, m)
			return true, nil
		case *sqlparser.SQLVal:
			redactSQLVal(n, m)
			return false, nil
		case *sqlparser.ComparisonExpr:
			redactComparison(n, m)
			return true, nil
		}
		return true, nil
	}
	return sqlparser.Walk(visit, stmt)
}

// redactColName rewrites the column name and any embedded table
// qualifier on a *ColName.
func redactColName(n *sqlparser.ColName, m *Mapping) {
	if n == nil {
		return
	}
	if !n.Name.IsEmpty() {
		n.Name = sqlparser.NewColIdent(m.RedactColumn(n.Name.String()))
	}
	if !n.Qualifier.IsEmpty() {
		n.Qualifier = redactTableName(n.Qualifier, m)
	}
}

// redactAliasedTableExpr rewrites the alias and (when the table
// expression is a TableName) the table identifier itself. Subqueries
// and VALUES expressions are left to the walker to descend into.
//
// TableHints carry user-provided values in TableHint.Value (e.g.
// rate('5m')); each is treated as a value-namespace token.
func redactAliasedTableExpr(n *sqlparser.AliasedTableExpr, m *Mapping) {
	if n == nil {
		return
	}
	if !n.As.IsEmpty() {
		n.As = sqlparser.NewTableIdent(m.RedactTable(n.As.String()))
	}
	if tn, ok := n.Expr.(sqlparser.TableName); ok {
		n.Expr = redactTableName(tn, m)
	}
	if n.TableHints != nil {
		for i := range n.TableHints.Hints {
			if n.TableHints.Hints[i].Value != "" {
				n.TableHints.Hints[i].Value = m.RedactValue(n.TableHints.Hints[i].Value)
			}
		}
	}
}

// redactTableName returns a copy of tn with each non-empty identifier
// segment replaced by a table-namespace token. Segments are mapped in
// left-to-right (db, schema, name) order so the rendered token order
// reads naturally — `s1.t1` rather than `t2.t1` for a qualified name.
// All three segments share the table namespace so a name appearing
// once as a qualifier and once as a leaf gets the same token.
func redactTableName(tn sqlparser.TableName, m *Mapping) sqlparser.TableName {
	if !tn.DbQualifier.IsEmpty() {
		tn.DbQualifier = sqlparser.NewTableIdent(m.RedactTable(tn.DbQualifier.String()))
	}
	if !tn.SchemaQualifier.IsEmpty() {
		tn.SchemaQualifier = sqlparser.NewTableIdent(m.RedactTable(tn.SchemaQualifier.String()))
	}
	if !tn.Name.IsEmpty() {
		tn.Name = sqlparser.NewTableIdent(m.RedactTable(tn.Name.String()))
	}
	return tn
}

// redactInsert handles INSERT-statement-specific identifiers that
// the generic walker would otherwise miss because Insert holds its
// target Table as a TableName by value.
func redactInsert(n *sqlparser.Insert, m *Mapping) {
	if n == nil {
		return
	}
	n.Table = redactTableName(n.Table, m)
	for i := range n.Columns {
		if !n.Columns[i].IsEmpty() {
			n.Columns[i] = sqlparser.NewColIdent(m.RedactColumn(n.Columns[i].String()))
		}
	}
}

// redactUpdate is currently a no-op — Update's TableExprs are visited
// via the walker (each is a *AliasedTableExpr) and SET expressions are
// covered by *ColName / *SQLVal handlers. Kept as a hook for future
// fields (e.g. ON DUPLICATE clauses) that may carry data.
func redactUpdate(n *sqlparser.Update, m *Mapping) {
	_ = n
	_ = m
}

// redactDelete handles Delete-specific Targets if present. The
// FROM-side TableExprs are reached via the regular walker.
func redactDelete(n *sqlparser.Delete, m *Mapping) {
	if n == nil {
		return
	}
	for i := range n.Targets {
		n.Targets[i] = redactTableName(n.Targets[i], m)
	}
}

// redactSQLVal converts a literal into a bind-arg-shaped token so the
// rendered SQL stays a syntactically valid prepared statement (":v1"
// rather than a bare "v1"). ValArg-typed nodes are already bind args
// and are left alone.
func redactSQLVal(n *sqlparser.SQLVal, m *Mapping) {
	if n == nil || n.Type == sqlparser.ValArg {
		return
	}
	tok := m.RedactValue(string(n.Val))
	n.Type = sqlparser.ValArg
	n.Val = []byte(":" + tok)
}

// redactComparison preserves the existing IN/NOT IN list-bind-arg
// shape: an entire tuple collapses to one ListArg token whose name is
// allocated from the value namespace.
func redactComparison(n *sqlparser.ComparisonExpr, m *Mapping) {
	if n == nil {
		return
	}
	if n.Operator != sqlparser.InStr && n.Operator != sqlparser.NotInStr {
		return
	}
	tuple, ok := n.Right.(sqlparser.ValTuple)
	if !ok {
		return
	}
	// Build a single representative key from the tuple values so two
	// IN clauses with the same shape map to the same token.
	key := "("
	for i, v := range tuple {
		if i > 0 {
			key += ","
		}
		if sv, ok := v.(*sqlparser.SQLVal); ok {
			key += string(sv.Val)
		} else {
			// Mixed/expression tuples skip the collapse path; the walker
			// will recurse into individual SQLVals normally.
			return
		}
	}
	key += ")"
	tok := m.RedactValue(key)
	n.Right = sqlparser.ListArg(append([]byte("::"), tok...))
}
