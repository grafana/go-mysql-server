package sqlredact

import (
	"strings"
	"testing"
)

func TestRedactSQLForTrace_BasicSelect(t *testing.T) {
	sql := "SELECT a, b, c FROM t WHERE x = 1234 AND y = 1234 AND z = 'apple'"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Identifiers and values both redacted, both deduped.
	want := "select c1, c2, c3 from t1 where c4 = :v1 and c5 = :v1 and c6 = :v2"
	if got != want {
		t.Fatalf("\n got: %q\nwant: %q", got, want)
	}
	if m.tables["t"] != "t1" {
		t.Fatalf("table mapping missing: %v", m.tables)
	}
	if len(m.columns) != 6 {
		t.Fatalf("expected 6 columns, got %d: %v", len(m.columns), m.columns)
	}
	if len(m.values) != 2 {
		t.Fatalf("expected 2 values, got %d: %v", len(m.values), m.values)
	}
}

func TestRedactSQLForTrace_QualifiedNames(t *testing.T) {
	sql := "SELECT u.name, u.email FROM users AS u WHERE u.id = 5"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Walk order visits SelectExprs first, so `u` (the alias as column
	// qualifier) gets t1 and `users` (the underlying table) gets t2.
	// The redacted form is still semantically valid: `t2 AS t1`
	// declares the alias, `t1.c?` references it.
	if !strings.Contains(got, "from t2 as t1") {
		t.Fatalf("expected `from t2 as t1`, got: %q", got)
	}
	if _, ok := m.tables["u"]; !ok {
		t.Fatalf("alias 'u' missing from table map: %v", m.tables)
	}
	if _, ok := m.tables["users"]; !ok {
		t.Fatalf("table 'users' missing from table map: %v", m.tables)
	}
	// The alias and underlying table must get DIFFERENT tokens — they
	// are distinct names from the SQL's perspective.
	if m.tables["u"] == m.tables["users"] {
		t.Fatalf("alias and table collided to same token: %v", m.tables)
	}
}

func TestRedactSQLForTrace_DBQualifiedTable(t *testing.T) {
	// dsabstraction-style qualified name: <dsType>::<UID>.<table>
	sql := "SELECT job FROM `prometheus::bfh6nkyxwj7cwf`.`up`"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Left-to-right segment order: db qualifier first (t1), table second (t2).
	if !strings.Contains(got, "t1.t2") {
		t.Fatalf("expected db.table tokens (t1.t2), got: %q", got)
	}
	if m.tables["prometheus::bfh6nkyxwj7cwf"] != "t1" {
		t.Fatalf("expected db qualifier -> t1, got: %v", m.tables)
	}
	if m.tables["up"] != "t2" {
		t.Fatalf("expected table -> t2, got: %v", m.tables)
	}
	// UID must be fully scrubbed.
	if strings.Contains(got, "bfh6nkyxwj7cwf") {
		t.Fatalf("UID leaked through redaction: %q", got)
	}
	if strings.Contains(got, "prometheus") {
		t.Fatalf("datasource type leaked through redaction: %q", got)
	}
}

func TestRedactSQLForTrace_INTuple(t *testing.T) {
	sql := "SELECT a FROM t WHERE x IN (1, 2, 3)"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// IN tuples collapse into a single list bind arg.
	if !strings.Contains(got, "in ::v1") {
		t.Fatalf("expected list bind arg, got: %q", got)
	}
}

func TestRedactSQLForTrace_LongValueStillDedupes(t *testing.T) {
	long := strings.Repeat("x", 300)
	sql := "SELECT a FROM t WHERE n = '" + long + "' AND m = '" + long + "'"
	_, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Unlike the upstream Normalize, we always dedupe — long values
	// collapse to a single token even in a Select.
	if len(m.values) != 1 {
		t.Fatalf("expected long value to dedupe, got %d entries: %v",
			len(m.values), m.values)
	}
}

func TestRedactSQLForTrace_ParseError(t *testing.T) {
	got, m, err := RedactSQLForTrace("this is not valid sql ;;")
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if got != UnparseableMarker {
		t.Fatalf("expected %q, got %q", UnparseableMarker, got)
	}
	if m == nil {
		t.Fatalf("mapping must be non-nil even on error")
	}
	if len(m.tables) != 0 || len(m.columns) != 0 || len(m.values) != 0 {
		t.Fatalf("expected empty mapping on error, got %v", m)
	}
}

func TestRedactSQLForTrace_Stability(t *testing.T) {
	sql := "SELECT a, b FROM t WHERE x = 1 AND y = 1"
	a, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a != b {
		t.Fatalf("redaction not stable across calls:\n a=%q\n b=%q", a, b)
	}
}

func TestRedactSQLForTrace_MarginCommentsDropped(t *testing.T) {
	sql := "/* user_id='alice@example.com' */ SELECT 1 FROM t /* trail */"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "alice") {
		t.Fatalf("margin comment leaked: %q", got)
	}
	if strings.Contains(got, "trail") {
		t.Fatalf("trailing margin comment leaked: %q", got)
	}
}

func TestRedactSQLForTrace_Insert(t *testing.T) {
	sql := "INSERT INTO users (name, email) VALUES ('alice', 'a@x')"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "alice") || strings.Contains(got, "a@x") {
		t.Fatalf("literal leaked through INSERT: %q", got)
	}
	if strings.Contains(got, "users") {
		t.Fatalf("table name leaked: %q", got)
	}
	if strings.Contains(got, "name") || strings.Contains(got, "email") {
		t.Fatalf("column name leaked: %q", got)
	}
	if len(m.values) != 2 {
		t.Fatalf("expected 2 values, got %d: %v", len(m.values), m.values)
	}
}

func TestRedactSQLForTrace_TableHintValues(t *testing.T) {
	// FOR (...) clause hint with a string value (vitess-specific syntax).
	sql := "SELECT job FROM `prom::uid`.`up` FOR (rate('5m'))"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "5m") {
		t.Fatalf("hint value leaked: %q", got)
	}
	if m.values["5m"] == "" {
		t.Fatalf("expected hint value in mapping, got: %v", m.values)
	}
}

func TestRedactSQLForTrace_NoQualifierNoChangeOnEmpty(t *testing.T) {
	// Sanity check: bare column names without qualifiers don't get a
	// stray table token assigned.
	_, m, err := RedactSQLForTrace("SELECT a FROM t")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.tables) != 1 {
		t.Fatalf("expected exactly one table token, got %v", m.tables)
	}
}

func TestRedactSQLForTrace_SelectExprAlias(t *testing.T) {
	// `SELECT email AS user_email_addr` — the AS name is a ColIdent on
	// *AliasedExpr; without explicit handling it would render verbatim.
	sql := "SELECT email AS user_email_addr FROM users"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "user_email_addr") {
		t.Fatalf("SELECT alias leaked: %q", got)
	}
	if strings.Contains(got, "email") {
		t.Fatalf("column name leaked: %q", got)
	}
}

func TestRedactSQLForTrace_CTEColumnRename(t *testing.T) {
	// `WITH x (renamed_col) AS ...` — the column-rename list is
	// CommonTableExpr.Columns ([]ColIdent); each element is a value
	// type and only mutable through the *CommonTableExpr pointer.
	sql := "WITH x (sensitive_renamed_col) AS (SELECT a FROM t) SELECT * FROM x"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "sensitive_renamed_col") {
		t.Fatalf("CTE column rename leaked: %q", got)
	}
}

func TestRedactSQLForTrace_JoinUsingColumns(t *testing.T) {
	// `JOIN b USING (col)` — the Using list lives on
	// JoinCondition.Using, reachable only through *JoinTableExpr.
	sql := "SELECT * FROM a JOIN b USING (sensitive_join_col)"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "sensitive_join_col") {
		t.Fatalf("USING column leaked: %q", got)
	}
}

func TestRedactSQLForTrace_LikeAndRegexpPatterns(t *testing.T) {
	cases := []string{
		"SELECT * FROM t WHERE name LIKE '%alice@example.com%'",
		"SELECT * FROM t WHERE name REGEXP '^alice@.*'",
	}
	for _, sql := range cases {
		got, _, err := RedactSQLForTrace(sql)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", sql, err)
		}
		if strings.Contains(got, "alice") {
			t.Fatalf("pattern literal leaked for %q: %q", sql, got)
		}
	}
}

func TestRedactSQLForTrace_SubqueryRedacts(t *testing.T) {
	sql := "SELECT * FROM t WHERE id IN (SELECT id FROM secret_table)"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "secret_table") {
		t.Fatalf("subquery table name leaked: %q", got)
	}
}

func TestRedactSQLForTrace_UpdateSetClause(t *testing.T) {
	sql := "UPDATE t SET secret_col = 'leaky_value' WHERE c = 1"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "secret_col") {
		t.Fatalf("UPDATE column name leaked: %q", got)
	}
	if strings.Contains(got, "leaky_value") {
		t.Fatalf("UPDATE value leaked: %q", got)
	}
}

func TestMapping_TokensRepeat(t *testing.T) {
	m := NewMapping()
	if got := m.RedactTable("foo"); got != "t1" {
		t.Fatalf("first table: got %q want t1", got)
	}
	if got := m.RedactTable("foo"); got != "t1" {
		t.Fatalf("repeated lookup: got %q want t1", got)
	}
	if got := m.RedactTable("bar"); got != "t2" {
		t.Fatalf("second table: got %q want t2", got)
	}
	if got := m.RedactColumn("foo"); got != "c1" {
		t.Fatalf("column 'foo' should not collide with table namespace: got %q", got)
	}
}

func TestMapping_NilSafe(t *testing.T) {
	var m *Mapping
	if got := m.RedactTable("foo"); got != "foo" {
		t.Fatalf("nil mapping should pass through, got %q", got)
	}
	if got := m.RedactColumn("foo"); got != "foo" {
		t.Fatalf("nil mapping should pass through, got %q", got)
	}
}
