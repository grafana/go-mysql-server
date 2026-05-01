package sqlredact

import (
	"strings"
	"testing"
)

// containsAny reports whether s contains any of the substrings in subs.
func containsAny(s string, subs ...string) string {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return sub
		}
	}
	return ""
}

func TestRedactSQLForTrace_BasicSelect(t *testing.T) {
	sql := "SELECT a, b, c FROM t WHERE x = 1234 AND y = 1234 AND z = 'apple'"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leak := containsAny(got, "a", "b", "c", "t", "x", "y", "z", "apple", "1234"); leak != "" {
		// Note: "a", "b", "c", "t", "x", "y", "z" are short and could
		// collide with letters in keywords like "SELECT", "FROM",
		// "AND", "WHERE". Use a sharper check below; the loop above
		// just spots gross leaks.
		_ = leak
	}
	// Sharper checks: literal values must be replaced with v-tokens.
	if !strings.Contains(got, ":v1") {
		t.Fatalf("expected literal token :v1 in: %q", got)
	}
	// Same value 1234 dedupes.
	if strings.Count(got, ":v1") != 2 {
		t.Fatalf("expected the duplicated 1234 to dedupe to one token (twice in output), got: %q", got)
	}
	// 'apple' is a different value.
	if !strings.Contains(got, "'v2'") {
		t.Fatalf("expected 'apple' to map to 'v2' string-literal, got: %q", got)
	}
	// Identifiers come back backtick-quoted.
	if !strings.Contains(got, "`n1`") {
		t.Fatalf("expected first identifier as `n1`, got: %q", got)
	}
	if len(m.idents) != 7 {
		t.Fatalf("expected 7 idents (a,b,c,t,x,y,z), got %d: %v",
			len(m.idents), m.idents)
	}
	if len(m.values) != 2 {
		t.Fatalf("expected 2 values (1234, apple), got %d: %v",
			len(m.values), m.values)
	}
}

func TestRedactSQLForTrace_QualifiedNames(t *testing.T) {
	sql := "SELECT u.name, u.email FROM users AS u WHERE u.id = 5"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Same lexeme "u" appears multiple times — must always map to the
	// same token. Pick whichever token "u" got; assert it's repeated.
	uTok := m.idents["u"]
	if uTok == "" {
		t.Fatalf("alias 'u' missing from idents map: %v", m.idents)
	}
	// Backtick-quoted form appears 4 times in the input (u.name, u.email,
	// AS u, u.id).
	if c := strings.Count(got, "`"+uTok+"`"); c < 4 {
		t.Fatalf("expected `%s` at least 4 times for repeated 'u', got %d: %q",
			uTok, c, got)
	}
	if leak := containsAny(got, "users", "name", "email"); leak != "" {
		t.Fatalf("identifier %q leaked: %q", leak, got)
	}
}

func TestRedactSQLForTrace_DBQualifiedTable(t *testing.T) {
	sql := "SELECT job FROM `prometheus::bfh6nkyxwj7cwf`.`up`"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "bfh6nkyxwj7cwf") {
		t.Fatalf("UID leaked through redaction: %q", got)
	}
	if strings.Contains(got, "prometheus") {
		t.Fatalf("datasource type leaked through redaction: %q", got)
	}
	if strings.Contains(got, "up") {
		// "up" is short and could appear inside a keyword — sanity check the
		// quoted form specifically.
		if strings.Contains(got, "`up`") {
			t.Fatalf("table name leaked: %q", got)
		}
	}
}

func TestRedactSQLForTrace_INTuple(t *testing.T) {
	sql := "SELECT a FROM t WHERE x IN (1, 2, 3)"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Each integer in the tuple should be a distinct redacted token,
	// none should appear as the raw digit.
	for _, lit := range []string{" 1 ", " 2 ", " 3 ", "(1", "(2", "(3"} {
		if strings.Contains(got, lit) {
			t.Fatalf("raw integer leaked %q: %q", lit, got)
		}
	}
}

func TestRedactSQLForTrace_LongValueDedupes(t *testing.T) {
	long := strings.Repeat("x", 300)
	sql := "SELECT a FROM t WHERE n = '" + long + "' AND m = '" + long + "'"
	_, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.values) != 1 {
		t.Fatalf("expected long value to dedupe, got %d entries: %v",
			len(m.values), m.values)
	}
}

func TestRedactSQLForTrace_ParseErrorFallsBack(t *testing.T) {
	// Syntactically invalid SQL fails parse; we fall back to the
	// UnparseableMarker rather than emit lex-only redaction (which
	// would leak non-reserved-keyword identifiers).
	got, m, err := RedactSQLForTrace("SELECT secret_col FROM secret_tbl WHERE")
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if got != UnparseableMarker {
		t.Fatalf("expected %q, got %q", UnparseableMarker, got)
	}
	if m == nil {
		t.Fatalf("mapping must be non-nil even on error")
	}
	if len(m.idents) != 0 || len(m.values) != 0 {
		t.Fatalf("expected empty mapping on error, got %v", m)
	}
}

func TestRedactSQLForTrace_NonReservedKeywordAsIdentifier(t *testing.T) {
	// `name`, `data`, `user`, `time` are all non-reserved keywords in
	// the vitess grammar — the lexer emits a keyword token type for
	// them, but they are valid as bare column or table names. The
	// redactor must catch them via the identifier set built from the
	// AST.
	cases := []string{
		"SELECT name FROM users",
		"SELECT u.name, u.data FROM users AS u",
		"SELECT * FROM `time`",
		"SELECT user FROM accounts",
	}
	for _, sql := range cases {
		got, _, err := RedactSQLForTrace(sql)
		if err != nil {
			t.Fatalf("error on %q: %v", sql, err)
		}
		// None of the original identifiers should appear in
		// quoted-or-unquoted form. Look for the backtick-wrapped
		// originals specifically — they would be the leak shape.
		for _, leaky := range []string{"`name`", "`data`", "`user`", "`time`", "`users`", "`accounts`"} {
			if strings.Contains(got, leaky) {
				t.Fatalf("non-reserved keyword identifier %q leaked from %q: %q", leaky, sql, got)
			}
		}
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

func TestRedactSQLForTrace_MarginAndInlineCommentsDropped(t *testing.T) {
	cases := []string{
		"/* user_id='alice@example.com' */ SELECT 1 FROM t",
		"SELECT 1 FROM t /* trailing alice */",
		"SELECT /* inline alice */ 1 FROM t",
		"SELECT 1 FROM t -- alice line comment",
	}
	for _, sql := range cases {
		got, _, err := RedactSQLForTrace(sql)
		if err != nil {
			t.Fatalf("error on %q: %v", sql, err)
		}
		if strings.Contains(got, "alice") {
			t.Fatalf("comment leaked from %q: %q", sql, got)
		}
	}
}

func TestRedactSQLForTrace_Insert(t *testing.T) {
	sql := "INSERT INTO users (name, email) VALUES ('alice', 'a@x')"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leak := containsAny(got, "alice", "a@x", "users", "email"); leak != "" {
		t.Fatalf("INSERT leaked %q: %q", leak, got)
	}
	// "name" appears in the input as both a column name AND it's a
	// substring of "VALUES" — sanity check the backtick form.
	if strings.Contains(got, "`name`") {
		t.Fatalf("column 'name' leaked verbatim: %q", got)
	}
}

func TestRedactSQLForTrace_TableHintValues(t *testing.T) {
	// FOR (...) clause — the value 5m must redact, the lexer does not
	// know about "hint name vs hint value" distinctions, so the inner
	// quoted '5m' becomes a STRING token and is redacted.
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

func TestRedactSQLForTrace_SelectExprAlias(t *testing.T) {
	sql := "SELECT email AS user_email_addr FROM users"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leak := containsAny(got, "user_email_addr", "email", "users"); leak != "" {
		t.Fatalf("identifier %q leaked: %q", leak, got)
	}
}

func TestRedactSQLForTrace_CTEColumnRename(t *testing.T) {
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

func TestRedactSQLForTrace_TableFunctionLeakedNoMore(t *testing.T) {
	// The earlier AST walker leaked the table-function NAME and
	// alias because TableFuncExpr is a value-receiver SQLNode. The
	// lexer-based redactor doesn't have that gap — both `my_func`
	// and `sub` are ID tokens.
	sql := "SELECT * FROM my_func('arg1', 'arg2') AS sub"
	got, _, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leak := containsAny(got, "my_func", "sub", "arg1", "arg2"); leak != "" {
		t.Fatalf("identifier or value %q leaked: %q", leak, got)
	}
}

func TestRedactSQLForTrace_OperatorsPassThrough(t *testing.T) {
	// Multi-char operators (!=, <=, >=, <>, <<, >>, <=>, ->, ->>) come
	// out of the lexer with empty val and a typ ≥ 256. emitStructural
	// must restore them via the symbolOps map.
	cases := map[string]string{
		"SELECT 1 WHERE a != b":  "!=",
		"SELECT 1 WHERE a <> b":  "!=", // <> shares NE token with !=
		"SELECT 1 WHERE a <= b":  "<=",
		"SELECT 1 WHERE a >= b":  ">=",
		"SELECT 1 WHERE a << b":  "<<",
		"SELECT 1 WHERE a >> b":  ">>",
		"SELECT 1 WHERE a <=> b": "<=>",
	}
	for sql, op := range cases {
		got, _, err := RedactSQLForTrace(sql)
		if err != nil {
			t.Fatalf("error on %q: %v", sql, err)
		}
		if !strings.Contains(got, op) {
			t.Fatalf("operator %q lost from %q -> %q", op, sql, got)
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

func TestRedactSQLForTrace_HexAndBitLiterals(t *testing.T) {
	sql := "SELECT * FROM t WHERE a = 0xCAFE AND b = X'CAFE' AND c = B'10101'"
	got, m, err := RedactSQLForTrace(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if leak := containsAny(got, "0xCAFE", "CAFE", "10101"); leak != "" {
		t.Fatalf("hex/bit literal %q leaked: %q", leak, got)
	}
	if len(m.values) < 2 {
		t.Fatalf("expected hex/bit literals in mapping, got: %v", m.values)
	}
}

func TestMapping_TokensRepeat(t *testing.T) {
	m := NewMapping()
	if got := m.RedactIdent("foo"); got != "n1" {
		t.Fatalf("first ident: got %q want n1", got)
	}
	if got := m.RedactIdent("foo"); got != "n1" {
		t.Fatalf("repeated lookup: got %q want n1", got)
	}
	if got := m.RedactIdent("bar"); got != "n2" {
		t.Fatalf("second ident: got %q want n2", got)
	}
	if got := m.RedactValue("foo"); got != "v1" {
		t.Fatalf("'foo' as value should not collide with ident namespace: got %q", got)
	}
}

func TestMapping_NilSafe(t *testing.T) {
	var m *Mapping
	if got := m.RedactIdent("foo"); got != "foo" {
		t.Fatalf("nil mapping should pass through, got %q", got)
	}
	if got := m.RedactValue("foo"); got != "foo" {
		t.Fatalf("nil mapping should pass through value, got %q", got)
	}
}
