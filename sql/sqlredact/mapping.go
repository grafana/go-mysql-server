package sqlredact

import (
	"fmt"
	"strconv"
)

// Mapping records a per-query substitution from original identifier
// or value text to a stable, low-entropy token. Tokens are minted on
// first lookup in walk order; subsequent lookups for the same original
// return the same token. Three independent namespaces are kept so a
// table, column, and value sharing the same surface form get distinct
// tokens.
//
// Tokens are intentionally short and repeating (t1, c1, v1, ...) so
// trace storage compresses well across many queries with the same
// shape. Hashes would be anti-compression.
//
// A Mapping is safe to use from a single goroutine. The intended
// usage is: build during parse, read during execute. No locking.
type Mapping struct {
	tables  map[string]string
	columns map[string]string
	values  map[string]string

	tCount int
	cCount int
	vCount int
}

// NewMapping returns an empty mapping with the three namespaces
// initialized.
func NewMapping() *Mapping {
	return &Mapping{
		tables:  map[string]string{},
		columns: map[string]string{},
		values:  map[string]string{},
	}
}

// RedactTable returns a stable token for orig in the table namespace.
// The empty string is returned unchanged so the caller can preserve
// "no qualifier" cases without a special branch.
func (m *Mapping) RedactTable(orig string) string {
	if m == nil || orig == "" {
		return orig
	}
	if t, ok := m.tables[orig]; ok {
		return t
	}
	m.tCount++
	t := "t" + strconv.Itoa(m.tCount)
	m.tables[orig] = t
	return t
}

// RedactColumn returns a stable token for orig in the column namespace.
func (m *Mapping) RedactColumn(orig string) string {
	if m == nil || orig == "" {
		return orig
	}
	if t, ok := m.columns[orig]; ok {
		return t
	}
	m.cCount++
	t := "c" + strconv.Itoa(m.cCount)
	m.columns[orig] = t
	return t
}

// RedactValue returns a stable token for orig in the value namespace.
// The token is a bare name (no leading colon); callers that want bind
// var syntax should prefix.
func (m *Mapping) RedactValue(orig string) string {
	if m == nil {
		return orig
	}
	if t, ok := m.values[orig]; ok {
		return t
	}
	m.vCount++
	t := "v" + strconv.Itoa(m.vCount)
	m.values[orig] = t
	return t
}

// Tables returns a copy of the original→token map for tables.
func (m *Mapping) Tables() map[string]string { return copyMap(m.tables) }

// Columns returns a copy of the original→token map for columns.
func (m *Mapping) Columns() map[string]string { return copyMap(m.columns) }

// Values returns a copy of the original→token map for values.
func (m *Mapping) Values() map[string]string { return copyMap(m.values) }

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// String returns a debug-friendly summary, used in trace error events
// when redaction is partial. Stable ordering by token name.
func (m *Mapping) String() string {
	if m == nil {
		return "<nil mapping>"
	}
	return fmt.Sprintf("sqlredact.Mapping{tables:%d cols:%d vals:%d}",
		m.tCount, m.cCount, m.vCount)
}
