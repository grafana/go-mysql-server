// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraceRedaction_DefaultEnabled(t *testing.T) {
	ctx := NewContext(context.Background())
	require.True(t, ctx.TraceRedactionEnabled(), "redaction must be enabled by default")
}

func TestTraceRedaction_WithTraceRedactionFalse_OptsOut(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(false))
	require.False(t, ctx.TraceRedactionEnabled(), "WithTraceRedaction(false) should disable redaction")
}

func TestTraceRedaction_WithTraceRedactionTrue_Explicit(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(true))
	require.True(t, ctx.TraceRedactionEnabled(), "WithTraceRedaction(true) must keep redaction enabled")
}

func TestTraceRedaction_NilContext(t *testing.T) {
	var ctx *Context
	require.True(t, ctx.TraceRedactionEnabled(), "nil context should report redaction enabled (secure default)")
	require.Equal(t, "foo", ctx.RedactNameForTrace("foo"), "nil context name redaction should pass through")
	require.Equal(t, "SELECT 1", ctx.RedactQueryForTrace("SELECT 1"), "nil context query redaction should pass through")
}

func TestTraceRedaction_RedactQueryForTracePopulatesMapping(t *testing.T) {
	ctx := NewContext(context.Background())
	got := ctx.RedactQueryForTrace("SELECT a FROM t WHERE x = 1")
	require.NotContains(t, got, "FROM t", "table name not redacted")
	require.NotContains(t, got, "from t ", "table name not redacted (lowercased form)")
	require.NotNil(t, ctx.RedactionMapping(), "mapping must be populated after RedactQueryForTrace")
}

func TestTraceRedaction_NameUsesParsedMapping(t *testing.T) {
	ctx := NewContext(context.Background())
	_ = ctx.RedactQueryForTrace("SELECT a FROM users WHERE id = 1")
	tok := ctx.RedactNameForTrace("users")
	require.NotEqual(t, "users", tok, "expected redacted token for 'users', got original")
	// Same name must return the same token across calls.
	require.Equal(t, tok, ctx.RedactNameForTrace("users"), "name redaction must be stable")
}

func TestTraceRedaction_NameWithoutParsePhase(t *testing.T) {
	// rowexec spans may fire on contexts where no SQL was parsed.
	// Name redaction should still mint a stable token.
	ctx := NewContext(context.Background())
	a := ctx.RedactNameForTrace("orphan_table")
	require.NotEqual(t, "orphan_table", a, "expected a redacted token, got original")
	require.Equal(t, a, ctx.RedactNameForTrace("orphan_table"), "token must be stable across calls")
}

func TestTraceRedaction_DisabledPassesThrough(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(false))
	const q = "SELECT a FROM t WHERE x = 1"
	require.Equal(t, q, ctx.RedactQueryForTrace(q), "disabled redaction must pass through")
	require.Equal(t, "users", ctx.RedactNameForTrace("users"), "disabled redaction must pass through name")
	require.Nil(t, ctx.RedactionMapping(), "disabled context must not allocate a mapping")
}

type stubStringer string

func (s stubStringer) String() string { return string(s) }

func TestTraceRedaction_RedactStringerForTrace(t *testing.T) {
	enabled := NewContext(context.Background())
	require.Containsf(t,
		enabled.RedactStringerForTrace(stubStringer("user_col + 5")),
		"redacted",
		"enabled context should mask SQL fragment")

	disabled := NewContext(context.Background(), WithTraceRedaction(false))
	const original = "user_col + 5"
	require.Equal(t, original, disabled.RedactStringerForTrace(stubStringer(original)),
		"disabled context should return original Stringer text")

	var nilCtx *Context
	require.Equal(t, original, nilCtx.RedactStringerForTrace(stubStringer(original)),
		"nil context should return original Stringer text")
}

func TestTraceRedaction_UnparseableQuery(t *testing.T) {
	ctx := NewContext(context.Background())
	require.Truef(t,
		strings.Contains(ctx.RedactQueryForTrace("not even close to sql ;;"), "unparseable"),
		"expected unparseable marker for invalid SQL")
}
