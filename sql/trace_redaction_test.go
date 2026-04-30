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
)

func TestTraceRedaction_DefaultEnabled(t *testing.T) {
	ctx := NewContext(context.Background())
	if !ctx.TraceRedactionEnabled() {
		t.Fatalf("redaction must be enabled by default")
	}
}

func TestTraceRedaction_WithTraceRedactionFalse_OptsOut(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(false))
	if ctx.TraceRedactionEnabled() {
		t.Fatalf("WithTraceRedaction(false) should disable redaction")
	}
}

func TestTraceRedaction_WithTraceRedactionTrue_Explicit(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(true))
	if !ctx.TraceRedactionEnabled() {
		t.Fatalf("WithTraceRedaction(true) must keep redaction enabled")
	}
}

func TestTraceRedaction_NilContext(t *testing.T) {
	var ctx *Context
	if !ctx.TraceRedactionEnabled() {
		t.Fatalf("nil context should report redaction enabled (secure default)")
	}
	if got := ctx.RedactNameForTrace("foo"); got != "foo" {
		t.Fatalf("nil context name redaction should pass through, got %q", got)
	}
	if got := ctx.RedactQueryForTrace("SELECT 1"); got != "SELECT 1" {
		t.Fatalf("nil context query redaction should pass through, got %q", got)
	}
}

func TestTraceRedaction_RedactQueryForTracePopulatesMapping(t *testing.T) {
	ctx := NewContext(context.Background())
	got := ctx.RedactQueryForTrace("SELECT a FROM t WHERE x = 1")
	if strings.Contains(got, "FROM t") || strings.Contains(got, "from t ") {
		t.Fatalf("table name not redacted: %q", got)
	}
	m := ctx.RedactionMapping()
	if m == nil {
		t.Fatalf("mapping must be populated after RedactQueryForTrace")
	}
}

func TestTraceRedaction_NameUsesParsedMapping(t *testing.T) {
	ctx := NewContext(context.Background())
	_ = ctx.RedactQueryForTrace("SELECT a FROM users WHERE id = 1")
	tok := ctx.RedactNameForTrace("users")
	if tok == "users" {
		t.Fatalf("expected redacted token for 'users', got original")
	}
	// Same name should return the same token across calls.
	if got := ctx.RedactNameForTrace("users"); got != tok {
		t.Fatalf("name redaction unstable: first %q, second %q", tok, got)
	}
}

func TestTraceRedaction_NameWithoutParsePhase(t *testing.T) {
	// rowexec spans may fire on contexts where no SQL was parsed.
	// Name redaction should still mint a stable token.
	ctx := NewContext(context.Background())
	a := ctx.RedactNameForTrace("orphan_table")
	if a == "orphan_table" {
		t.Fatalf("expected token, got original")
	}
	b := ctx.RedactNameForTrace("orphan_table")
	if a != b {
		t.Fatalf("token not stable across calls: %q vs %q", a, b)
	}
}

func TestTraceRedaction_DisabledPassesThrough(t *testing.T) {
	ctx := NewContext(context.Background(), WithTraceRedaction(false))
	const q = "SELECT a FROM t WHERE x = 1"
	if got := ctx.RedactQueryForTrace(q); got != q {
		t.Fatalf("disabled redaction must pass through, got %q", got)
	}
	if got := ctx.RedactNameForTrace("users"); got != "users" {
		t.Fatalf("disabled redaction must pass through name, got %q", got)
	}
	if ctx.RedactionMapping() != nil {
		t.Fatalf("disabled context must not allocate a mapping")
	}
}

func TestTraceRedaction_UnparseableQuery(t *testing.T) {
	ctx := NewContext(context.Background())
	got := ctx.RedactQueryForTrace("not even close to sql ;;")
	if !strings.Contains(got, "unparseable") {
		t.Fatalf("expected unparseable marker, got %q", got)
	}
}
