// Copyright 2026 Dolthub, Inc.
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

package plan_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func leftDeepUnion(parts []sql.Schema) sql.Node {
	var n sql.Node = plan.NewEmptyTableWithSchema(parts[0])
	for _, s := range parts[1:] {
		n = plan.NewSetOp(plan.UnionType, n, plan.NewEmptyTableWithSchema(s), false, nil, nil, nil)
	}
	return n
}

func TestSetOpSchema(t *testing.T) {
	col := func(nullable bool) sql.Schema {
		return sql.Schema{{Name: "a", Type: types.Int64, Source: "t", Nullable: nullable}}
	}
	tests := []struct {
		name         string
		node         sql.Node
		wantNullable bool
	}{
		{
			name:         "no nullable part",
			node:         leftDeepUnion([]sql.Schema{col(false), col(false), col(false)}),
			wantNullable: false,
		},
		{
			name:         "nullable part at the bottom of the chain",
			node:         leftDeepUnion([]sql.Schema{col(false), col(true), col(false), col(false)}),
			wantNullable: true,
		},
		{
			name:         "nullable part at the top of the chain",
			node:         leftDeepUnion([]sql.Schema{col(false), col(false), col(false), col(true)}),
			wantNullable: true,
		},
		{
			name:         "nullable first part",
			node:         leftDeepUnion([]sql.Schema{col(true), col(false), col(false)}),
			wantNullable: true,
		},
		{
			name: "nullable part in a nested right chain",
			node: plan.NewSetOp(plan.UnionType,
				leftDeepUnion([]sql.Schema{col(false), col(false)}),
				leftDeepUnion([]sql.Schema{col(false), col(true)}),
				false, nil, nil, nil),
			wantNullable: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.node.Schema()
			require.Len(t, got, 1)
			require.Equal(t, "a", got[0].Name)
			require.Equal(t, "t", got[0].Source)
			require.Equal(t, tc.wantNullable, got[0].Nullable)
		})
	}
}

// The runtime never frees a memory-profile bucket, so a walk that calls Schema at every level of
// a UNION chain must add allocation stacks per walk depth, not per pair of walk and Schema depth.
func TestSetOpSchemaProfileStacks(t *testing.T) {
	const parts = 25
	schemas := make([]sql.Schema, parts)
	for i := range schemas {
		schemas[i] = sql.Schema{
			{Name: "a", Type: types.Int64, Source: "t"},
			{Name: "b", Type: types.Text, Source: "t"},
		}
	}
	chain := leftDeepUnion(schemas)

	var walk func(n sql.Node)
	walk = func(n sql.Node) {
		n.Schema()
		if so, ok := n.(*plan.SetOp); ok {
			walk(so.Left())
		}
	}

	rate := runtime.MemProfileRate
	t.Cleanup(func() { runtime.MemProfileRate = rate })
	runtime.MemProfileRate = 1

	before := setOpSchemaStacks()
	walk(chain)
	after := setOpSchemaStacks()

	require.LessOrEqual(t, after-before, 4*parts)
}

func setOpSchemaStacks() int {
	runtime.GC()
	runtime.GC()
	n, _ := runtime.MemProfile(nil, true)
	records := make([]runtime.MemProfileRecord, n+1024)
	n, _ = runtime.MemProfile(records, true)
	count := 0
	for _, r := range records[:n] {
		frames := runtime.CallersFrames(r.Stack())
		for {
			f, more := frames.Next()
			if strings.Contains(f.Function, "plan.(*SetOp).Schema") {
				count++
				break
			}
			if !more {
				break
			}
		}
	}
	return count
}
