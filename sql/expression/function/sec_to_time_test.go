// Copyright 2020-2022 Dolthub, Inc.
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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSecToTime(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name:     "Basic conversion: 2378 seconds",
			input:    2378,
			expected: mustParseTime("00:39:38"),
		},
		{
			name:     "One hour",
			input:    3600,
			expected: mustParseTime("01:00:00"),
		},
		{
			name:     "One day",
			input:    86400,
			expected: mustParseTime("24:00:00"),
		},
		{
			name:     "Beyond 24 hours",
			input:    90061,
			expected: mustParseTime("25:01:01"),
		},
		{
			name:     "Zero seconds",
			input:    0,
			expected: mustParseTime("00:00:00"),
		},
		{
			name:     "Negative value",
			input:    -3600,
			expected: mustParseTime("-01:00:00"),
		},
		{
			name:     "Hour, minute, second",
			input:    3661,
			expected: mustParseTime("01:01:01"),
		},
		{
			name:     "Decimal seconds (truncated)",
			input:    1234.5,
			expected: mustParseTime("00:20:34"),
		},
		{
			name:     "NULL value",
			input:    nil,
			expected: nil,
		},
		{
			name:     "String number",
			input:    "3600",
			expected: mustParseTime("01:00:00"),
		},
		{
			name:     "Large value",
			input:    838*3600 + 59*60 + 59,
			expected: mustParseTime("838:59:59"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			f := NewSecToTime(expression.NewLiteral(tt.input, types.LongText))
			result, err := f.Eval(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSecToTimeType(t *testing.T) {
	f := NewSecToTime(expression.NewLiteral(3600, types.Int64))
	require.Equal(t, types.Time, f.Type())
}

func TestSecToTimeNullable(t *testing.T) {
	f := NewSecToTime(expression.NewLiteral(nil, types.Int64))
	require.True(t, f.IsNullable())
}

func TestSecToTimeWithChildren(t *testing.T) {
	f := NewSecToTime(expression.NewLiteral(3600, types.Int64))
	newF, err := f.WithChildren(expression.NewLiteral(7200, types.Int64))
	require.NoError(t, err)
	require.IsType(t, &SecToTime{}, newF)
}

func TestSecToTimeString(t *testing.T) {
	f := NewSecToTime(expression.NewLiteral(3600, types.Int64))
	require.Equal(t, "SEC_TO_TIME(3600)", f.String())
}

// Helper function to parse time strings for testing
func mustParseTime(timeStr string) types.Timespan {
	ctx := sql.NewEmptyContext()
	t, _, err := types.Time.Convert(ctx, timeStr)
	if err != nil {
		panic(err)
	}
	return t.(types.Timespan)
}