// Copyright 2020-2024 Dolthub, Inc.
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

func TestWeightString(t *testing.T) {
	testCases := []struct {
		name     string
		args     []interface{}
		expected interface{}
		err      bool
	}{
		// Basic functionality tests
		{"simple string", []interface{}{"ABC"}, []byte{0, 97, 0, 98, 0, 99}, false}, // 'A' -> 'a' (97), 'B' -> 'b' (98), 'C' -> 'c' (99)
		{"lowercase string", []interface{}{"abc"}, []byte{0, 97, 0, 98, 0, 99}, false},
		{"empty string", []interface{}{""}, []byte{}, false},
		{"single char", []interface{}{"A"}, []byte{0, 97}, false},

		// NULL handling
		{"null input", []interface{}{nil}, nil, false},

		// AS CHAR tests
		{"as char with length", []interface{}{"AB", "CHAR", 5}, []byte{0, 97, 0, 98, 0, 32, 0, 32, 0, 32}, false}, // padded with spaces
		{"as char truncate", []interface{}{"ABCDE", "CHAR", 3}, []byte{0, 97, 0, 98, 0, 99}, false},
		{"as char exact length", []interface{}{"ABC", "CHAR", 3}, []byte{0, 97, 0, 98, 0, 99}, false},

		// AS BINARY tests
		{"as binary with length", []interface{}{"AB", "BINARY", 5}, []byte{65, 66, 0, 0, 0}, false}, // padded with null bytes
		{"as binary truncate", []interface{}{"ABCDE", "BINARY", 3}, []byte{65, 66, 67}, false},
		{"as binary exact length", []interface{}{"ABC", "BINARY", 3}, []byte{65, 66, 67}, false},

		// Mixed case handling (should be case-insensitive for character strings)
		{"mixed case", []interface{}{"AbC"}, []byte{0, 97, 0, 98, 0, 99}, false},

		// Special characters
		{"with space", []interface{}{"A B"}, []byte{0, 97, 0, 32, 0, 98}, false},
		{"with numbers", []interface{}{"A1B"}, []byte{0, 97, 0, 49, 0, 98}, false},

		// Unicode characters (simplified handling)
		{"unicode char", []interface{}{"Ä"}, []byte{0, 196}, false},

		// Type conversion
		{"numeric input", []interface{}{123}, []byte{0, 49, 0, 50, 0, 51}, false}, // "123"

		// Edge cases
		{"zero length char", []interface{}{"ABC", "CHAR", 0}, []byte{}, false},
		{"zero length binary", []interface{}{"ABC", "BINARY", 0}, []byte{}, false},
		{"negative length char", []interface{}{"ABC", "CHAR", -1}, []byte{0, 97, 0, 98, 0, 99}, false}, // should ignore negative length
		{"negative length binary", []interface{}{"ABC", "BINARY", -1}, []byte{65, 66, 67}, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			// Convert test args to expressions
			args := make([]sql.Expression, len(tt.args))
			for i, arg := range tt.args {
				if arg == nil {
					args[i] = expression.NewLiteral(nil, types.Null)
				} else {
					switch v := arg.(type) {
					case int:
						args[i] = expression.NewLiteral(int64(v), types.Int64)
					case string:
						args[i] = expression.NewLiteral(v, types.LongText)
					default:
						args[i] = expression.NewLiteral(v, types.LongText)
					}
				}
			}

			f, err := NewWeightString(args...)
			if tt.err {
				require.Error(err)
				return
			}
			require.NoError(err)

			v, err := f.Eval(ctx, nil)
			require.NoError(err)

			if tt.expected == nil {
				require.Nil(v)
			} else {
				require.Equal(tt.expected, v)
			}
		})
	}
}

func TestWeightStringArguments(t *testing.T) {
	require := require.New(t)

	// Test invalid number of arguments
	_, err := NewWeightString()
	require.Error(err)

	// Test too many arguments
	args := make([]sql.Expression, 4)
	for i := range args {
		args[i] = expression.NewLiteral("test", types.Text)
	}
	_, err = NewWeightString(args...)
	require.Error(err)

	// Test valid argument counts
	validArgs := [][]sql.Expression{
		{expression.NewLiteral("test", types.Text)},
		{expression.NewLiteral("test", types.Text), expression.NewLiteral("CHAR", types.Text)},
		{expression.NewLiteral("test", types.Text), expression.NewLiteral("CHAR", types.Text), expression.NewLiteral(5, types.Int64)},
	}

	for _, args := range validArgs {
		_, err := NewWeightString(args...)
		require.NoError(err)
	}
}

func TestWeightStringBinaryDetection(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	// Test with binary literal - we need to test the actual binary behavior
	// In practice, this would be a binary string type expression
	f, err := NewWeightString(expression.NewLiteral("ABC", types.LongText), expression.NewLiteral("BINARY", types.LongText))
	require.NoError(err)

	v, err := f.Eval(ctx, nil)
	require.NoError(err)

	// For binary data, should return the data as-is
	expected := []byte("ABC")
	require.Equal(expected, v)
}

func TestWeightStringCollationWeights(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []byte
	}{
		{"uppercase A", "A", []byte{0, 97}},    // A -> a (case insensitive)
		{"lowercase a", "a", []byte{0, 97}},    // a -> a
		{"uppercase Z", "Z", []byte{0, 122}},   // Z -> z
		{"lowercase z", "z", []byte{0, 122}},   // z -> z
		{"digit 0", "0", []byte{0, 48}},        // 0 -> 0
		{"digit 9", "9", []byte{0, 57}},        // 9 -> 9
		{"space", " ", []byte{0, 32}},          // space -> space
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			f, err := NewWeightString(expression.NewLiteral(tt.input, types.LongText))
			require.NoError(err)

			v, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.expected, v)
		})
	}
}