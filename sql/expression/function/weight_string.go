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
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// WeightString implements the SQL function WEIGHT_STRING() which returns a binary representation
// of a string's comparison and sorting value (collation weight)
type WeightString struct {
	str    sql.Expression
	asType sql.Expression // CHAR or BINARY
	length sql.Expression // length parameter for AS clause
}

var _ sql.FunctionExpression = (*WeightString)(nil)
var _ sql.CollationCoercible = (*WeightString)(nil)

// NewWeightString creates a new WeightString expression
func NewWeightString(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("WEIGHT_STRING", "1, 2, or 3", len(args))
	}

	var asType, length sql.Expression
	if len(args) >= 2 {
		asType = args[1]
	}
	if len(args) == 3 {
		length = args[2]
	}

	return &WeightString{
		str:    args[0],
		asType: asType,
		length: length,
	}, nil
}

// FunctionName implements sql.FunctionExpression
func (w *WeightString) FunctionName() string {
	return "weight_string"
}

// Description implements sql.FunctionExpression
func (w *WeightString) Description() string {
	return "returns a binary string representing the comparison and sorting value of the input string."
}

// Children implements the Expression interface
func (w *WeightString) Children() []sql.Expression {
	children := []sql.Expression{w.str}
	if w.asType != nil {
		children = append(children, w.asType)
	}
	if w.length != nil {
		children = append(children, w.length)
	}
	return children
}

// Resolved implements the Expression interface
func (w *WeightString) Resolved() bool {
	for _, child := range w.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

// IsNullable implements the Expression interface
func (w *WeightString) IsNullable() bool {
	return w.str.IsNullable()
}

// Type implements the Expression interface
func (w *WeightString) Type() sql.Type {
	return types.LongBlob
}

// CollationCoercibility implements the interface sql.CollationCoercible
func (w *WeightString) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	// Weight string returns binary data, so use binary collation
	return sql.Collation_binary, 4
}

// String implements the Expression interface
func (w *WeightString) String() string {
	children := w.Children()
	childStrs := make([]string, len(children))
	for i, child := range children {
		childStrs[i] = child.String()
	}
	return fmt.Sprintf("weight_string(%s)", strings.Join(childStrs, ", "))
}

// WithChildren implements the Expression interface
func (w *WeightString) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewWeightString(children...)
}

// Eval implements the Expression interface
func (w *WeightString) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	strVal, err := w.str.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if strVal == nil {
		return nil, nil
	}

	// Convert to string
	strText, _, err := types.LongText.Convert(ctx, strVal)
	if err != nil {
		return nil, err
	}
	inputStr := strText.(string)

	// Handle AS clause if present
	var targetLength int64 = -1
	var asBinary bool = false

	if w.asType != nil {
		asTypeVal, err := w.asType.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if asTypeVal != nil {
			asTypeStr, _, err := types.LongText.Convert(ctx, asTypeVal)
			if err != nil {
				return nil, err
			}
			asTypeString := strings.ToUpper(asTypeStr.(string))
			if asTypeString == "BINARY" {
				asBinary = true
			}
		}

		// Get length if specified
		if w.length != nil {
			lengthVal, err := w.length.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			if lengthVal != nil {
				lengthInt, _, err := types.Int64.Convert(ctx, lengthVal)
				if err != nil {
					return nil, err
				}
				targetLength = lengthInt.(int64)
			}
		}
	}

	// Apply length and type conversion if specified
	var processedStr string
	if targetLength >= 0 {
		if asBinary {
			// For BINARY, pad with null bytes (0x00)
			if len(inputStr) < int(targetLength) {
				processedStr = inputStr + strings.Repeat("\x00", int(targetLength)-len(inputStr))
			} else {
				processedStr = inputStr[:targetLength]
			}
		} else {
			// For CHAR, pad with spaces
			if len(inputStr) < int(targetLength) {
				processedStr = inputStr + strings.Repeat(" ", int(targetLength)-len(inputStr))
			} else {
				processedStr = inputStr[:targetLength]
			}
		}
	} else {
		processedStr = inputStr
	}

	// Generate weight string based on collation
	// For binary strings or when AS BINARY is specified, return the string as-is
	if asBinary || w.isBinaryString(ctx, w.str) {
		result := []byte(processedStr)
		if result == nil {
			result = []byte{}
		}
		return result, nil
	}

	// For character strings, generate collation weights
	// This is a simplified implementation - real MySQL uses complex collation algorithms
	weightBytes := w.generateCollationWeights(ctx, processedStr)
	if weightBytes == nil {
		weightBytes = []byte{}
	}
	return weightBytes, nil
}

// isBinaryString checks if the expression represents a binary string
func (w *WeightString) isBinaryString(ctx *sql.Context, expr sql.Expression) bool {
	exprType := expr.Type()
	// Check if it's a binary type
	switch exprType.Type() {
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		return true
	}
	
	// Don't treat text or numeric types as binary
	switch exprType.Type() {
	case sqltypes.Text, sqltypes.VarChar, sqltypes.Char:
		return false
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		return false
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return false
	case sqltypes.Float32, sqltypes.Float64, sqltypes.Decimal:
		return false
	}
	
	// Check if it has binary collation (but only for string types)
	if collExpr, ok := expr.(sql.CollationCoercible); ok {
		collation, _ := collExpr.CollationCoercibility(ctx)
		return collation == sql.Collation_binary
	}
	
	return false
}

// generateCollationWeights generates simplified collation weights for character strings
func (w *WeightString) generateCollationWeights(ctx *sql.Context, str string) []byte {
	// This is a simplified implementation of collation weight generation
	// Real MySQL uses complex Unicode Collation Algorithm (UCA) weights
	
	var weights []byte
	
	for _, r := range str {
		// For simplicity, use a basic weight calculation
		// In practice, MySQL uses sophisticated collation tables
		if r <= 127 {
			// ASCII characters - use a simple case-insensitive weight
			if r >= 'A' && r <= 'Z' {
				r = r + 32 // Convert to lowercase for case-insensitive comparison
			}
			// Add primary weight
			weights = append(weights, byte(r>>8), byte(r&0xFF))
		} else {
			// Unicode characters - simplified weight
			weights = append(weights, byte(r>>8), byte(r&0xFF))
		}
	}
	
	return weights
}