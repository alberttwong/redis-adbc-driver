// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"fmt"
	"math/big"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// valueAt extracts row i of an Arrow array as a Value.
func valueAt(arr arrow.Array, i int) (Value, error) {
	if dict, ok := arr.(*array.Dictionary); ok {
		if dict.IsNull(i) {
			ct, err := colTypeFromArrow(dict.DataType())
			if err != nil {
				return Value{}, err
			}
			return nullValue(ct), nil
		}
		return valueAt(dict.Dictionary(), dict.GetValueIndex(i))
	}
	if arr.DataType().ID() == arrow.NULL {
		return nullValue(typeNull), nil
	}
	ct, err := colTypeFromArrow(arr.DataType())
	if err != nil {
		return Value{}, err
	}
	if arr.IsNull(i) {
		return nullValue(ct), nil
	}
	switch a := arr.(type) {
	case *array.Boolean:
		return boolValue(a.Value(i)), nil
	case *array.Int8:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Int16:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Int32:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Int64:
		return intValue(ct, a.Value(i)), nil
	case *array.Uint8:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Uint16:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Uint32:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Uint64:
		v := a.Value(i)
		if v > 1<<63-1 {
			return Value{}, fmt.Errorf("uint64 value %d out of range", v)
		}
		return intValue(ct, int64(v)), nil
	case *array.Float16:
		return floatValue(ct, float64(a.Value(i).Float32())), nil
	case *array.Float32:
		return floatValue(ct, float64(a.Value(i))), nil
	case *array.Float64:
		return floatValue(ct, a.Value(i)), nil
	case *array.Decimal32:
		return decimalValue(big.NewInt(int64(a.Value(i))), ct.Precision, ct.Scale), nil
	case *array.Decimal64:
		return decimalValue(big.NewInt(int64(a.Value(i))), ct.Precision, ct.Scale), nil
	case *array.Decimal128:
		return decimalValue(a.Value(i).BigInt(), ct.Precision, ct.Scale), nil
	case *array.Decimal256:
		return decimalValue(a.Value(i).BigInt(), ct.Precision, ct.Scale), nil
	case *array.String:
		return stringValue(a.Value(i)), nil
	case *array.LargeString:
		return stringValue(a.Value(i)), nil
	case *array.StringView:
		return stringValue(a.Value(i)), nil
	case *array.Binary:
		return binaryValue(string(a.Value(i))), nil
	case *array.LargeBinary:
		return binaryValue(string(a.Value(i))), nil
	case *array.BinaryView:
		return binaryValue(string(a.Value(i))), nil
	case *array.FixedSizeBinary:
		return binaryValue(string(a.Value(i))), nil
	case *array.Date32:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Date64:
		return intValue(ct, floorDiv(int64(a.Value(i)), 86_400_000)), nil
	case *array.Time32:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Time64:
		return intValue(ct, int64(a.Value(i))), nil
	case *array.Timestamp:
		return intValue(ct, int64(a.Value(i))), nil
	}
	return Value{}, fmt.Errorf("unsupported Arrow type %s", arr.DataType())
}

// appendValue appends a value (already coerced to t) to a builder created
// for t.ArrowType().
func appendValue(b array.Builder, t ColType, v Value) error {
	if v.Null {
		b.AppendNull()
		return nil
	}
	switch bb := b.(type) {
	case *array.NullBuilder:
		bb.AppendNull()
	case *array.BooleanBuilder:
		bb.Append(v.I != 0)
	case *array.Int16Builder:
		bb.Append(int16(v.I))
	case *array.Int32Builder:
		bb.Append(int32(v.I))
	case *array.Int64Builder:
		bb.Append(v.I)
	case *array.Float32Builder:
		bb.Append(float32(v.F))
	case *array.Float64Builder:
		bb.Append(v.F)
	case *array.Decimal128Builder:
		bb.Append(decimal128.FromBigInt(v.D))
	case *array.Decimal256Builder:
		bb.Append(decimal256.FromBigInt(v.D))
	case *array.StringBuilder:
		bb.Append(v.S)
	case *array.BinaryBuilder:
		bb.Append([]byte(v.S))
	case *array.Date32Builder:
		bb.Append(arrow.Date32(v.I))
	case *array.Time32Builder:
		bb.Append(arrow.Time32(v.I))
	case *array.Time64Builder:
		bb.Append(arrow.Time64(v.I))
	case *array.TimestampBuilder:
		bb.Append(arrow.Timestamp(v.I))
	default:
		return fmt.Errorf("unsupported builder %T for %s", b, t.Kind)
	}
	return nil
}

// resultColumn describes one column of a result set.
type resultColumn struct {
	Name string
	Type ColType
}

func resultSchema(cols []resultColumn) *arrow.Schema {
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{Name: c.Name, Type: c.Type.ArrowType(), Nullable: true}
	}
	return arrow.NewSchema(fields, nil)
}

// buildRecord converts rows of values into an Arrow record batch.
func buildRecord(mem memory.Allocator, cols []resultColumn, rows [][]Value) (arrow.RecordBatch, error) {
	schema := resultSchema(cols)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	for _, row := range rows {
		for j, c := range cols {
			v, err := Coerce(row[j], c.Type)
			if err != nil {
				return nil, err
			}
			if err := appendValue(rb.Field(j), c.Type, v); err != nil {
				return nil, err
			}
		}
	}
	return rb.NewRecordBatch(), nil
}

// recordsReader builds a RecordReader over rows, chunked into batches.
func recordsReader(mem memory.Allocator, cols []resultColumn, rows [][]Value) (array.RecordReader, error) {
	const batchSize = 65536
	var recs []arrow.RecordBatch
	for start := 0; start < len(rows) || (start == 0 && len(rows) == 0); start += batchSize {
		end := min(start+batchSize, len(rows))
		rec, err := buildRecord(mem, cols, rows[start:end])
		if err != nil {
			for _, r := range recs {
				r.Release()
			}
			return nil, err
		}
		recs = append(recs, rec)
		if len(rows) == 0 {
			break
		}
	}
	rdr, err := array.NewRecordReader(resultSchema(cols), recs)
	for _, r := range recs {
		r.Release()
	}
	return rdr, err
}
