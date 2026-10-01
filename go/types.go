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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// Kind is the logical type of a column or value.
type Kind int

const (
	KindNull Kind = iota
	KindBool
	KindInt16
	KindInt32
	KindInt64
	KindFloat32
	KindFloat64
	KindDecimal
	KindString
	KindBinary
	KindDate
	KindTime
	KindTimestamp
	KindInterval
)

var kindNames = map[Kind]string{
	KindNull:      "null",
	KindBool:      "bool",
	KindInt16:     "int16",
	KindInt32:     "int32",
	KindInt64:     "int64",
	KindFloat32:   "float32",
	KindFloat64:   "float64",
	KindDecimal:   "decimal",
	KindString:    "string",
	KindBinary:    "binary",
	KindDate:      "date",
	KindTime:      "time",
	KindTimestamp: "timestamp",
	KindInterval:  "interval",
}

func (k Kind) String() string { return kindNames[k] }

func (k Kind) isInteger() bool { return k == KindInt16 || k == KindInt32 || k == KindInt64 }
func (k Kind) isFloat() bool   { return k == KindFloat32 || k == KindFloat64 }
func (k Kind) isNumeric() bool { return k.isInteger() || k.isFloat() || k == KindDecimal }

// indexedAsNumeric reports whether values of this kind are stored as numbers
// in the row HASH and therefore indexed as NUMERIC SORTABLE fields.
func (k Kind) indexedAsNumeric() bool {
	switch k {
	case KindBool, KindInt16, KindInt32, KindInt64, KindFloat32, KindFloat64,
		KindDecimal, KindDate, KindTime, KindTimestamp:
		return true
	}
	return false
}

// ColType is the logical type of a column; it is persisted in the table
// metadata and maps 1:1 onto an Arrow type.
type ColType struct {
	Kind      Kind
	Unit      arrow.TimeUnit // time and timestamp
	TZ        string         // timestamp: "" (naive) or "UTC"
	Precision int32          // decimal
	Scale     int32          // decimal
	// Strings: Length is the declared length of VARCHAR(n) / CHAR(n), 0
	// without one, and Fixed marks the blank-padded CHAR types (see
	// lengths.go).
	Length int32
	Fixed  bool
}

type colTypeJSON struct {
	Kind      string `json:"kind"`
	Unit      string `json:"unit,omitempty"`
	TZ        string `json:"tz,omitempty"`
	Precision int32  `json:"precision,omitempty"`
	Scale     int32  `json:"scale,omitempty"`
	Length    int32  `json:"length,omitempty"`
	Fixed     bool   `json:"fixed,omitempty"`
}

var unitNames = map[arrow.TimeUnit]string{
	arrow.Second:      "s",
	arrow.Millisecond: "ms",
	arrow.Microsecond: "us",
	arrow.Nanosecond:  "ns",
}

func (t ColType) MarshalJSON() ([]byte, error) {
	j := colTypeJSON{Kind: t.Kind.String()}
	if t.Kind == KindTime || t.Kind == KindTimestamp {
		j.Unit = unitNames[t.Unit]
		j.TZ = t.TZ
	}
	if t.Kind == KindDecimal {
		j.Precision = t.Precision
		j.Scale = t.Scale
	}
	if t.Kind == KindString {
		j.Length = t.Length
		j.Fixed = t.Fixed
	}
	return json.Marshal(j)
}

func (t *ColType) UnmarshalJSON(data []byte) error {
	var j colTypeJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	found := false
	for k, name := range kindNames {
		if name == j.Kind {
			t.Kind = k
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("unknown column kind %q", j.Kind)
	}
	for u, name := range unitNames {
		if name == j.Unit {
			t.Unit = u
		}
	}
	t.TZ = j.TZ
	t.Precision = j.Precision
	t.Scale = j.Scale
	t.Length = j.Length
	t.Fixed = j.Fixed
	return nil
}

var (
	typeNull     = ColType{Kind: KindNull}
	typeBool     = ColType{Kind: KindBool}
	typeInt16    = ColType{Kind: KindInt16}
	typeInt32    = ColType{Kind: KindInt32}
	typeInt64    = ColType{Kind: KindInt64}
	typeFloat32  = ColType{Kind: KindFloat32}
	typeFloat64  = ColType{Kind: KindFloat64}
	typeString   = ColType{Kind: KindString}
	typeBinary   = ColType{Kind: KindBinary}
	typeDate     = ColType{Kind: KindDate}
	typeInterval = ColType{Kind: KindInterval}
)

func timeType(unit arrow.TimeUnit) ColType { return ColType{Kind: KindTime, Unit: unit} }

func timestampType(unit arrow.TimeUnit, tz string) ColType {
	return ColType{Kind: KindTimestamp, Unit: unit, TZ: tz}
}

func decimalType(precision, scale int32) ColType {
	return ColType{Kind: KindDecimal, Precision: precision, Scale: scale}
}

// unitForPrecision maps SQL fractional-second precision onto an Arrow unit.
func unitForPrecision(p int) arrow.TimeUnit {
	switch {
	case p <= 0:
		return arrow.Second
	case p <= 3:
		return arrow.Millisecond
	case p <= 6:
		return arrow.Microsecond
	default:
		return arrow.Nanosecond
	}
}

func precisionForUnit(u arrow.TimeUnit) int {
	switch u {
	case arrow.Second:
		return 0
	case arrow.Millisecond:
		return 3
	case arrow.Microsecond:
		return 6
	default:
		return 9
	}
}

// ArrowType returns the Arrow type used to represent this column.
func (t ColType) ArrowType() arrow.DataType {
	switch t.Kind {
	case KindBool:
		return arrow.FixedWidthTypes.Boolean
	case KindInt16:
		return arrow.PrimitiveTypes.Int16
	case KindInt32:
		return arrow.PrimitiveTypes.Int32
	case KindInt64:
		return arrow.PrimitiveTypes.Int64
	case KindFloat32:
		return arrow.PrimitiveTypes.Float32
	case KindFloat64:
		return arrow.PrimitiveTypes.Float64
	case KindDecimal:
		if t.Precision <= 38 {
			return &arrow.Decimal128Type{Precision: t.Precision, Scale: t.Scale}
		}
		return &arrow.Decimal256Type{Precision: t.Precision, Scale: t.Scale}
	case KindString:
		return arrow.BinaryTypes.String
	case KindBinary:
		return arrow.BinaryTypes.Binary
	case KindDate:
		return arrow.FixedWidthTypes.Date32
	case KindTime:
		if t.Unit == arrow.Second || t.Unit == arrow.Millisecond {
			return &arrow.Time32Type{Unit: t.Unit}
		}
		return &arrow.Time64Type{Unit: t.Unit}
	case KindTimestamp:
		return &arrow.TimestampType{Unit: t.Unit, TimeZone: t.TZ}
	case KindInterval:
		return arrow.FixedWidthTypes.MonthDayNanoInterval
	default:
		return arrow.Null
	}
}

// SQLName returns the SQL type name reported through GetObjects.
func (t ColType) SQLName() string {
	switch t.Kind {
	case KindBool:
		return "BOOLEAN"
	case KindInt16:
		return "SMALLINT"
	case KindInt32:
		return "INTEGER"
	case KindInt64:
		return "BIGINT"
	case KindFloat32:
		return "REAL"
	case KindFloat64:
		return "DOUBLE PRECISION"
	case KindDecimal:
		return fmt.Sprintf("NUMERIC(%d,%d)", t.Precision, t.Scale)
	case KindString:
		switch {
		case t.Fixed && t.Length > 0:
			return fmt.Sprintf("CHAR(%d)", t.Length)
		case t.Fixed:
			return "BPCHAR"
		case t.Length > 0:
			return fmt.Sprintf("VARCHAR(%d)", t.Length)
		}
		return "VARCHAR"
	case KindBinary:
		return "VARBINARY"
	case KindDate:
		return "DATE"
	case KindTime:
		return fmt.Sprintf("TIME(%d)", precisionForUnit(t.Unit))
	case KindTimestamp:
		if t.TZ != "" {
			return fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", precisionForUnit(t.Unit))
		}
		return fmt.Sprintf("TIMESTAMP(%d)", precisionForUnit(t.Unit))
	case KindInterval:
		return "INTERVAL"
	default:
		return "NULL"
	}
}

// colTypeFromArrow maps an Arrow type (from bulk ingest) onto a column type.
func colTypeFromArrow(dt arrow.DataType) (ColType, error) {
	switch t := dt.(type) {
	case *arrow.DictionaryType:
		return colTypeFromArrow(t.ValueType)
	case *arrow.BooleanType:
		return typeBool, nil
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Uint8Type:
		return typeInt16, nil
	case *arrow.Int32Type, *arrow.Uint16Type:
		return typeInt32, nil
	case *arrow.Int64Type, *arrow.Uint32Type, *arrow.Uint64Type:
		return typeInt64, nil
	case *arrow.Float16Type, *arrow.Float32Type:
		return typeFloat32, nil
	case *arrow.Float64Type:
		return typeFloat64, nil
	case *arrow.Decimal32Type:
		return decimalType(t.Precision, t.Scale), nil
	case *arrow.Decimal64Type:
		return decimalType(t.Precision, t.Scale), nil
	case *arrow.Decimal128Type:
		return decimalType(t.Precision, t.Scale), nil
	case *arrow.Decimal256Type:
		return decimalType(t.Precision, t.Scale), nil
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return typeString, nil
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		return typeBinary, nil
	case *arrow.Date32Type, *arrow.Date64Type:
		return typeDate, nil
	case *arrow.Time32Type:
		return timeType(t.Unit), nil
	case *arrow.Time64Type:
		return timeType(t.Unit), nil
	case *arrow.TimestampType:
		tz := ""
		if t.TimeZone != "" {
			// Instants are stored normalized to UTC.
			tz = "UTC"
		}
		return timestampType(t.Unit, tz), nil
	case *arrow.MonthDayNanoIntervalType, *arrow.MonthIntervalType, *arrow.DayTimeIntervalType, *arrow.DurationType:
		return typeInterval, nil
	case *arrow.NullType:
		return typeString, nil
	}
	return ColType{}, fmt.Errorf("unsupported Arrow type %s", dt)
}

// sqlTypeSpec is a parsed SQL type name, e.g. NUMERIC(10, 2).
type sqlTypeSpec struct {
	Name   string // upper-cased, multi-word names joined by a single space
	Params []int
	WithTZ bool
}

func colTypeFromSQL(spec sqlTypeSpec) (ColType, error) {
	param := func(i, def int) int {
		if i < len(spec.Params) {
			return spec.Params[i]
		}
		return def
	}
	switch spec.Name {
	case "BOOLEAN", "BOOL":
		return typeBool, nil
	case "SMALLINT", "INT2", "TINYINT":
		return typeInt16, nil
	case "INTEGER", "INT", "INT4", "MEDIUMINT":
		return typeInt32, nil
	case "BIGINT", "INT8", "LONG":
		return typeInt64, nil
	case "REAL", "FLOAT4":
		return typeFloat32, nil
	case "DOUBLE PRECISION", "DOUBLE", "FLOAT8":
		return typeFloat64, nil
	case "FLOAT":
		if len(spec.Params) > 0 && spec.Params[0] <= 24 {
			return typeFloat32, nil
		}
		return typeFloat64, nil
	case "NUMERIC", "DECIMAL", "DEC":
		p := param(0, 38)
		s := param(1, 0)
		if len(spec.Params) == 0 {
			s = 9
		}
		if p < 1 || p > 76 {
			return ColType{}, fmt.Errorf("invalid NUMERIC precision %d", p)
		}
		return decimalType(int32(p), int32(s)), nil
	case "VARCHAR", "CHARACTER VARYING", "NVARCHAR":
		return stringTypeFromSQL(spec, false)
	case "CHAR", "CHARACTER", "NCHAR", "BPCHAR":
		return stringTypeFromSQL(spec, true)
	case "TEXT", "STRING", "CLOB":
		return typeString, nil
	case "VARBINARY", "BLOB", "BYTEA", "BINARY", "BYTES", "BINARY VARYING":
		return typeBinary, nil
	case "DATE":
		return typeDate, nil
	case "TIME":
		return timeType(unitForPrecision(param(0, 6))), nil
	case "TIMESTAMP", "DATETIME":
		tz := ""
		if spec.WithTZ {
			tz = "UTC"
		}
		return timestampType(unitForPrecision(param(0, 6)), tz), nil
	case "TIMESTAMPTZ":
		return timestampType(unitForPrecision(param(0, 6)), "UTC"), nil
	case "INTERVAL":
		return typeInterval, nil
	}
	return ColType{}, fmt.Errorf("unsupported SQL type %s", strings.ToUpper(spec.Name))
}
