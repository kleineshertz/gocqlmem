package eval_gocqlmem

import (
	"fmt"
	"strconv"

	"github.com/capillariesio/cqlmem/eval"
	"github.com/shopspring/decimal"
)

var GocqlmemEvalFunctions = map[string]eval.EvalFunction{
	"cast": callCast,
}

/*
ascii	text, varchar
bigint	tinyint, smallint, int, float, double, decimal, varint, text, varchar
boolean	text, varchar
counter	tinyint, smallint, int, bigint, float, double, decimal, varint, text, varchar
date	timestamp
decimal	tinyint, smallint, int, bigint, float, double, varint, text, varchar
double	tinyint, smallint, int, bigint, float, decimal, varint, text, varchar
float	tinyint, smallint, int, bigint, double, decimal, varint, text, varchar
inet	text, varchar
int	tinyint, smallint, bigint, float, double, decimal, varint, text, varchar
smallint	tinyint, int, bigint, float, double, decimal, varint, text, varchar
time	text, varchar
timestamp	date, text, varchar
timeuuid	timestamp, date, text, varchar
tinyint	tinyint, smallint, int, bigint, float, double, decimal, varint, text, varchar
uuid	text, varchar
varint	tinyint, smallint, int, bigint, float, double, decimal, text, varchar
*/
func callCast(args []any) (any, error) {
	if err := eval.CheckArgs("cast", 2, len(args)); err != nil {
		return nil, err
	}

	dataType, ok := args[1].(DataType)
	if !ok {
		return nil, fmt.Errorf("cannot convert cast() arg %v to DataType", args[1])
	}

	switch args[0].(type) {
	case int64, int32, int16, int8:
		typedVal, err := eval.CastToInt64(args[0])
		if err != nil {
			return nil, fmt.Errorf("cannot cast int %v to %v: %s", typedVal, dataType, err.Error())
		}
		switch dataType {
		case DataTypeBigint, DataTypeSmallint, DataTypeTinyint, DataTypeInt, DataTypeVarint:
			return int64(typedVal), nil
		case DataTypeFloat, DataTypeDouble:
			return float64(typedVal), nil
		case DataTypeDecimal:
			return decimal.NewFromInt(typedVal), nil
		case DataTypeText, DataTypeVarchar:
			return strconv.FormatInt(typedVal, 10), nil
		default:
			return nil, fmt.Errorf("cannot cast int %v to %v", typedVal, dataType)
		}
	case float32, float64:
		typedVal, err := eval.CastToFloat64(args[0])
		if err != nil {
			return nil, fmt.Errorf("cannot cast float %v to %v: %s", typedVal, dataType, err.Error())
		}
		switch dataType {
		case DataTypeBigint, DataTypeSmallint, DataTypeTinyint, DataTypeInt, DataTypeVarint:
			return float64(typedVal), nil
		case DataTypeFloat, DataTypeDouble:
			return float64(typedVal), nil
		case DataTypeDecimal:
			return decimal.NewFromFloat(typedVal), nil
		case DataTypeText, DataTypeVarchar:
			return strconv.FormatFloat(typedVal, 'f', -1, 64), nil
		default:
			return nil, fmt.Errorf("cannot cast float %v to %v", typedVal, dataType)
		}
	case bool:
		typedVal, ok := args[0].(bool)
		if !ok {
			return nil, fmt.Errorf("cannot cast bool %v to bool", typedVal)
		}
		switch dataType {
		case DataTypeBoolean:
			return typedVal, nil
		case DataTypeText, DataTypeVarchar:
			if typedVal {
				return "TRUE", nil
			} else {
				return "FALSE", nil
			}
		default:
			return nil, fmt.Errorf("cannot cast bool %v to %v", typedVal, dataType)
		}
	case decimal.Decimal:
		typedVal, err := eval.CastToDecimal(args[0])
		if err != nil {
			return nil, fmt.Errorf("cannot cast decimal %v to %v: %s", typedVal, dataType, err.Error())
		}
		switch dataType {
		case DataTypeBigint, DataTypeSmallint, DataTypeTinyint, DataTypeInt, DataTypeVarint:
			return typedVal.BigInt().Int64(), nil
		case DataTypeFloat, DataTypeDouble:
			floatVal, _ := typedVal.Float64()
			return floatVal, nil
		case DataTypeDecimal:
			return typedVal, nil
		case DataTypeText, DataTypeVarchar:
			return typedVal.String(), nil
		default:
			return nil, fmt.Errorf("cannot cast decimal %v to %v", typedVal, dataType)
		}
	default:
		return nil, fmt.Errorf("cannot cast %v to %v, unsupported source type", args[0], dataType)
	}
}
