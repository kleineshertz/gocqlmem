package eval_gocqlmem

import (
	"cmp"
	"fmt"
	"strings"

	"github.com/capillariesio/gocqlmem/eval"
)

type DataType string

const (
	DataTypeAscii     DataType = "ascii"
	DataTypeBigint    DataType = "bigint"
	DataTypeBlob      DataType = "blob"
	DataTypeBoolean   DataType = "boolean"
	DataTypeCounter   DataType = "counter"
	DataTypeDate      DataType = "date"
	DataTypeDecimal   DataType = "decimal"
	DataTypeDouble    DataType = "double"
	DataTypeDuration  DataType = "duration"
	DataTypeFloat     DataType = "float"
	DataTypeInet      DataType = "inet"
	DataTypeInt       DataType = "int"
	DataTypeSmallint  DataType = "smallint"
	DataTypeText      DataType = "text"
	DataTypeTime      DataType = "time"
	DataTypeTimestamp DataType = "timestamp"
	DataTypeTimeuuid  DataType = "timeuuid"
	DataTypeTinyint   DataType = "tinyint"
	DataTypeUuid      DataType = "uuid"
	DataTypeVarchar   DataType = "varchar"
	DataTypeVarint    DataType = "varint"
	DataTypeUnknown   DataType = "unknown"
)

func StringToDataType(s string) DataType {
	switch strings.ToLower(s) {
	case string(DataTypeAscii):
		return DataTypeAscii
	case string(DataTypeBigint):
		return DataTypeBigint
	case string(DataTypeBlob):
		return DataTypeBlob
	case string(DataTypeBoolean):
		return DataTypeBoolean
	case string(DataTypeCounter):
		return DataTypeCounter
	case string(DataTypeDate):
		return DataTypeDate
	case string(DataTypeDecimal):
		return DataTypeDecimal
	case string(DataTypeDouble):
		return DataTypeDouble
	case string(DataTypeDuration):
		return DataTypeDuration
	case string(DataTypeFloat):
		return DataTypeFloat
	case string(DataTypeInet):
		return DataTypeInet
	case string(DataTypeInt):
		return DataTypeInt
	case string(DataTypeSmallint):
		return DataTypeSmallint
	case string(DataTypeText):
		return DataTypeText
	case string(DataTypeTime):
		return DataTypeTime
	case string(DataTypeTimestamp):
		return DataTypeTimestamp
	case string(DataTypeTimeuuid):
		return DataTypeTimeuuid
	case string(DataTypeTinyint):
		return DataTypeTinyint
	case string(DataTypeUuid):
		return DataTypeUuid
	case string(DataTypeVarchar):
		return DataTypeVarchar
	case string(DataTypeVarint):
		return DataTypeVarint
	default:
		return DataTypeUnknown
	}
}

func CastToInternalType(val any, cqlType DataType) (any, error) {
	switch cqlType {
	case DataTypeInt, DataTypeBigint, DataTypeTinyint, DataTypeSmallint:
		return eval.CastToInt64(val)

	case DataTypeDouble, DataTypeFloat:
		return eval.CastToFloat64(val)

	case DataTypeText, DataTypeVarchar:
		typedVal, ok := any(val).(string)
		if !ok {
			return 0, fmt.Errorf("cast %v to string failed", val)
		}
		return typedVal, nil
	default:
		return 0, fmt.Errorf("unknown column type %v", cqlType)
	}
}

func CompareInternalType(left any, right any, cqlType DataType) (int, error) {
	if left == nil {
		return 0, fmt.Errorf("left is nil, not allowed in partition/clustering key comparison, dev error")
	}
	if right == nil {
		return 0, fmt.Errorf("right is nil, not allowed in partition/clustering key comparison, dev error")
	}
	switch cqlType {
	case DataTypeInt, DataTypeBigint, DataTypeTinyint, DataTypeSmallint:
		typedLeft, okLeft := any(left).(int64)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to int64 failed", left)
		}
		typedRight, okRight := any(right).(int64)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to int64 failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil

	case DataTypeDouble, DataTypeFloat:
		typedLeft, okLeft := any(left).(float64)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to float64 failed", left)
		}
		typedRight, okRight := any(right).(float64)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to float64 failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil
	case DataTypeBoolean:
		typedLeft, okLeft := any(left).(bool)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to bool failed", left)
		}
		typedRight, okRight := any(right).(bool)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to bool failed", right)
		}
		if typedLeft == true && typedRight == false {
			return 1, nil
		} else if typedLeft == false && typedRight == true {
			return -1, nil
		} else {
			return 0, nil
		}
	case DataTypeText, DataTypeVarchar:
		typedLeft, okLeft := any(left).(string)
		if !okLeft {
			return 0, fmt.Errorf("left cast %v to string failed", left)
		}
		typedRight, okRight := any(right).(string)
		if !okRight {
			return 0, fmt.Errorf("right cast %v to string failed", right)
		}
		return cmp.Compare(typedLeft, typedRight), nil
	default:
		return 0, fmt.Errorf("unknown column type %v", cqlType)
	}
}
