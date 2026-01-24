package eval_gocqlmem

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
	switch s {
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
