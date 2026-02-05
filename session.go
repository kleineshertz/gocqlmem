package gocqlmem

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sync"
	"time"

	"gopkg.in/inf.v0"
)

type Session struct {
	KeyspaceMap map[string]*Keyspace
	Lock        sync.RWMutex
}

func NewSession() *Session {
	return &Session{
		KeyspaceMap: map[string]*Keyspace{},
	}
}

func (s *Session) createKeyspace(cmd *CommandCreateKeyspace) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	_, alreadyExists := s.KeyspaceMap[cmd.KeyspaceName]
	if alreadyExists && cmd.IfNotExists {
		return nil
	}
	if alreadyExists && !cmd.IfNotExists {
		return fmt.Errorf("cannot create keyspace %s, it already exists and no IF NOT EXISTS were specified", cmd.KeyspaceName)
	}
	s.KeyspaceMap[cmd.KeyspaceName] = newKeyspace()
	return nil
}

func (s *Session) dropKeyspace(cmd *CommandDropKeyspace) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	ks, alreadyExists := s.KeyspaceMap[cmd.KeyspaceName]
	if !alreadyExists && cmd.IfExists {
		return nil
	}
	if !alreadyExists && !cmd.IfExists {
		return fmt.Errorf("cannot drop keyspace %s, it was not found and no IF EXISTS were specified", cmd.KeyspaceName)
	}

	ks.Lock.Lock()
	delete(s.KeyspaceMap, cmd.KeyspaceName)
	ks.Lock.Unlock()

	return nil
}

func (s *Session) createTable(cmd *CommandCreateTable) error {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.createTable(cmd)
}

func (s *Session) dropTable(cmd *CommandDropTable) error {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.dropTable(cmd)
}

func (s *Session) execInsert(cmd *CommandInsert) (bool, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return false, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execInsert(cmd)
}

func (s *Session) execSelect(cmd *CommandSelect) ([]string, [][]any, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return []string{}, [][]any{}, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execSelect(cmd)
}

func (s *Session) execUpdate(cmd *CommandUpdate) (bool, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return false, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execUpdate(cmd)
}

func (s *Session) execDelete(cmd *CommandDelete) (bool, error) {
	s.Lock.RLock()

	ks, ksExists := s.KeyspaceMap[cmd.GetCtxKeyspace()]
	s.Lock.RUnlock()
	if !ksExists {
		return false, fmt.Errorf("keyspace %s does not exist", cmd.GetCtxKeyspace())
	}

	return ks.execDelete(cmd)
}

/**/
type RowData struct {
	Columns []string
	Values  []interface{}
}

// func dereference(i interface{}) interface{} {
// 	return reflect.Indirect(reflect.ValueOf(i)).Interface()
// }

// func (r *RowData) rowMap(m map[string]interface{}) {
// 	for i, column := range r.Columns {
// 		val := dereference(r.Values[i])
// 		if valVal := reflect.ValueOf(val); valVal.Kind() == reflect.Slice {
// 			valCopy := reflect.MakeSlice(valVal.Type(), valVal.Len(), valVal.Cap())
// 			reflect.Copy(valCopy, valVal)
// 			m[column] = valCopy.Interface()
// 		} else {
// 			m[column] = val
// 		}
// 	}
// }

type nextIter struct {
	qry   *Query
	pos   int
	oncea sync.Once
	once  sync.Once
	next  *Iter
}

// func (n *nextIter) fetch() *Iter {
// 	n.once.Do(func() {
// 		// if the query was specifically run on a connection then re-use that
// 		// connection when fetching the next results
// 		if n.qry.conn != nil {
// 			n.next = n.qry.conn.executeQuery(n.qry.Context(), n.qry)
// 		} else {
// 			n.next = n.qry.session.executeQuery(n.qry)
// 		}
// 	})
// 	return n.next
// }

// func (n *nextIter) fetchAsync() {
// 	n.oncea.Do(func() {
// 		go n.fetch()
// 	})
// }

type Type int

const (
	TypeCustom    Type = 0x0000
	TypeAscii     Type = 0x0001
	TypeBigInt    Type = 0x0002
	TypeBlob      Type = 0x0003
	TypeBoolean   Type = 0x0004
	TypeCounter   Type = 0x0005
	TypeDecimal   Type = 0x0006
	TypeDouble    Type = 0x0007
	TypeFloat     Type = 0x0008
	TypeInt       Type = 0x0009
	TypeText      Type = 0x000A
	TypeTimestamp Type = 0x000B
	TypeUUID      Type = 0x000C
	TypeVarchar   Type = 0x000D
	TypeVarint    Type = 0x000E
	TypeTimeUUID  Type = 0x000F
	TypeInet      Type = 0x0010
	TypeDate      Type = 0x0011
	TypeTime      Type = 0x0012
	TypeSmallInt  Type = 0x0013
	TypeTinyInt   Type = 0x0014
	TypeDuration  Type = 0x0015
	TypeList      Type = 0x0020
	TypeMap       Type = 0x0021
	TypeSet       Type = 0x0022
	TypeUDT       Type = 0x0030
	TypeTuple     Type = 0x0031
)

// TypeInfo describes a Cassandra specific data type.
type TypeInfo interface {
	Type() Type
	Version() byte
	Custom() string

	// New creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver.
	//
	// If there is no corresponding Go type for the CQL type, New panics.
	//
	// Deprecated: Use NewWithError instead.
	New() interface{}

	// NewWithError creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver.
	//
	// If there is no corresponding Go type for the CQL type, NewWithError returns an error.
	NewWithError() (interface{}, error)
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo TypeInfo
}

type resultMetadata struct {
	flags int

	// only if flagPageState
	pagingState []byte

	columns  []ColumnInfo
	colCount int

	// this is a count of the total number of columns which can be scanned,
	// it is at minimum len(columns) but may be larger, for instance when a column
	// is a UDT or tuple.
	actualColCount int
}

type Iter struct {
	err     error
	pos     int
	meta    resultMetadata
	numRows int
	next    *nextIter
	//host    *HostInfo

	//framer *framer
	closed int32

	//RetrievedTypes  []CqlDataTypeType
	RetrievedNames  []string
	RetrievedValues [][]any
}

type UUID [16]byte

// String returns the name of the identifier.
func (t Type) String() string {
	switch t {
	case TypeCustom:
		return "custom"
	case TypeAscii:
		return "ascii"
	case TypeBigInt:
		return "bigint"
	case TypeBlob:
		return "blob"
	case TypeBoolean:
		return "boolean"
	case TypeCounter:
		return "counter"
	case TypeDecimal:
		return "decimal"
	case TypeDouble:
		return "double"
	case TypeFloat:
		return "float"
	case TypeInt:
		return "int"
	case TypeText:
		return "text"
	case TypeTimestamp:
		return "timestamp"
	case TypeUUID:
		return "uuid"
	case TypeVarchar:
		return "varchar"
	case TypeTimeUUID:
		return "timeuuid"
	case TypeInet:
		return "inet"
	case TypeDate:
		return "date"
	case TypeDuration:
		return "duration"
	case TypeTime:
		return "time"
	case TypeSmallInt:
		return "smallint"
	case TypeTinyInt:
		return "tinyint"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	case TypeSet:
		return "set"
	case TypeVarint:
		return "varint"
	case TypeTuple:
		return "tuple"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
	}
}

type NativeType struct {
	proto  byte
	typ    Type
	custom string // only used for TypeCustom
}

func NewNativeType(proto byte, typ Type, custom string) NativeType {
	return NativeType{proto, typ, custom}
}

func (t NativeType) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t NativeType) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (s NativeType) Type() Type {
	return s.typ
}

func (s NativeType) Version() byte {
	return s.proto
}

func (s NativeType) Custom() string {
	return s.custom
}

func (s NativeType) String() string {
	switch s.typ {
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", s.typ, s.custom)
	default:
		return s.typ.String()
	}
}

type CollectionType struct {
	NativeType
	Key  TypeInfo // only used for TypeMap
	Elem TypeInfo // only used for TypeMap, TypeList and TypeSet
}

func (t CollectionType) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t CollectionType) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (c CollectionType) String() string {
	switch c.typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", c.typ, c.Key, c.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", c.typ, c.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", c.typ, c.custom)
	default:
		return c.typ.String()
	}
}

type Duration struct {
	Months      int32
	Days        int32
	Nanoseconds int64
}

func goType(t TypeInfo) (reflect.Type, error) {
	switch t.Type() {
	case TypeVarchar, TypeAscii, TypeInet, TypeText:
		return reflect.TypeOf(*new(string)), nil
	case TypeBigInt, TypeCounter:
		return reflect.TypeOf(*new(int64)), nil
	case TypeTime:
		return reflect.TypeOf(*new(time.Duration)), nil
	case TypeTimestamp:
		return reflect.TypeOf(*new(time.Time)), nil
	case TypeBlob:
		return reflect.TypeOf(*new([]byte)), nil
	case TypeBoolean:
		return reflect.TypeOf(*new(bool)), nil
	case TypeFloat:
		return reflect.TypeOf(*new(float32)), nil
	case TypeDouble:
		return reflect.TypeOf(*new(float64)), nil
	case TypeInt:
		return reflect.TypeOf(*new(int)), nil
	case TypeSmallInt:
		return reflect.TypeOf(*new(int16)), nil
	case TypeTinyInt:
		return reflect.TypeOf(*new(int8)), nil
	case TypeDecimal:
		return reflect.TypeOf(*new(*inf.Dec)), nil
	case TypeUUID, TypeTimeUUID:
		return reflect.TypeOf(*new(UUID)), nil
	case TypeList, TypeSet:
		elemType, err := goType(t.(CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemType), nil
	case TypeMap:
		keyType, err := goType(t.(CollectionType).Key)
		if err != nil {
			return nil, err
		}
		valueType, err := goType(t.(CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(keyType, valueType), nil
	case TypeVarint:
		return reflect.TypeOf(*new(*big.Int)), nil
	case TypeTuple:
		// what can we do here? all there is to do is to make a list of interface{}
		tuple := t.(TupleTypeInfo)
		return reflect.TypeOf(make([]interface{}, len(tuple.Elems))), nil
	case TypeUDT:
		return reflect.TypeOf(make(map[string]interface{})), nil
	case TypeDate:
		return reflect.TypeOf(*new(time.Time)), nil
	case TypeDuration:
		return reflect.TypeOf(*new(Duration)), nil
	default:
		return nil, fmt.Errorf("cannot create Go type for unknown CQL type %s", t)
	}
}

type TupleTypeInfo struct {
	NativeType
	Elems []TypeInfo
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.typ))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

func (t TupleTypeInfo) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t TupleTypeInfo) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (iter *Iter) Columns() []ColumnInfo {
	return iter.meta.columns
}

// TupeColumnName will return the column name of a tuple value in a column named
// c at index n. It should be used if a specific element within a tuple is needed
// to be extracted from a map returned from SliceMap or MapScan.
func TupleColumnName(c string, n int) string {
	return fmt.Sprintf("%s[%d]", c, n)
}

// func (iter *Iter) readColumn() ([]byte, error) {
// 	return iter.framer.readBytesInternal()
// }

func (iter *Iter) RowData() (RowData, error) {
	if iter.err != nil {
		return RowData{}, iter.err
	}

	rowData := RowData{
		Columns: iter.RetrievedNames,
		Values:  make([]interface{}, len(iter.RetrievedNames)),
	}

	return rowData, nil
}

// func scanColumn(p []byte, col ColumnInfo, dest []interface{}) (int, error) {
// 	if dest[0] == nil {
// 		return 1, nil
// 	}

// 	if col.TypeInfo.Type() == TypeTuple {
// 		// this will panic, actually a bug, please report
// 		tuple := col.TypeInfo.(TupleTypeInfo)

// 		count := len(tuple.Elems)
// 		// here we pass in a slice of the struct which has the number number of
// 		// values as elements in the tuple
// 		if err := Unmarshal(col.TypeInfo, p, dest[:count]); err != nil {
// 			return 0, err
// 		}
// 		return count, nil
// 	} else {
// 		if err := Unmarshal(col.TypeInfo, p, dest[0]); err != nil {
// 			return 0, err
// 		}
// 		return 1, nil
// 	}
// }

func (iter *Iter) SliceMap() ([]map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}

	// Not checking for the error because we just did
	rowData, _ := iter.RowData()
	dataToReturn := make([]map[string]interface{}, 0)
	for iter.Scan(rowData.Values...) {
		m := make(map[string]interface{}, len(rowData.Columns))
		for i, column := range rowData.Columns {
			m[column] = rowData.Values[i]
		}
		dataToReturn = append(dataToReturn, m)
	}
	if iter.err != nil {
		return nil, iter.err
	}
	return dataToReturn, nil
}

func (iter *Iter) Close() error {
	return iter.err
}

type Query struct {
	stmt                string
	values              []interface{}
	session             *Session
	pageSize            int
	pageState           []byte
	disableAutoPage     bool
	disableSkipMetadata bool
}

func (q *Query) PageSize(n int) *Query {
	q.pageSize = n
	return q
}

func (q *Query) PageState(state []byte) *Query {
	q.pageState = state
	q.disableAutoPage = true
	return q
}

type Scanner interface {
	// Next advances the row pointer to point at the next row, the row is valid until
	// the next call of Next. It returns true if there is a row which is available to be
	// scanned into with Scan.
	// Next must be called before every call to Scan.
	Next() bool

	// Scan copies the current row's columns into dest. If the length of dest does not equal
	// the number of columns returned in the row an error is returned. If an error is encountered
	// when unmarshalling a column into the value in dest an error is returned and the row is invalidated
	// until the next call to Next.
	// Next must be called before calling Scan, if it is not an error is returned.
	Scan(...interface{}) error

	// Err returns the if there was one during iteration that resulted in iteration being unable to complete.
	// Err will also release resources held by the iterator, the Scanner should not used after being called.
	Err() error
}
type iterScanner struct {
	iter  *Iter
	cols  []interface{}
	valid bool
}

func (is *iterScanner) Next() bool {
	if is.iter.pos >= len(is.iter.RetrievedValues) {
		return false
	}
	for i := range len(is.iter.RetrievedNames) {
		is.cols[i] = is.iter.RetrievedValues[is.iter.pos][i]
	}
	is.iter.pos++
	return true
}

func (is *iterScanner) Scan(dest ...interface{}) error {
	for i := range len(is.cols) {
		dest[i] = is.cols[i]
	}
	return nil
}
func (is *iterScanner) Err() error {
	iter := is.iter
	is.iter = nil
	is.cols = nil
	is.valid = false
	return iter.Close()
}

func (iter *Iter) Scanner() Scanner {
	if iter == nil {
		return nil
	}

	return &iterScanner{iter: iter, cols: make([]interface{}, len(iter.RetrievedNames))}
}

func (q *Query) Iter() *Iter {
	cmds, err := ParseCommands(q.stmt)
	if err != nil {
		return &Iter{err: err}
	}
	if len(cmds) != 1 {
		return &Iter{err: fmt.Errorf("exactly one CQL cmd expected, got: %s", q.stmt)}
	}

	switch cmd := cmds[0].(type) {
	case *CommandCreateKeyspace:
		return &Iter{err: q.session.createKeyspace(cmd)}
	case *CommandUseKeyspace:
		return &Iter{}
	case *CommandDropKeyspace:
		return &Iter{err: q.session.dropKeyspace(cmd)}
	case *CommandCreateTable:
		return &Iter{err: q.session.createTable(cmd)}
	case *CommandDropTable:
		return &Iter{err: q.session.dropTable(cmd)}
	case *CommandInsert:
		isApplied, err := q.session.execInsert(cmd)
		if err != nil {
			return &Iter{err: err}
		}
		return &Iter{
			//RetrievedTypes:  []CqlDataTypeType{CqlDataTypeBool},
			RetrievedNames:  []string{"[applied]"},
			RetrievedValues: [][]any{{isApplied}}}
	case *CommandSelect:
		names, values, err := q.session.execSelect(cmd)
		if err != nil {
			return &Iter{err: err}
		}
		return &Iter{
			//RetrievedTypes:  types,
			RetrievedNames:  names,
			RetrievedValues: values}
	case *CommandUpdate:
		isApplied, err := q.session.execUpdate(cmd)
		if err != nil {
			return &Iter{err: err}
		}
		return &Iter{
			//RetrievedTypes:  []CqlDataTypeType{CqlDataTypeBool},
			RetrievedNames:  []string{"[applied]"},
			RetrievedValues: [][]any{{isApplied}}}

	case *CommandDelete:
		isApplied, err := q.session.execDelete(cmd)
		if err != nil {
			return &Iter{err: err}
		}
		return &Iter{
			//RetrievedTypes:  []CqlDataTypeType{CqlDataTypeBool},
			RetrievedNames:  []string{"[applied]"},
			RetrievedValues: [][]any{{isApplied}}}

	default:
		return &Iter{err: fmt.Errorf("Iter() does not support cmd %v", cmd)}
	}
}

func (iter *Iter) checkErrAndNotFound() error {
	if iter.err != nil {
		return iter.err
		//} else if iter.numRows == 0 {
	} else if len(iter.RetrievedValues) == 0 {
		return errors.New("not found")
	}
	return nil
}

func (iter *Iter) Scan(dest ...interface{}) bool {
	if iter.err != nil || iter.pos >= len(iter.RetrievedValues) {
		return false
	}

	if len(dest) != len(iter.RetrievedNames) {
		iter.err = fmt.Errorf("gocqlmem: not enough columns to scan into: have %d want %d", len(dest), len(iter.RetrievedNames))
		return false
	}

	for i := range len(iter.RetrievedNames) {
		dest[i] = iter.RetrievedValues[iter.pos][i]
	}

	iter.pos++
	return true

	// if iter.err != nil {
	// 	return false
	// }

	// if iter.pos >= iter.numRows {
	// 	if iter.next != nil {
	// 		*iter = *iter.next.fetch()
	// 		return iter.Scan(dest...)
	// 	}
	// 	return false
	// }

	// if iter.next != nil && iter.pos >= iter.next.pos {
	// 	iter.next.fetchAsync()
	// }

	// // currently only support scanning into an expand tuple, such that its the same
	// // as scanning in more values from a single column
	// if len(dest) != iter.meta.actualColCount {
	// 	iter.err = fmt.Errorf("gocql: not enough columns to scan into: have %d want %d", len(dest), iter.meta.actualColCount)
	// 	return false
	// }

	// // i is the current position in dest, could posible replace it and just use
	// // slices of dest
	// i := 0
	// for _, col := range iter.meta.columns {
	// 	colBytes, err := iter.readColumn()
	// 	if err != nil {
	// 		iter.err = err
	// 		return false
	// 	}

	// 	n, err := scanColumn(colBytes, col, dest[i:])
	// 	if err != nil {
	// 		iter.err = err
	// 		return false
	// 	}
	// 	i += n
	// }

	// iter.pos++
	// return true
}

func (iter *Iter) MapScan(m map[string]interface{}) bool {
	if iter.err != nil {
		return false
	}

	rowDataValues := make([]any, len(iter.RetrievedNames))
	if iter.Scan(rowDataValues...) {
		for i, name := range iter.RetrievedNames {
			m[name] = rowDataValues[i]
		}
		return true
	}
	return false

	// // Not checking for the error because we just did
	// rowData, _ := iter.RowData()

	// for i, col := range rowData.Columns {
	// 	if dest, ok := m[col]; ok {
	// 		rowData.Values[i] = dest
	// 	}
	// }

	// if iter.Scan(rowData.Values...) {
	// 	rowData.rowMap(m)
	// 	return true
	// }
	// return false
}

// INSERT, UPDATE, DELETE
func (q *Query) MapScanCAS(dest map[string]interface{}) (applied bool, err error) {
	iter := q.Iter()
	if err := iter.checkErrAndNotFound(); err != nil {
		return false, err
	}
	iter.MapScan(dest)
	applied = dest["[applied]"].(bool)
	delete(dest, "[applied]")

	return applied, iter.Close()
}

// CREATE KEYSPACE
func (q *Query) Exec() error {
	return q.Iter().Close()
}

func (s *Session) Query(stmt string, values ...interface{}) *Query {
	qry := Query{}
	qry.session = s
	qry.stmt = stmt
	qry.values = values
	return &qry
}

/**/
