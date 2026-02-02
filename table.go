package gocqlmem

import (
	"cmp"
	"fmt"
	"go/ast"
	"go/token"
	"math"
	"slices"
	"strconv"
	"sync"

	"github.com/capillariesio/gocqlmem/eval"
	"github.com/capillariesio/gocqlmem/eval_gocqlmem"
)

type PrimaryKeyType int

const (
	PrimaryKeyPartition PrimaryKeyType = iota
	PrimaryKeyClustering
	PrimaryKeyNone
)

type ColumnDef struct {
	Name            string
	PrimaryKey      PrimaryKeyType
	DataType        eval_gocqlmem.DataType
	ClusteringOrder ClusteringOrderType
}

type Table struct {
	ColumnDefs   []*ColumnDef // Partition,clustering,other
	ColumnValues [][]any      // Partition,clustering,other
	ColumnTokens [][]int64    // Partition keys only
	ColumnDefMap map[string]int
	Lock         sync.RWMutex
}

func createColDef(name string, mapColType map[string]eval_gocqlmem.DataType, primaryKeyType PrimaryKeyType, mapColClusteringOrder map[string]ClusteringOrderType) (*ColumnDef, error) {
	dataType, ok := mapColType[name]
	if !ok {
		return nil, fmt.Errorf("cannot find definition for column %s", name)
	}
	clusteringOrder, ok := mapColClusteringOrder[name]
	if !ok {
		clusteringOrder = ClusteringOrderNone
	}
	// For partition keys force ASC, we need it for our internal purposes when we walk through values
	if primaryKeyType == PrimaryKeyPartition {
		clusteringOrder = ClusteringOrderAsc
	}
	if !ok {
		return nil, fmt.Errorf("cannot find definition for column %s", name)
	}
	return &ColumnDef{
		Name:            name,
		PrimaryKey:      primaryKeyType,
		DataType:        dataType,
		ClusteringOrder: clusteringOrder,
	}, nil
}

func newTable(cmd *CommandCreateTable) (*Table, error) {
	t := Table{
		ColumnDefs:   make([]*ColumnDef, len(cmd.ColumnDefs)),
		ColumnValues: make([][]any, len(cmd.ColumnDefs)),
		ColumnTokens: make([][]int64, len(cmd.PartitionKeyColumns)),
		ColumnDefMap: map[string]int{},
	}

	mapColType := map[string]eval_gocqlmem.DataType{}
	var err error
	for _, createTableColDef := range cmd.ColumnDefs {
		mapColType[createTableColDef.Name] = createTableColDef.Type
	}

	mapColClusteringOrder := map[string]ClusteringOrderType{}
	for _, orderByField := range cmd.ClusteringOrderBy {
		// Check definition is present
		if _, ok := mapColType[orderByField.FieldName]; !ok {
			return nil, fmt.Errorf("cannot find definition for clustering order column %s", orderByField.FieldName)
		}
		// Check it's a clustering column
		var isClustering bool
		for _, name := range cmd.ClusteringKeyColumns {
			if orderByField.FieldName == name {
				isClustering = true
				break
			}
		}
		if !isClustering {
			return nil, fmt.Errorf("clustering order column %s is not in the list of clustering keys %v", orderByField.FieldName, cmd.ClusteringKeyColumns)
		}
		// Save ASC/DESC to temp map
		mapColClusteringOrder[orderByField.FieldName] = orderByField.ClusteringOrder
	}

	colDefIdx := 0
	t.ColumnDefMap = map[string]int{}
	// Partition columns first
	for _, name := range cmd.PartitionKeyColumns {
		if t.ColumnDefs[colDefIdx], err = createColDef(name, mapColType, PrimaryKeyPartition, mapColClusteringOrder); err != nil {
			return nil, err
		}
		t.ColumnDefMap[name] = colDefIdx
		colDefIdx++
	}
	// Clustering columns next
	for _, name := range cmd.ClusteringKeyColumns {
		if t.ColumnDefs[colDefIdx], err = createColDef(name, mapColType, PrimaryKeyClustering, mapColClusteringOrder); err != nil {
			return nil, err
		}
		t.ColumnDefMap[name] = colDefIdx
		colDefIdx++
	}
	// All other columns next, in the order of appearance in the CREATE TABLE cmd
	for _, createTableColDef := range cmd.ColumnDefs {
		if _, ok := t.ColumnDefMap[createTableColDef.Name]; !ok {
			if t.ColumnDefs[colDefIdx], err = createColDef(createTableColDef.Name, mapColType, PrimaryKeyNone, mapColClusteringOrder); err != nil {
				return nil, err
			}
			t.ColumnDefMap[createTableColDef.Name] = colDefIdx
			colDefIdx++
		}
	}

	return &t, nil
}

func (t *Table) getClusteringKeyOrderByName(name string) ClusteringOrderType {
	for _, colDef := range t.ColumnDefs {
		if name == colDef.Name {
			return colDef.ClusteringOrder
		}
	}
	return ClusteringOrderNone
}

type ClusteringKeyEntry struct {
	Idx int
	Key string
}

func (t *Table) getRowSequenceFromColumnDefAndSelectOrderBy(orderByFieldsFromSelect []*OrderByField) ([]int, error) {
	totalRows := len(t.ColumnValues[0])
	if len(orderByFieldsFromSelect) == 0 {
		result := make([]int, totalRows)
		for i := range totalRows {
			result[i] = i
		}
		return result, nil
	}

	tempClusteringKey := make([]ClusteringKeyEntry, totalRows)
	for _, orderByFieldFromSelect := range orderByFieldsFromSelect {
		// Each field from SELECT ORDER by must be a clustering key
		tableClusteringOrder := t.getClusteringKeyOrderByName(orderByFieldFromSelect.FieldName)
		if tableClusteringOrder == ClusteringOrderNone {
			return nil, fmt.Errorf("cannot process ORDER BY %s, this field is not a clustering key", orderByFieldFromSelect.FieldName)
		}
		colIdx := t.ColumnDefMap[orderByFieldFromSelect.FieldName]
		var lastVal any
		var lastTempClusteringKeySegment int
		for i := range totalRows {
			if lastVal == nil {
				lastVal = t.ColumnValues[colIdx][i]
				if tableClusteringOrder == orderByFieldFromSelect.ClusteringOrder {
					lastTempClusteringKeySegment = 0
				} else {
					lastTempClusteringKeySegment = math.MaxInt32
				}
			} else {
				if t.ColumnValues[colIdx][i] != lastVal {
					if tableClusteringOrder == orderByFieldFromSelect.ClusteringOrder {
						lastTempClusteringKeySegment++
					} else {
						lastTempClusteringKeySegment--
					}
				}
			}
			if colIdx == 0 {
				tempClusteringKey[i] = ClusteringKeyEntry{Idx: i, Key: fmt.Sprintf("0x%08X", lastTempClusteringKeySegment)}
			} else {
				tempClusteringKey[i] = ClusteringKeyEntry{Idx: i, Key: fmt.Sprintf("%s0x%08X", tempClusteringKey[i].Key, lastTempClusteringKeySegment)}
			}
		}
	}

	slices.SortFunc(tempClusteringKey, func(e1, e2 ClusteringKeyEntry) int {
		return cmp.Compare(e1.Key, e2.Key)
	})

	result := make([]int, len(tempClusteringKey))
	for i := range len(tempClusteringKey) {
		result[i] = tempClusteringKey[i].Idx
	}
	return result, nil
}

func convertLexemToInternalType(lexem *Lexem, cqlType eval_gocqlmem.DataType) (any, error) {
	if lexem.T == LexemNull {
		return nil, nil
	}
	switch cqlType {
	case eval_gocqlmem.DataTypeBigint, eval_gocqlmem.DataTypeTinyint, eval_gocqlmem.DataTypeSmallint:
		if lexem.T != LexemNumberLiteral {
			return 0, fmt.Errorf("cannot convert %v to integer, lexem type %d not supported", lexem.V, lexem.T)
		}
		val, err := strconv.Atoi(lexem.V)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %v to integer: %s", lexem.V, err.Error())
		}
		return int64(val), nil

	case eval_gocqlmem.DataTypeText, eval_gocqlmem.DataTypeVarchar:
		if lexem.T != LexemStringLiteral {
			return 0, fmt.Errorf("cannot convert %v to string, lexem type %d not supported", lexem.V, lexem.T)
		}
		return lexem.V, nil

	case eval_gocqlmem.DataTypeBoolean:
		if lexem.T != LexemBoolLiteral {
			return 0, fmt.Errorf("cannot convert %v to bool, lexem type %d not supported", lexem.V, lexem.T)
		}
		return lexem.V == "TRUE", nil

	default:
		return 0, fmt.Errorf("unknown column type %v", cqlType)
	}
}

func getRowIndexFromColumnDefAndInsert(columnValues [][]any, columnDefs []*ColumnDef, insertedColumnValues map[string]any) (int, bool, error) {
	topIdx := 0                       // Top candidate for replacement
	bottomIdx := len(columnValues[0]) // One step below the last candidate
	var isExists bool
	for tableColIdx, tableColDef := range columnDefs {
		insertedColVal := insertedColumnValues[tableColDef.Name]
		newStartIdx := -1
		newEndIdx := -1
		curIdx := topIdx
		for curIdx < bottomIdx {
			curVal := columnValues[tableColIdx][curIdx]
			compareResult, err := eval_gocqlmem.CompareInternalType(curVal, insertedColVal, tableColDef.DataType)
			if err != nil {
				return 0, false, fmt.Errorf("cannot compare existing %v to inserted %v", curVal, insertedColVal)
			}
			if tableColDef.ClusteringOrder == ClusteringOrderDesc {
				compareResult *= -1
			}
			if compareResult == 0 {
				if newStartIdx == -1 {
					newStartIdx = curIdx
				}
				newEndIdx = curIdx + 1
			} else if compareResult == 1 {
				// curVal > insertedColVal (ASC) or curVal < insertedColVal (DESC), time to end looking
				if newStartIdx == -1 {
					return curIdx, false, nil
				}
				newEndIdx = curIdx
				break
			}
			curIdx++
		}
		if newStartIdx == -1 {
			// No equal or greater (ASC) or smaller (DESC) values found in this column,
			// ready to insert in the previously harvested range startIdx,endIdx
			if tableColDef.ClusteringOrder == ClusteringOrderAsc {
				return bottomIdx, false, nil
			}
			return bottomIdx, false, nil
		}
		// Now we have a range of size at least to, say:
		// newStartIdx = 5 (with value eq to the inserted one)
		// newEndIdx = 6 (with value > to the inserted one, or beyond the range)

		// Proceed to the next key column with updated range
		topIdx = newStartIdx
		bottomIdx = newEndIdx

		// If there are no more key columns, insert here
		if tableColIdx == len(columnDefs)-1 || columnDefs[tableColIdx+1].PrimaryKey == PrimaryKeyNone {
			isExists = true
			break
		}
	}
	return bottomIdx, isExists, nil
}

func (t *Table) execInternalUpsert(cmd *CommandInsert, insertedColumnValues map[string]any) (bool, error) {
	for _, tableColDef := range t.ColumnDefs {
		if tableColDef.PrimaryKey == PrimaryKeyPartition || tableColDef.PrimaryKey == PrimaryKeyClustering {
			var isPresent bool
			for _, colName := range cmd.ColumnNames {
				if colName == tableColDef.Name {
					isPresent = true
					break
				}
			}
			if !isPresent {
				return false, fmt.Errorf("partition/clustering column key %s must be specified in the upsert command", tableColDef.Name)
			}
		}
	}

	var err error
	var insertIdx int
	var isAlreadyExists bool

	if len(t.ColumnValues[0]) > 0 {
		insertIdx, isAlreadyExists, err = getRowIndexFromColumnDefAndInsert(t.ColumnValues, t.ColumnDefs, insertedColumnValues)
		if err != nil {
			return false, fmt.Errorf("cannot find upsert idx for %v: %s", insertedColumnValues, err.Error())
		}

		if isAlreadyExists && cmd.IfNotExists {
			return false, nil
		}

		if isAlreadyExists && !cmd.IfNotExists {
			return false, fmt.Errorf("cannot upsert duplicate %v", insertedColumnValues)
		}

		for tableColIdx, tableColDef := range t.ColumnDefs {
			val, ok := insertedColumnValues[tableColDef.Name]
			if !ok {
				val = nil
			}
			t.ColumnValues[tableColIdx] = slices.Insert(t.ColumnValues[tableColIdx], insertIdx, val)
		}
	} else {
		for tableColIdx, tableColDef := range t.ColumnDefs {
			val, ok := insertedColumnValues[tableColDef.Name]
			if !ok {
				val = nil
			}
			t.ColumnValues[tableColIdx] = append(t.ColumnValues[tableColIdx], val)
		}
	}
	return true, nil
}

func (t *Table) execInsert(cmd *CommandInsert) (bool, error) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	var err error
	insertedColumnValues := map[string]any{}
	for i, name := range cmd.ColumnNames {
		insertedColumnValues[name], err = convertLexemToInternalType(cmd.ColumnValues[i], t.ColumnDefs[t.ColumnDefMap[name]].DataType)
		if err != nil {
			return false, fmt.Errorf("cannot cast upserted column %d: %s", i, err.Error())
		}
		if insertedColumnValues[name] == nil && (t.ColumnDefs[t.ColumnDefMap[name]].PrimaryKey == PrimaryKeyPartition || t.ColumnDefs[t.ColumnDefMap[name]].PrimaryKey == PrimaryKeyClustering) {
			return false, fmt.Errorf("cannot upsert NULL into a partition/clustered key column %s", name)
		}
	}

	return t.execInternalUpsert(cmd, insertedColumnValues)
}

func (t *Table) execSelect(cmd *CommandSelect) ([]string, [][]any, error) {
	t.Lock.RLock()
	defer t.Lock.RUnlock()

	selectSeq, err := t.getRowSequenceFromColumnDefAndSelectOrderBy(cmd.OrderByFields)
	if err != nil {
		return nil, nil, err
	}

	resultNames := []string{}
	for _, selectLexems := range cmd.SelectExpLexems {
		s, as, err := lexemsToString(selectLexems)
		if err != nil {
			return nil, nil, err
		}
		if as == "" {
			as = s
		}
		resultNames = append(resultNames, as)
	}

	resultRows := [][]any{}
	valMap := eval.VarValuesMap{}
	valMap[""] = map[string]any{}
	valMap[cmd.TableName] = map[string]any{}
	for _, i := range selectSeq {
		resultRow := []any{}
		for colIdx, colDef := range t.ColumnDefs {
			valMap[""][colDef.Name] = t.ColumnValues[colIdx][i]
			valMap[cmd.TableName][colDef.Name] = t.ColumnValues[colIdx][i]
		}

		eCtx := eval.NewPlainEvalCtx(eval_gocqlmem.GocqlmemEvalFunctions, eval_gocqlmem.GocqlmemEvalConstants, valMap)
		isIncludeAny, err := eCtx.Eval(cmd.WhereExpAst)
		if err != nil {
			return nil, nil, err
		}

		isInclude, ok := isIncludeAny.(bool)
		if !ok {
			return nil, nil, fmt.Errorf("where expressions return %T, expected bool", isIncludeAny)
		}

		if isInclude {
			for _, selectExpAst := range cmd.SelectExpAsts {
				eCtx := eval.NewPlainEvalCtx(eval_gocqlmem.GocqlmemEvalFunctions, eval_gocqlmem.GocqlmemEvalConstants, valMap)
				val, err := eCtx.Eval(selectExpAst)
				if err != nil {
					return nil, nil, err
				}
				resultRow = append(resultRow, val)
			}
			resultRows = append(resultRows, resultRow)
		}
	}

	return resultNames, resultRows, nil
}

func getInsertedPriKeyColumnNameFromEql(tableName string, columnDefMap map[string]int, exp ast.Expr) (string, error) {
	switch typedExp := exp.(type) {
	case *ast.Ident:
		if _, ok := columnDefMap[typedExp.Name]; ok {
			return typedExp.Name, nil
		}
		// It's an ident, but not a column name, maybe it's a still valid right-side ident like col1 == TRUE
		return "", nil

	case *ast.SelectorExpr:
		switch tableIdent := typedExp.X.(type) {
		case *ast.Ident:
			if tableIdent.Name != tableName {
				// It's a selector ident, but not a column in this table, maybe it's a still valid right-side ident like col1 == some_namespace.some_selector
				return "", nil
			}
			return typedExp.Sel.Name, nil
		default:
			return "", fmt.Errorf("expected %s.col_name == ..., got invalid table name of type %T", tableName, tableIdent)
		}
	default:
		// It's a generic expression, maybe it's a still valid right-side ident like col1 == some_func(a*b)
		return "", nil
	}
}

func getInsertedPriKeyColumnValuePairFromEql(tableName string, columnDefs []*ColumnDef, columnDefMap map[string]int, eqlExp ast.BinaryExpr) (string, any, error) {
	var colName string
	var err error
	var colValExp ast.Expr

	// Try left side for column name
	colName, err = getInsertedPriKeyColumnNameFromEql(tableName, columnDefMap, eqlExp.X)
	if err != nil {
		return "", nil, err
	}
	if colName != "" {
		colValExp = eqlExp.Y
	} else {
		// Try right side for column name
		colName, err = getInsertedPriKeyColumnNameFromEql(tableName, columnDefMap, eqlExp.Y)
		if err != nil {
			return "", nil, err
		}
		if colName != "" {
			colValExp = eqlExp.X
		} else {
			return "", nil, fmt.Errorf("cannot find column ident in the expected col1 == ... , got %T == %T", eqlExp.X, eqlExp.Y)
		}
	}

	// Column value exp can be something like round(2.3), it does not have to be a literal
	eCtx := eval.NewPlainEvalCtx(eval_gocqlmem.GocqlmemEvalFunctions, eval_gocqlmem.GocqlmemEvalConstants, nil)
	colValAny, err := eCtx.Eval(colValExp)
	if err != nil {
		return "", nil, fmt.Errorf("cannot evaluate column %s value: %s", colName, err.Error())
	}

	if colValAny == nil {
		return colName, nil, nil
	}

	internalColVal, err := eval_gocqlmem.CastToInternalType(colValAny, columnDefs[columnDefMap[colName]].DataType)
	if err != nil {
		return "", nil, fmt.Errorf("cannot cast column %s value (%v): %s", colName, colValAny, err.Error())
	}

	return colName, internalColVal, nil
}

func harvestInsertedPriKeyValuesFromAstExp(tableName string, columnDefs []*ColumnDef, columnDefMap map[string]int, exp ast.Expr, colValueMap map[string]any) error {
	switch typedExp := exp.(type) {
	case *ast.BinaryExpr:
		switch typedExp.Op {
		case token.LAND:
			if err := harvestInsertedPriKeyValuesFromAstExp(tableName, columnDefs, columnDefMap, typedExp.X, colValueMap); err != nil {
				return fmt.Errorf("error harvesting left side of AND: %s", err.Error())
			}
			if err := harvestInsertedPriKeyValuesFromAstExp(tableName, columnDefs, columnDefMap, typedExp.Y, colValueMap); err != nil {
				return fmt.Errorf("error harvesting right side of AND: %s", err.Error())
			}
		case token.EQL:
			colName, colVal, err := getInsertedPriKeyColumnValuePairFromEql(tableName, columnDefs, columnDefMap, *typedExp)
			if err != nil {
				return fmt.Errorf("cannot get column name value pair: %s", err.Error())
			}
			colValueMap[colName] = colVal
		default:
			return fmt.Errorf("cannot harvest, expected top-level AND or ==, got op %d", typedExp.Op)
		}
	default:
		return fmt.Errorf("cannot harvest, expected top-level AND or ==, got exp %T", typedExp)
	}
	return nil
}

// Convert "WHERE col1 == 'a' and col2 == 100` into col1:'a',col2:100
func getInsertedPriKeyValuesFromWhereClause(tableName string, columnDefs []*ColumnDef, columnDefMap map[string]int, whereExpAst ast.Expr) (map[string]any, error) {
	// 1. Detect all colX = exp fragments
	// 2. Ensure they are linked with AND
	// 3. Ensure the combined col1==... AND COL2==... is at the top of ast
	// 4. For each colX==... evaluate the exp and add it to the result map
	colValueMap := map[string]any{}
	if err := harvestInsertedPriKeyValuesFromAstExp(tableName, columnDefs, columnDefMap, whereExpAst, colValueMap); err != nil {
		return nil, fmt.Errorf("cannot obtain primary key values from WHERE expression: %s", err.Error())
	}
	return colValueMap, nil
}

func (t *Table) execUpdate(cmd *CommandUpdate) (bool, error) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	for _, tableColDef := range t.ColumnDefs {
		if tableColDef.PrimaryKey == PrimaryKeyPartition || tableColDef.PrimaryKey == PrimaryKeyClustering {
			for _, colSetExp := range cmd.ColumnSetExpressions {
				if colSetExp.Name == tableColDef.Name {
					return false, fmt.Errorf("partition/clustering column key %s cannot be modified in the update command", tableColDef.Name)
				}
			}
		}
	}

	var err error
	updatedNonKeyColValues := map[string]any{}
	for i, colSetExp := range cmd.ColumnSetExpressions {
		eCtx := eval.NewPlainEvalCtx(eval_gocqlmem.GocqlmemEvalFunctions, eval_gocqlmem.GocqlmemEvalConstants, nil)
		updatedNonKeyColValues[colSetExp.Name], err = eCtx.Eval(cmd.ColumnSetExpAsts[i])
		if err != nil {
			return false, fmt.Errorf("cannot calculate updated column %d: %s", i, err.Error())
		}
		updatedNonKeyColValues[colSetExp.Name], err = eval_gocqlmem.CastToInternalType(updatedNonKeyColValues[colSetExp.Name], t.ColumnDefs[t.ColumnDefMap[colSetExp.Name]].DataType)
		if err != nil {
			return false, fmt.Errorf("cannot cast updated column %d: %s", i, err.Error())
		}
	}

	valMap := eval.VarValuesMap{}
	valMap[""] = map[string]any{}
	valMap[cmd.TableName] = map[string]any{}
	var isAlreadyExists bool
	for i := range len(t.ColumnValues[0]) {
		clear(valMap[""])
		for colIdx, colDef := range t.ColumnDefs {
			valMap[""][colDef.Name] = t.ColumnValues[colIdx][i]
			valMap[cmd.TableName][colDef.Name] = t.ColumnValues[colIdx][i]
		}

		eCtx := eval.NewPlainEvalCtx(eval_gocqlmem.GocqlmemEvalFunctions, eval_gocqlmem.GocqlmemEvalConstants, valMap)
		isUpdateAny, err := eCtx.Eval(cmd.WhereExpAst)
		if err != nil {
			return false, err
		}

		isUpdate, ok := isUpdateAny.(bool)
		if !ok {
			return false, fmt.Errorf("where expressions return %T, expected bool", isUpdateAny)
		}

		if isUpdate {
			isAlreadyExists = true
			for _, colSetExp := range cmd.ColumnSetExpressions {
				colDefIdx, ok := t.ColumnDefMap[colSetExp.Name]
				if !ok {
					return false, fmt.Errorf("cannot update column %s, it is not in the table definition", colSetExp.Name)
				}
				t.ColumnValues[colDefIdx][i] = updatedNonKeyColValues[colSetExp.Name]
			}
		}
	}

	if isAlreadyExists {
		return true, nil
	}

	if cmd.IfExists {
		return false, nil
	}

	// UPSERT

	insertCmd := CommandInsert{
		CtxKeyspace:  cmd.CtxKeyspace,
		TableName:    cmd.TableName,
		ColumnNames:  make([]string, 0),
		ColumnValues: make([]*Lexem, 0),
		IfNotExists:  false, // We know it does not exist, this is why update became upsert
	}

	// Primary key columns must be set, we have to convert "WHERE col1 = 'a' and col2 = 100` into col1:'a',col2:100
	allInsertedColValues, err := getInsertedPriKeyValuesFromWhereClause(cmd.TableName, t.ColumnDefs, t.ColumnDefMap, cmd.WhereExpAst)
	if err != nil {
		return false, err
	}

	// Add non-primary columns to the map
	for colName, val := range updatedNonKeyColValues {
		allInsertedColValues[colName] = val
	}

	insertedColCount := 0
	for insertedColName := range allInsertedColValues {
		insertCmd.ColumnNames = append(insertCmd.ColumnNames, insertedColName)
		insertCmd.ColumnValues = append(insertCmd.ColumnValues, &Lexem{LexemNull, ""}) // Not used by upsert, all used values are in insertedColumnValues
		insertedColCount++
	}
	return t.execInternalUpsert(&insertCmd, allInsertedColValues)
}

func (t *Table) execDelete(cmd *CommandDelete) (bool, error) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	// TODO: delete

	return false, nil
}
